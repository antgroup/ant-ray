import antkv
import os
import re
from antkv import rocksdb
from raystreaming.state.utils import make_directory_created, remove_files
from raystreaming.state.state_config import AntKVStateConfig, HDFSStateConfig
from .hdfs import HDFSBackend
import logging
logger = logging.getLogger(__name__)


class AntKVSnapshot:
    """ AntKV Snapshot instance.
    This class could create an instance for helping sync data with remote
    persistence storage.
    """
    ANTKV_SNAPSHOT_PATTERN = re.compile(".*\.(sst|blob)$")
    ANTKV_META_PATTERN = re.compile("(CURRENT|MANIFEST-|OPTIONS-).*")
    ANTKV_BLOB_DIR = "value_logs"

    def __init__(self, state_name, key_group, snapshot_config):
        self.hdfs_config = HDFSStateConfig(snapshot_config)
        self.hdfs_db = HDFSBackend(self.hdfs_config)
        self.state_name = state_name
        self.key_group = key_group
        logger.info(
            "Init antkv snapshot instance for {} in key group {}.".format(
                self.state_name, self.key_group))

    def remote_snapshot_path(self):
        """ Generate remote snapshot path with key group and state name.
        """
        logger.info(f"Root dir {self.hdfs_db.get_hdfs_root_dir()}")
        return os.path.join(self.hdfs_db.get_hdfs_root_dir(), "snapshot",
                            self.state_name, str(self.key_group))

    def snapshot(self, checkpoint_id, local_snapshot_path):
        current_snapshot_dir = os.path.join(self.remote_snapshot_path(),
                                            str(checkpoint_id))
        current_snapshot_blob_dir = os.path.join(self.remote_snapshot_path(),
                                                 str(checkpoint_id),
                                                 AntKVSnapshot.ANTKV_BLOB_DIR)

        self.hdfs_db.get_db().create_dir(current_snapshot_dir)
        self.hdfs_db.get_db().create_dir(current_snapshot_blob_dir)

        matched = self.list_all_snapshot_files(local_snapshot_path)
        logger.debug(f"Matched files {matched}")
        for matched_name in matched:
            logger.debug(f"matched file {matched_name}.")
            if os.path.isdir(matched_name):
                logger.info(f"Ignore directory file {matched_name}.")
                continue
            base_name = os.path.basename(matched_name)
            if AntKVSnapshot.ANTKV_BLOB_DIR in matched_name:
                self.hdfs_db.get_db().local_write_to_remote(
                    matched_name,
                    os.path.join(current_snapshot_blob_dir, base_name))
            else:
                self.hdfs_db.get_db().local_write_to_remote(
                    matched_name, os.path.join(current_snapshot_dir,
                                               base_name))

    def rollback_snapshot(self, checkpoint_id, local_snapshot_path):
        """ Download remote snapshot to local directory from persistence storage.
        """
        current_snapshot_dir = os.path.join(self.remote_snapshot_path(),
                                            str(checkpoint_id))
        store_with_blob_dir = os.path.join(local_snapshot_path,
                                           AntKVSnapshot.ANTKV_BLOB_DIR)
        make_directory_created(store_with_blob_dir)

        # NOTE(lingxuan.zlx): Look up root dir and value logs dir both since
        # dfs's unsupported to walk recursivily.
        matches = (list(self.hdfs_db.get_db().list_files(current_snapshot_dir))
                   + list(self.hdfs_db.get_db().list_files(
                       os.path.join(current_snapshot_dir,
                                    AntKVSnapshot.ANTKV_BLOB_DIR))))

        logger.info(f"Matched in rollback snapshot : {list(matches)}")
        for matched_file in set(matches):
            base_name = os.path.basename(matched_file)
            logger.debug(f"Read {base_name} from remote")
            if AntKVSnapshot.ANTKV_BLOB_DIR in matched_file:
                self.hdfs_db.get_db().local_read_from_remote(
                    os.path.join(store_with_blob_dir, base_name), matched_file)
            else:
                self.hdfs_db.get_db().local_read_from_remote(
                    os.path.join(local_snapshot_path, base_name), matched_file)

    def delete_snapshot(self, checkpoint_id, local_snapshot_path):
        current_snapshot_dir = os.path.join(self.remote_snapshot_path(),
                                            str(checkpoint_id))
        current_snapshot_blob_dir = os.path.join(self.remote_snapshot_path(),
                                                 str(checkpoint_id),
                                                 AntKVSnapshot.ANTKV_BLOB_DIR)
        self.hdfs_db.get_db().remove_dir(current_snapshot_dir)
        # NOTE(lingxuna.zlx): incremental checkpoint should delete blob files.
        self.hdfs_db.get_db().remove_dir(current_snapshot_blob_dir)

    def list_all_snapshot_files(self, local_snapshot_path):
        matches = []
        for root, dirnames, filenames in os.walk(local_snapshot_path):
            logger.info(f"List {root} {dirnames} {filenames}")
            filtered_list = filter(
                lambda name: (AntKVSnapshot.ANTKV_SNAPSHOT_PATTERN.match(name) or AntKVSnapshot.ANTKV_META_PATTERN.match(name)),  # noqa
                filenames)
            for filename in filtered_list:
                matches.append(os.path.join(root, filename))
        return matches


class AntKVBackend:
    def __init__(self, jobname, state_name, local_store_path,
                 local_snapshot_path, antkv_config: AntKVStateConfig,
                 key_group, total_memory, metric_group):
        self.jobname = jobname
        self.state_name = state_name
        self.local_store_path = local_store_path
        self.local_snapshot_path = local_snapshot_path
        self.options = antkv.AntKVOptions()
        self.antkv_config = antkv_config
        self.key_group = key_group
        self.write_options = rocksdb.WriteOptions()
        self.read_options = rocksdb.ReadOptions()
        self.antkv_snapshot = AntKVSnapshot(self.state_name, self.key_group,
                                            self.antkv_config.config)
        self.metric_group = metric_group
        self.__open(self.local_store_path)

    def __open(self, store_path):
        """Open a default antkv db.
        """
        self.options.create_if_missing = True
        self.options.ttl_sec = self.antkv_config.state_backend_ttl()
        make_directory_created(self.local_store_path)
        logger.info(f"Open {self.local_store_path} for creating db.")
        s, self.db = antkv.AntKV.Open(self.options, store_path)
        if not s.ok():
            raise RuntimeError(
                f"Open {store_path} for db creating failed, status code"
                f" {s.code()}.")

    def get_db(self):
        return self.db

    def snapshot(self, checkpoint_id: int):
        """Create snapshot in specific checkpoint id.
        Antkv store backend shall not be allowed users to write and take
        snapshot in the same time.
        """
        current_checkpoint_path = self.local_snapshot_path.replace(
            "CHECKPOINT_ID", str(checkpoint_id))
        logger.info("Do snapshot in id {}.".format(checkpoint_id))
        status, checkpoint = antkv.AntKVCheckpoint.Create(self.db)
        logger.info("Do snapshot {} status {} with path {}.".format(
            checkpoint_id, status, current_checkpoint_path))
        # Antkv notes that checkpoint path should not be created before
        # createing checkpoint.
        remove_files(current_checkpoint_path)
        st, _ = checkpoint.CreateCheckpoint(current_checkpoint_path)
        logger.info("Create snapshot status {}, code {}.".format(
            st.ok(), st.code()))

        self.antkv_snapshot.snapshot(checkpoint_id, current_checkpoint_path)

    def rollback_snapshot(self, checkpoint_id: int):
        self.close()
        current_snapshot_path = self.local_snapshot_path.replace(
            "CHECKPOINT_ID", str(checkpoint_id))

        if self.antkv_config.remote_store_enable:
            logger.info(
                "Antkv enabled remote store, reload data from persistence"
                "storage.")
            self.antkv_snapshot.rollback_snapshot(checkpoint_id,
                                                  current_snapshot_path)

        self._hardlink_snapshot_to_store(current_snapshot_path)

        self.__open(self.local_store_path)

    def _hardlink_snapshot_to_store(self, local_snapshot_path):
        store_with_blob_dir = os.path.join(self.local_store_path,
                                           AntKVSnapshot.ANTKV_BLOB_DIR)
        make_directory_created(store_with_blob_dir)
        snapshot_files = self.antkv_snapshot.list_all_snapshot_files(
            local_snapshot_path)
        logger.info(f"Snapshot files {snapshot_files}")
        for matched_name in snapshot_files:
            if os.path.isdir(matched_name):
                logger.info(f"Ignore directory file {matched_name}.")
                continue
            base_name = os.path.basename(matched_name)
            if AntKVSnapshot.ANTKV_BLOB_DIR in matched_name:
                target_name = os.path.join(store_with_blob_dir, base_name)
            else:
                target_name = os.path.join(self.local_store_path, base_name)
            logger.info(f"Hard link {matched_name} to {target_name}.")
            os.link(matched_name, target_name)

    def delete_snapshot(self, checkpoint_id: int):
        current_checkpoint_path = self.local_snapshot_path.replace(
            "CHECKPOINT_ID", str(checkpoint_id))
        remove_files(current_checkpoint_path)
        self.antkv_snapshot.delete_snapshot(checkpoint_id,
                                            current_checkpoint_path)

    def close(self):
        self.db.Close()
        remove_files(self.local_store_path)
