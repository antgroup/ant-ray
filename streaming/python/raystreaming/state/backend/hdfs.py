from abc import ABC, abstractmethod
import logging
import os
import uuid
try:
    import oss2
except Exception:
    print("import oss2 failed. please check module env.")

from raystreaming.state.key_value_state import KeyValueState
from raystreaming.state.state_config import HDFSStateConfig
from raystreaming.state.utils import remove_files
try:
    import zdfs
except Exception:
    print("import zdfs failed, please check module env.")

logger = logging.getLogger(__name__)

DEFAULT_HDFS_BUFFER_SIZE = 16 * 1024


class RemoteHDFS(ABC):
    """Sync local file to specific remote file.
    """

    @abstractmethod
    def local_write_to_remote(self, local_file_name, remote_file_name):
        pass

    """Sync remote file to local.
    """

    @abstractmethod
    def local_read_from_remote(self, local_file_name, remote_file_name):
        pass

    @abstractmethod
    def create_dir(self, dir_path):
        pass

    @abstractmethod
    def remove_dir(self, dir_path):
        pass

    @abstractmethod
    def list_files(self, dir_path):
        pass


class PanguStateBackend(KeyValueState, RemoteHDFS):
    """
    file system state backend
    """

    def __init__(self, cluster_name, root_dir):
        self.__cluster_name = cluster_name
        self.__dir = root_dir
        self.__fs = None
        logger.info("cluster_name is {}, root dir is {}.".format(
            self.__cluster_name, self.__dir))
        logger.info("use pangu.")
        self.__cluster_name = self.__cluster_name[8:]
        from raystreaming.runtime.pangu_fs_lib import PanguFileSystem
        self.__fs = PanguFileSystem(self.__cluster_name)
        try:
            logger.info("creating dir {}.".format(self.__dir))
            self.__fs.mkdir(self.__dir)
        except FileExistsError:
            logger.info("dir already exists, skipped.")

        logger.info("Checkpoint state init success.")

    def list_files(self, dir_path="."):
        full_path = self.__gen_file_path(dir_path)
        res = []
        if self.__fs is not None:
            for stat in self.__fs.listdir(full_path):
                if not stat.is_dir():
                    res.append(stat.get_path())
        return map(lambda x: os.path.join(dir_path, x), res)

    def rename(self, src, dst):
        logger.info("rename {} to {}".format(src, dst))
        if self.__fs is not None:
            self.__fs.rename(
                self.__gen_file_path(src), self.__gen_file_path(dst))

    def get(self, key):
        logger.info("Get value of key {} start.".format(key))
        if self.__fs is not None:
            content = b""
            file_path = self.__gen_file_path(key)
            try:
                with self.__fs.open(file_path, self.__fs.FLAG_GENERIC_READ,
                                    0) as file:
                    for line in file:
                        content += line
            except IOError:
                logger.info(
                    "Get value of key {} failed, return null.".format(key))
                return None
            logger.info("Get value of key {} success.".format(key))
            return content

    def put(self, key, value):
        logger.info("Put value of key {} start.".format(key))
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            try:
                logger.info("Try to remove value of key: {}.".format(key))
                self._delete(key)
            except FileNotFoundError:
                pass
            try:
                self.__fs.create(file_path, 0o755)
            except FileExistsError:
                logger.warn("File {} existed".format(file_path))
            file = self.__fs.open(file_path, self.__fs.FLAG_SEQUENTIAL_WRITE,
                                  0)
            file.append(value)
            file.close()
            logger.info("Put value of key {} success.".format(key))

    def local_write_to_remote(self, local_file_name, remote_file_name):
        logger.info(
            f"Write local file {local_file_name} to remote {remote_file_name}")
        if self.__fs is not None:
            file_path = self.__gen_file_path(remote_file_name)
            try:
                logger.info(
                    "Try to remove value of key: {}.".format(remote_file_name))
                self._delete(remote_file_name)
            except FileNotFoundError:
                pass
            try:
                self.__fs.create(file_path, 0o755)
            except FileExistsError:
                logger.warn("File {} existed".format(file_path))
            file = self.__fs.open(file_path, self.__fs.FLAG_SEQUENTIAL_WRITE,
                                  0)
            with open(local_file_name, "rb") as local_file:
                data = local_file.read(DEFAULT_HDFS_BUFFER_SIZE)
                while data != b"":
                    file.append(data)
                    data = local_file.read(DEFAULT_HDFS_BUFFER_SIZE)

            file.close()
            logger.info(
                "Put value of key {} success.".format(remote_file_name))

    def local_read_from_remote(self, local_file_name, remote_file_name):
        logger.info(
            f"Read remote file {remote_file_name} to remote {local_file_name}")
        if self.__fs is not None:
            file_path = self.__gen_file_path(remote_file_name)
            try:
                with self.__fs.open(file_path, self.__fs.FLAG_GENERIC_READ,
                                    0) as file:
                    with open(local_file_name, "wb") as local_file:
                        for line in file:
                            local_file.write(line)

            except IOError:
                logger.info("Get value of key {} failed, return null.".format(
                    remote_file_name))
                return None
            logger.info(
                "Read content of key {} success.".format(remote_file_name))

    def _delete(self, key):
        logger.info("Remove value of key {} start.".format(key))
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            self.__fs.remove(file_path)
            logger.info("Remove value of key {} success.".format(key))

    def delete(self, key):
        self._delete(key)

    def _exists(self, key):
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            # Note(lingxuan.zlx): query file exists or not by using file system
            # stat function since there is no such function in pangu fs lib.
            try:
                self.__fs.stat(file_path)
            except FileNotFoundError:
                return False
            return True
        return False

    def exists(self, key):
        return self._exists(key)

    def __gen_file_path(self, key):
        return os.path.join(self.__dir, key)

    def create_dir(self, dir_path):
        """ Create remote directory of pangu.
        """
        if self.__fs is not None:
            file_path = self.__gen_file_path(dir_path)
            self.__fs.mkdir(file_path)

    def remove_dir(self, dir_path):
        """ Remove remote directory for pangu.
        """
        if self.__fs is not None:
            file_path = self.__gen_file_path(dir_path)
            try:
                self.__fs.rmdir(file_path)
            except FileNotFoundError:
                pass


class DfsStateBackend(KeyValueState, RemoteHDFS):
    """
    Distributed file system state backend
    """

    def __init__(self, cluster_name, root_dir):
        self.__cluster_name = cluster_name
        self.__dir = root_dir
        self.__fs = None
        self.__options = None

        logger.info("cluster_name is {}, root dir is {}.".format(
            self.__cluster_name, self.__dir))
        logger.info("use dfs.")
        self.__options = zdfs.FileSystemOptions()
        pangu_options = zdfs.PanguOptions()
        # Thread num in hard code.
        pangu_options.io_thread_num = 2
        pangu_options.callback_thread_num = 2
        pangu_options.callback_in_iothread = True
        pangu_options.log_level = zdfs.LogLevel_ERROR
        zdfs.PanguFileSystem.SetOptions(pangu_options)

        self.__fs = zdfs.PanguFileSystem.Create(self.__cluster_name,
                                                self.__options)
        try:
            logger.info("creating dir {}.".format(self.__dir))
            ec = self.__fs.CreateDirectory(self.__dir,
                                           zdfs.CreateDirectoryOptions())
            if ec != 0:
                logger.info("create directory {}.".format(ec))
        except FileExistsError:
            logger.info("dir already exists, skipped.")

    def list_files(self, dir_path):
        full_path = self.__gen_file_path(dir_path)
        res = []
        if self.__fs is not None:
            # DFS sdk suggests us to look up whole directory by pages.
            options_list_dir = zdfs.ListDirectoryOptions()
            options_list_dir.full_stat = False
            options_list_dir.all = False
            has_more = True
            while has_more:
                ec, entries, entry_stats, has_more = self.__fs.ListDirectory(
                    full_path, options_list_dir)
                options_list_dir.prev_entry = entries[-1]
                res += list(
                    filter(lambda entry: len(entry) > 1 and entry[-1] != "/",
                           entries))
        return map(lambda x: os.path.join(dir_path, x), res)

    def rename(self, src, dst):
        logger.info("rename {} to {}".format(src, dst))
        if self.__fs is not None:
            ec = self.__fs.Rename(
                self.__gen_file_path(src), self.__gen_file_path(dst),
                zdfs.RenameOptions())
            if ec != 0:
                logger.info("Rename file error code : {}.".format(ec))

    def get(self, key):
        logger.info("Get value of key {} start.".format(key))
        if self.__fs is not None:
            content = b""
            file_path = self.__gen_file_path(key)
            ec, file = self.__fs.OpenFile(file_path, zdfs.OpenMode_READ_ONLY,
                                          zdfs.OpenFileOptions())
            if ec != 0:
                logger.info(f"Get file {file_path} error {ec}")
                return content
            ec, stat = self.__fs.Stat(file_path, zdfs.StatOptions())
            read_count = 0
            while read_count < stat.file.length:
                ec, data, read_len = file.Read(stat.file.length - read_count,
                                               zdfs.FileReadOptions())
                content += data
                read_count += read_len
                if ec != 0:
                    logger.warning(f"Read failed, error code {ec}")
                    break
            ec = file.Close(zdfs.FileCloseOptions())
            logger.info("Get value of key {}, length {} success.".format(
                key, stat.file.length))
        return content

    def put(self, key, value):
        logger.info("Put value of key {} start.".format(key))
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            try:
                logger.info("Try to remove value of key: {}.".format(key))
                self.delete(key)
            except FileNotFoundError:
                pass
            ec = self.__fs.CreateFile(file_path, zdfs.CreateFileOptions())
            ec, file = self.__fs.OpenFile(file_path, zdfs.OpenMode_WRITE_ONLY,
                                          zdfs.OpenFileOptions())
            if ec != 0:
                logger.info(f"Get file {file_path} error {ec}")
            ec, offset = file.Append(value, zdfs.FileAppendOptions())
            ec, length = file.Flush(zdfs.FileFlushOptions())
            ec = file.Close(zdfs.FileCloseOptions())
            logger.info(
                "Put value of key {}, offset {}, length {} success.".format(
                    key, offset, length))

    def local_write_to_remote(self, local_file_name, remote_file_name):
        logger.info(
            f"Write local file {local_file_name} to remote {remote_file_name}")
        if self.__fs is not None:
            file_path = self.__gen_file_path(remote_file_name)
            try:
                logger.info(
                    "Try to remove value of key: {}.".format(remote_file_name))
                self.delete(remote_file_name)
            except FileNotFoundError:
                pass
            ec = self.__fs.CreateFile(file_path, zdfs.CreateFileOptions())
            ec, file = self.__fs.OpenFile(file_path, zdfs.OpenMode_WRITE_ONLY,
                                          zdfs.OpenFileOptions())
            if ec != 0:
                logger.info(f"Get file {file_path} error {ec}")
            with open(local_file_name, "rb") as local_file:
                data = local_file.read(DEFAULT_HDFS_BUFFER_SIZE)
                while data != b"":
                    ec, offset = file.Append(data, zdfs.FileAppendOptions())
                    data = local_file.read(DEFAULT_HDFS_BUFFER_SIZE)

            ec, length = file.Flush(zdfs.FileFlushOptions())
            ec = file.Close(zdfs.FileCloseOptions())
            logger.info(
                "Put value of remote file name {}, length {} success.".format(
                    remote_file_name, length))

    def local_read_from_remote(self, local_file_name, remote_file_name):
        logger.info(
            f"Read remote file {remote_file_name} to local {local_file_name}")
        if self.__fs is not None:
            file_path = self.__gen_file_path(remote_file_name)
            ec, file = self.__fs.OpenFile(file_path, zdfs.OpenMode_READ_ONLY,
                                          zdfs.OpenFileOptions())
            if ec != 0:
                logger.info(f"Get file {file_path} error {ec}")

            ec, stat = self.__fs.Stat(file_path, zdfs.StatOptions())
            total_size = stat.file.length
            with open(local_file_name, "wb") as local_file:
                # NOTE(lingxuan.zlx): the returned buffer will be aligned with
                # 0x00 if once read buffer length bigger than real size.
                read_count = 0
                while read_count < total_size:
                    remained_read_len = total_size - read_count
                    read_batch_size = (DEFAULT_HDFS_BUFFER_SIZE
                                       if remained_read_len >
                                       DEFAULT_HDFS_BUFFER_SIZE else
                                       remained_read_len)
                    ec, data, read_len = file.Read(read_batch_size,
                                                   zdfs.FileReadOptions())
                    read_count += read_len
                    local_file.write(data)
                    if ec != 0:
                        logger.warning(f"Read failed, error code {ec}")
                        break
                logger.info(f"Get dfs read count {read_count}")
            ec = file.Close(zdfs.FileCloseOptions())
            logger.info(
                "Get value of remote file name {}, length {} success.".format(
                    remote_file_name, stat.file.length))

    def _delete(self, key):
        logger.info("Remove value of key {} start.".format(key))
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            ec = self.__fs.Delete(file_path, zdfs.DeleteOptions())
            if ec == 0:
                logger.info("Remove value of key {} success.".format(key))
            else:
                logger.warn(
                    "Remove value of key {} failed, error code {}.".format(
                        key, ec))

    def delete(self, key):
        if self.exists(key):
            self._delete(key)

    def _exists(self, key):
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            ec, stat = self.__fs.Stat(file_path, zdfs.StatOptions())
            return stat.path != ""
        return False

    def exists(self, key):
        return self._exists(key)

    def __gen_file_path(self, key):
        return os.path.join(self.__dir, key)

    def create_dir(self, dir_path):
        """ Create remote directory of dfs.
        """
        if self.__fs is not None:
            file_path = self.__gen_file_path(dir_path)
            ec = self.__fs.CreateDirectory(file_path,
                                           zdfs.CreateDirectoryOptions())
            logger.info(
                f"Create dfs directory {file_path}, error code : {ec}.")

    def remove_dir(self, dir_path):
        """ Remove remote directory for dfs.
        """
        self.delete(dir_path)


class OssStateBackend(KeyValueState, RemoteHDFS):

    TMP_DIR = "/tmp"

    def __init__(self, endpoint, bucket_name, store_dir, access_id,
                 access_key):
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.store_dir = store_dir
        self.access_id = access_id
        self.access_key = access_key
        self.__open()

    def __open(self):
        auth = oss2.Auth(self.access_id, self.access_key)
        self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        logger.info(f"Open oss bucket {self.bucket_name} on {self.endpoint}")

    def put(self, key, value):
        full_path = os.path.join(self.store_dir, key)
        self.bucket.put_object(full_path, value)

    def get(self, key):
        """ Get content from remote file.
        """
        full_path = os.path.join(self.store_dir, key)
        temp_file = os.path.join(OssStateBackend.TMP_DIR, str(uuid.uuid4()))
        try:
            self.bucket.get_object_to_file(full_path, temp_file)
        except oss2.exceptions.NoSuchKey:
            return None
        content = b""
        with open(temp_file, "rb") as f:
            for data in f:
                content += data
        remove_files(temp_file)
        return content

    def delete(self, key):
        full_path = os.path.join(self.store_dir, key)
        self.bucket.delete_object(full_path)

    def local_write_to_remote(self, local_file_name, remote_file_name):
        full_path = os.path.join(self.store_dir, remote_file_name)
        self.bucket.put_object_from_file(full_path, local_file_name)

    def local_read_from_remote(self, local_file_name, remote_file_name):
        full_path = os.path.join(self.store_dir, remote_file_name)
        self.bucket.get_object_to_file(full_path, local_file_name)

    def create_dir(self, dir_path):
        logger.info("Skip created dir for oss state backend.")

    def remove_dir(self, dir_path):
        full_dir = os.path.join(self.store_dir, dir_path)
        for obj in oss2.ObjectIterator(self.bucket, prefix=full_dir):
            self.bucket.delete_object(obj.key)

    def list_files(self, dir_path):
        full_dir = os.path.join(self.store_dir, dir_path)
        return [
            obj.key
            for obj in oss2.ObjectIterator(self.bucket, prefix=full_dir)
        ]


class HDFSBackend:
    def __init__(self, hdfs_config: HDFSStateConfig, metric_group=None):
        self.hdfs_config = hdfs_config
        self.hdfs_db = None
        self.__hdfs_root_dir = None
        self.__open()

    def __open(self):
        if self.hdfs_config.state_backend_hdfs_store_type == "PANGU":
            self.hdfs_db = PanguStateBackend(
                self.hdfs_config.state_backend_pangu_cluster_name,
                self.hdfs_config.state_backend_pangu_root_dir)
            self.__hdfs_root_dir = \
                self.hdfs_config.state_backend_pangu_root_dir
        elif self.hdfs_config.state_backend_hdfs_store_type == "DFS":
            self.hdfs_db = DfsStateBackend(
                self.hdfs_config.state_backend_dfs_cluster_name,
                self.hdfs_config.state_backend_dfs_store_dir)
            self.__hdfs_root_dir = self.hdfs_config.state_backend_dfs_store_dir
        elif self.hdfs_config.state_backend_hdfs_store_type == "OSS":
            self.hdfs_db = OssStateBackend(
                self.hdfs_config.state_backend_oss_endpoint,
                self.hdfs_config.state_backend_oss_bucket,
                self.hdfs_config.state_backend_oss_store_dir,
                self.hdfs_config.state_backend_oss_access_id,
                self.hdfs_config.state_backend_oss_access_key)
            self.__hdfs_root_dir = self.hdfs_config.state_backend_oss_store_dir

    def get_db(self):
        return self.hdfs_db

    def get_hdfs_root_dir(self):
        return self.__hdfs_root_dir

    def snapshot(self, checkpoint_id: int):
        pass

    def rollback_snapshot(self, checkpoint_id: int):
        pass

    def close(self):
        pass
