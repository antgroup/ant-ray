from raystreaming.state.backend.antkv import AntKVBackend
from antkv import rocksdb
from raystreaming.state.key_value_state import KeyValueState
import logging
import pickle
import codecs
logger = logging.getLogger(__name__)


class AntKVKeyValueStore(KeyValueState):
    def __init__(self, state_manager, jobname, state_name, config,
                 metric_group):
        self.antkv_backend = AntKVBackend(
            jobname=jobname,
            state_name=state_name,
            local_store_path=state_manager.local_store_path,
            local_snapshot_path=state_manager.local_snapshot_path,
            antkv_config=config,
            key_group=state_manager.key_group,
            total_memory=0,
            metric_group=metric_group)
        self.__init_metric(jobname, state_name, metric_group)

    def __init_metric(self, jobname, state_name, metric_group):
        logger.info(f"Init state metrics for {state_name}.")
        metric_tags = {
            "stateName": state_name,
            "jobname": jobname,
            "type": "antkv",
            "language": "python"
        }
        self.write_meter = metric_group.get_meter(
            "write_meter", metric_tags) if metric_group is not None else None
        self.read_meter = metric_group.get_meter(
            "read_meter", metric_tags) if metric_group is not None else None
        self.delete_meter = metric_group.get_meter(
            "delete_meter", metric_tags) if metric_group is not None else None

    def __convert_obj_to_str(self, obj):
        return codecs.encode(pickle.dumps(obj), "base64").decode()

    def __convert_str_to_obj(self, byte_obj):
        return pickle.loads(codecs.decode(byte_obj.encode(), "base64"))

    def get(self, key):
        if self.read_meter is not None:
            self.read_meter.update(1)

        key = self.__convert_obj_to_str(key)
        status, ret_slice = self.antkv_backend.get_db().Get(
            self.antkv_backend.read_options, rocksdb.Slice(key))
        # NOTE(lingxuan.zlx): sure returned slice data only if
        # status is non-notfound.

        return None if status.IsNotFound() else self.__convert_str_to_obj(
            ret_slice)

    def put(self, key, value):
        if self.write_meter is not None:
            self.write_meter.update(1)

        key = self.__convert_obj_to_str(key)
        value = self.__convert_obj_to_str(value)

        return self.antkv_backend.get_db().Put(
            self.antkv_backend.write_options, rocksdb.Slice(key),
            rocksdb.Slice(value))

    def delete(self, key: str):
        if self.delete_meter is not None:
            self.delete_meter.update(1)

        key = self.__convert_obj_to_str(key)
        return self.antkv_backend.get_db().Delete(
            self.antkv_backend.write_options, rocksdb.Slice(key))

    def snapshot(self, checkpoint_id: int):
        self.antkv_backend.snapshot(checkpoint_id)

    def rollback_snapshot(self, checkpoint_id: int):
        if checkpoint_id > 0:
            self.antkv_backend.rollback_snapshot(checkpoint_id)
        else:
            logger.info(f"Skip rollback checkpoint : {checkpoint_id}.")

    def delete_snapshot(self, checkpoint_id: int):
        self.antkv_backend.delete_snapshot(checkpoint_id)
