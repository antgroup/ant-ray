from raystreaming.state.backend.hdfs import HDFSBackend
from raystreaming.state.state_config import HDFSStateConfig
from raystreaming.state.key_value_state import KeyValueState


class HDFSKeyValueStore(KeyValueState):
    def __init__(self, hdfs_config: HDFSStateConfig, metric_group=None):
        self.hdfs_backend = HDFSBackend(hdfs_config, metric_group)

    def get(self, key):
        return self.hdfs_backend.get_db().get(key)

    def put(self, key, value):
        return self.hdfs_backend.get_db().put(key, value)

    def delete(self, key):
        return self.hdfs_backend.get_db().delete(key)

    def snapshot(self, checkpoint_id: int):
        self.hdfs_backend.snapshot(checkpoint_id)

    def rollback_snapshot(self, checkpoint_id: int):
        self.hdfs_backend.rollback_snapshot(checkpoint_id)

    def delete_snapshot(self, checkpoint_id: int):
        self.hdfs_backend.delete(checkpoint_id)
