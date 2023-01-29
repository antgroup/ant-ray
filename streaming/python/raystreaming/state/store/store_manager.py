from abc import abstractclassmethod
from raystreaming.state.state_config import (
    AntKVStateConfig,
    StateConfig,
    HDFSStateConfig,
)
from raystreaming.state.state_config import StateBackendType
from .antkv_store import AntKVKeyValueStore
from .memory_store import MemoryKeyValueStore
from .hdfs_store import HDFSKeyValueStore
import logging
logger = logging.getLogger(__name__)


class StoreManager:
    def __init__(self, jobname, state_name, config, key_group, metric_group):
        self.jobname = jobname
        self.state_name = state_name
        self.config = config
        self.metric_group = metric_group
        self.key_group = key_group

    def build_store_manager(self):
        state_config = StateConfig(self.config)
        logger.info(f"Build store manager config : {self.config}")
        if state_config.state_backend_type() in [
                StateBackendType.ANTKV, StateBackendType.ROCKSDB
        ]:
            return AntKVStoreManager(self.jobname, self.state_name,
                                     self.config, self.key_group,
                                     self.metric_group)
        elif state_config.state_backend_type() is StateBackendType.MEMORY:
            return MemoryKVStoreManager(self.jobname, self.state_name,
                                        self.config, self.key_group,
                                        self.metric_group)
        elif state_config.state_backend_type() is StateBackendType.HDFS:
            return HDFSKVStoreManager(self.jobname, self.state_name,
                                      self.config, self.key_group,
                                      self.metric_group)
        else:
            raise RuntimeError(
                f"No such {state_config.state_backend_type()} state backend")

    @abstractclassmethod
    def get_store(self, state_type):
        pass


class MemoryKVStoreManager(StoreManager):
    def __init__(self, jobname: str, state_name: str, config, key_group,
                 metric_group):
        super().__init__(jobname, state_name, config, key_group, metric_group)
        self.memory_kv_store = None

    def build_key_value_store(self):
        if self.memory_kv_store is None:
            self.memory_kv_store = MemoryKeyValueStore(
                self, self.jobname, self.state_name, self.config,
                self.metric_group)
        return self.memory_kv_store

    def snapshot(self, checkpoint_id: int):
        if self.memory_kv_store is not None:
            self.memory_kv_store.snapshot(checkpoint_id)

    def rollback_snapshot(self, checkpoint_id: int):
        if self.memory_kv_store is not None:
            self.memory_kv_store.rollback_snapshot(checkpoint_id)

    def delete_checkpoint(self, checkpoint_id: int):
        if self.memory_kv_store is not None:
            self.memory_kv_store.delete_snapshot(checkpoint_id)


class HDFSKVStoreManager(StoreManager):
    def __init__(self, jobname: str, state_name: str, config, key_group,
                 metric_group):
        super().__init__(jobname, state_name, config, key_group, metric_group)
        self.hdfs_config = HDFSStateConfig(self.config)
        self.hdfs_kv_store = None

    def build_key_value_store(self):
        if self.hdfs_kv_store is None:
            self.hdfs_kv_store = HDFSKeyValueStore(self.hdfs_config,
                                                   self.metric_group)
        return self.hdfs_kv_store

    def snapshot(self, checkpoint_id: int):
        if self.hdfs_kv_store is not None:
            self.hdfs_kv_store.snapshot(checkpoint_id)

    def rollback_snapshot(self, checkpoint_id: int):
        if self.hdfs_kv_store is not None:
            self.hdfs_kv_store.rollback_snapshot(checkpoint_id)

    def delete_checkpoint(self, checkpoint_id: int):
        if self.hdfs_kv_store is not None:
            self.hdfs_kv_store.delete_snapshot(checkpoint_id)


class AntKVStoreManager(StoreManager):
    def __init__(self, jobname: str, state_name: str, config, key_group,
                 metric_group):
        super().__init__(jobname, state_name, config, key_group, metric_group)
        self.antkv_config = AntKVStateConfig(
            config) if type(config) is dict else config
        self.jobname = jobname
        self.metric_group = metric_group
        self.state_name = state_name
        self.key_group = key_group
        self.kv_store = None

    @property
    def local_store_path(self):
        return (f"{self.antkv_config.local_store_path}/{self.jobname}/" +
                f"{self.state_name}/{self.key_group}")

    @property
    def local_snapshot_path(self):
        return (f"{self.antkv_config.local_snapshot_path}/{self.jobname}/" +
                f"{self.state_name}/{self.key_group}/CHECKPOINT_ID")

    def build_key_value_store(self):
        if self.kv_store is None:
            self.kv_store = AntKVKeyValueStore(
                self, self.jobname, self.state_name, self.antkv_config,
                self.metric_group)
        return self.kv_store

    def snapshot(self, checkpoint_id: int):
        if self.kv_store is not None:
            self.kv_store.snapshot(checkpoint_id)

    def rollback_snapshot(self, checkpoint_id: int):
        if self.kv_store is not None:
            self.kv_store.rollback_snapshot(checkpoint_id)

    def delete_snapshot(self, checkpoint_id: int):
        if self.kv_store is not None:
            self.kv_store.delete_snapshot(checkpoint_id)
