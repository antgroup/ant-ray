from enum import Enum
import logging
from raystreaming.constants import StreamingConstants
from raystreaming.config import ConfigHelper
logger = logging.getLogger(__name__)


class StateBackendType(Enum):
    UNKNOWN = -1
    MEMORY = 0
    ROCKSDB = 1
    ANTKV = 2
    HBASE = 3
    HDFS = 4
    TBASE = 5

    def get_type_by_name(name):
        for backend_type in StateBackendType:
            if name == backend_type.name:
                return backend_type
        return StateBackendType.UNKNOWN


class StateConfig:
    BACKEND_TYPE = "state.backend.type"

    BACKEND_TTL = "state.backend.ttl"

    STATE_CHECKPOINT_RETAINED_NUM = "state.checkpoint.retained.num"

    STATE_CHECKPOINT_INCREMENT_ENABLE = "state.checkpoint.increment.enable"

    MAX_PARALLELISM = "state.max.parallelism"

    STATE_ENABLE_DEBUG_LOG = "state.enable.debug.log"

    STATE_SERIALIZER_TYPE = "state.serializer.type"

    ASYNC_CHEKCPOINT_MODE = "state.checkpoint.async.mode"

    STATE_LOCAL_STORE_USE_JOB_DIR = "state.local.store.use.job.dir.enable"

    def __init__(self, config={}):
        self.config = config

    def _get_or_default(self, key, default):
        if key in self.config:
            return self.config[key]
        return default

    def _get_or_default_with_func(self, key, default, func):
        if key in self.config:
            return func(self.config[key])
        return default

    def state_backend_type(self):
        return self._get_or_default_with_func(
            self.BACKEND_TYPE, StateBackendType.MEMORY,
            lambda x: StateBackendType.get_type_by_name(x))
        return StateBackendType.MEMORY

    def state_local_store_use_job_dir(self):
        """To checkout whether local store dir should be used.
        In cluster mode, it could be used by default.
        """
        return self._get_or_default_with_func(
            self.STATE_LOCAL_STORE_USE_JOB_DIR, True,
            lambda x: x.lower() == "true")

    def state_backend_ttl(self):
        return self._get_or_default_with_func(self.BACKEND_TTL, 10800,
                                              lambda x: int(x))


class AntKVStateConfig(StateConfig):
    STATE_BACKEND_ANTKV_STORE_DIR = "state.backend.rocksdb.store.dir"

    STATE_BACKEND_ANTKV_SNAPSHOT_DIR = "state.backend.rocksdb.snapshot.dir"

    STATE_BACKEND_ANTKV_BACKGROUND_RATE_LIMIT = (
        "state.backend.antkv.background.rate.limit")

    STATE_BACKEND_ANTKV_METRIC_REPORT_INTERVAL = (
        "state.backend.antkv.metric.report.interval")

    STATE_BACKEND_ANTKV_STATISTIC_ENABLE = (
        "state.backend.antkv.statistic.enable")

    STATE_BACKEND_ANTKV_DELETE_THREAD_NUM = (
        "state.backend.antkv.delete.thread.num")

    STATE_BACKEND_ANTKV_REMOTE_STORE_ENABLE = (
        "state.backend.antkv.remote.store.enable")

    STATE_BACKEND_ANTKV_REMOTE_TRANSPORT_THREAD_NUM = (
        "state.backend.antkv.remote.transfer.thread.num")

    STATE_BACKEND_ANTKV_REMOTE_TRANSFER_BUFFER_SIZE = (
        "state.backend.antkv.remote.transfer.buffer.size")

    STATE_BACKEND_ANTKV_OPTIONS_CF_MAX_WRITE_BUFFER_NUMBER = (
        "state.backend.antkv.options.cf.write-buffer.num")

    STATE_BACKEND_ANTKV_OPTIONS_CF_WRITE_BUFFER_SIZE_MB = (
        "state.backend.antkv.options.cf.write-buffer.size.mb")

    STATE_BACKEND_ANTKV_OPTIONS_CF_BLOCK_INDEX_RESTART_INTERVAL = (
        "state.backend.antkv.options.cf.block-index.restart-interval")

    STATE_BACKEND_ANTKV_FLUSH_THREAD_NUM = (
        "state.backend.antkv.flush.thread.num")

    STATE_BACKEND_ANTKV_LRU_CACHE_SHARDBIT = (
        "state.backend.antkv.lrucache.shardbit")

    STATE_BACKEND_ANTKV_VALUE_COMPACTION_TYPE = (
        "state.backend.antkv.value.compression.type")

    STATE_BACKEND_ANTKV_PERF_NO_CURRENCE = (
        "state.backend.antkv.perf.no-ccurrence")

    STATE_BACKEND_ANTKV_ENABLE_LEARNED_INDEX = (
        "state.backend.antkv.enable-learned-index")

    STATE_BACKEND_ANTKV_LEARNED_INDEX_LEVEL = (
        "state.backend.antkv.learned-index-level")

    def __init__(self, config={}):
        super().__init__(config)
        self.__compatible_setting()

    def __compatible_setting(self):
        if (AntKVStateConfig.STATE_BACKEND_ANTKV_SNAPSHOT_DIR not in
                self.config):
            self.config[
                AntKVStateConfig.
                STATE_BACKEND_ANTKV_SNAPSHOT_DIR] = self.local_store_path

    @property
    def local_store_path(self):
        return self.config.get(AntKVStateConfig.STATE_BACKEND_ANTKV_STORE_DIR,
                               "/tmp/store")

    @property
    def local_snapshot_path(self):
        return self.config.get(AntKVStateConfig.STATE_BACKEND_ANTKV_STORE_DIR,
                               "/tmp/snapshot")

    @property
    def remote_store_enable(self):
        return super(AntKVStateConfig, self)._get_or_default_with_func(
            self.STATE_BACKEND_ANTKV_REMOTE_STORE_ENABLE, True,
            lambda x: x.lower() == "true")


class HDFSStateConfig(StateConfig):
    """HDFS state includes pangu and dfs. Actually dfs stands for pangu2
    cluster. We use 3-tuple data to define a hdfs config whose fields are
    type, config name and default value.
    """
    STATE_BACKEND_HDFS_STORE_TYPE = (str, "state.backend.hdfs.store.type",
                                     "PANGU")

    # Pangu config
    STATE_BACKEND_PANGU_CLUSTER_NAME = (str,
                                        "state.backend.pangu.cluster.name",
                                        "pangu://alipay-pangu-for-hdfs-test")
    STATE_BACKEND_PANGU_ROOT_DIR = (str, "state.backend.pangu.store.dir",
                                    "/raystate/default/")
    STATE_BACKEND_PANGU_USER_MYSQL_URL = (str,
                                          "state.backend.pangu.user.mysql.url",
                                          "")
    STATTE_BACKEND_PANGU_NUWA_GENCONFIG = (
        str, "state.backend.pangu.nuwa.genconfig", "true")
    STATTE_BACKEND_PANGU_NUWA_CLUSTER = (str,
                                         "state.backend.pangu.nuwa.cluster",
                                         "alipay-pangu-for-hdfs-test")
    STATTE_BACKEND_PANGU_NUWA_SERVERS = (
        str, "state.backend.pangu.nuwa.servers",
        ("alipay-pangu-for-hdfs-test," +
         "11.166.80.137:10240,11.166.207.233:10240,11.166.88.160:10240"))
    STATTE_BACKEND_PANGU_NUWA_PROXIES = (
        str, "state.backend.pangu.nuwa.proxies",
        "11.166.80.137:10245,11.166.207.233:10245,11.166.88.160:10245")

    STATE_BACKEND_PANGU_WRITE_BUFFER_SIZE = (
        int, "state.backend.pangu.write.buffer.size", 8388608)
    STATE_BACKEND_PANGU_WRITE_BLOCK_SIZE = (
        int, "state.backend.pangu.write.block.size", 4096)
    STATE_BACKEND_PANGU_FILE_COPY_NUM = (int,
                                         "state.backend.pangu.file.copy.num",
                                         3)
    STATE_BACKEND_PANGU_PROCESS_MEM_LIMIT = (
        int, "state.backend.pangu.file.process.mem.limit", 16777216)

    STATE_BACKEND_PANGU_IO_WRITE_BUFFER = (
        int, "state.backend.pangu.io.write.buffer.size", 524288)
    STATE_BACKEND_PANGU_STATISTIC_ENABLE = (
        bool, "state.backend.pangu.statistic.enable", False)
    STATE_BACKEND_PANGU_APSARA_LOG = (bool, "state.backend.pangu.apsara.log",
                                      False)

    # Dfs config
    STATE_BACKEND_DFS_CLUSTER_NAME = (
        str, "state.backend.dfs.cluster.name",
        "dfs://f-antq-test.aliyun-inc.test:10290")
    STATE_BACKEND_DFS_STORE_DIR = (str, "state.backend.dfs.store.dir",
                                   "/arc/streaming/java")
    STATE_BACKEND_DFS_WRITE_LIMIT_MB = (int,
                                        "state.backend.dfs.write.limit.mb",
                                        128)
    STATE_BACKEND_DFS_READ_LIMIT_MB = (int, "state.backend.dfs.read.limit.mb",
                                       256)
    STATE_BACKEND_DFS_IPC_MAXIDLETIME = (int,
                                         "state.backend.dfs.ipc.maxidletime",
                                         300000)
    STATE_BACKEND_DFS_IPC_TIMEOUT = (int, "state.backend.dfs.ipc.timeout",
                                     3000)
    STATE_BACKEND_DFS_IPC_RETRIES = (int, "state.backend.dfs.ipc.retries", 3)
    STATE_BACKEND_DFS_IPC_META_RETRY_INTERVAL = (
        int, "state.backend.dfs.ipc.meta.retry.interval", 20)

    # OSS config
    STATE_BACKEND_OSS_ENDPOINT = (str, "state.backend.oss.endpoint", "")
    STATE_BACKEND_OSS_BUCKET = (str, "state.backend.oss.bucket", "default")
    STATE_BACKEND_OSS_STORE_DIR = (str, "state.backend.oss.store.dir", "/tmp")
    STATE_BACKEND_OSS_ACCESS_ID = (str, "state.backend.oss.id", "default-id")
    STATE_BACKEND_OSS_ACCESS_KEY = (str, "state.backend.oss.key",
                                    "default-key")

    def __init__(self, config=None):
        if config is None:
            config = {}
        super().__init__(config)
        self.__compatible_setting()
        self.__generate_property_func()

    def __compatible_setting(self):
        key = HDFSStateConfig.STATE_BACKEND_HDFS_STORE_TYPE[1]
        if key not in self.config:
            self.config[key] = "PANGU" if (self.config.get(
                StreamingConstants.CP_STATE_BACKEND_TYPE,
                StreamingConstants.CP_STATE_BACKEND_DEFAULT
            ) == StreamingConstants.CP_STATE_BACKEND_PANGU) else "DFS"
        else:
            logger.info("Skip overwrite compatible HDFS config.")
            return
        logger.debug("Use compatible {} config.".format(self.config[key]))

        if self.config[key] == "PANGU":
            self.config[HDFSStateConfig.STATE_BACKEND_PANGU_CLUSTER_NAME[
                1]] = (ConfigHelper.get_cp_pangu_cluster_name(self.config))
            self.config[HDFSStateConfig.STATE_BACKEND_PANGU_ROOT_DIR[1]] = (
                ConfigHelper.get_cp_pangu_root_dir(self.config))
        else:
            self.config[HDFSStateConfig.STATE_BACKEND_DFS_CLUSTER_NAME[1]] = (
                ConfigHelper.get_cp_dfs_cluster_name(self.config))
            self.config[HDFSStateConfig.STATE_BACKEND_DFS_STORE_DIR[1]] = (
                ConfigHelper.get_cp_dfs_root_dir(self.config))
            logger.info(self.config)

    def __contains_hdfs_prefix(self, key):
        return "DFS" in key or "HDFS" in key or "PANGU" in key or "OSS" in key

    def __generate_property_func(self):
        for k, v in vars(HDFSStateConfig).items():
            if k.startswith("STATE_BACKEND") and self.__contains_hdfs_prefix(
                    k):
                logger.debug("Set function {} {}.".format(k, v))
                if not hasattr(self, k.lower()):
                    setattr(
                        self, k.lower(), v[0](self.config[v[1]])
                        if v[1] in self.config else v[2])
