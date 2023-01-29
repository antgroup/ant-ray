from raystreaming.constants import StreamingConstants
from raystreaming.utils import EnvUtil


class Config:
    STREAMING_JOB_NAME = "streaming.job.name"
    STREAMING_JOB_NAME_ALIAS = "job_name"
    STREAMING_OP_NAME = "streaming.op_name"
    STREAMING_WORKER_NAME = "streaming.worker_name"
    # channel
    CHANNEL_TYPE = "channel_type"
    MEMORY_CHANNEL = "memory_channel"
    NATIVE_CHANNEL = "native_channel"
    CHANNEL_SIZE = "channel_size"
    CHANNEL_SIZE_DEFAULT = 10**8
    IS_RECREATE = "streaming.is_recreate"
    # return from StreamingReader.getBundle if only empty message read in this
    # interval.
    TIMER_INTERVAL_MS = "timer_interval_ms"
    READ_TIMEOUT_MS = "read_timeout_ms"
    DEFAULT_READ_TIMEOUT_MS = "1000"
    # write an empty message if there is no data to be written in this
    # interval.
    STREAMING_EMPTY_MESSAGE_INTERVAL = "streaming.empty_message_interval"

    # operator type
    OPERATOR_TYPE = "operator_type"

    # flow control
    FLOW_CONTROL_TYPE = "streaming.flow_control_type"
    WRITER_CONSUMED_STEP = "streaming.writer.consumed_step"
    READER_CONSUMED_STEP = "streaming.reader.consumed_step"

    # This flag controls whether "dynamic rebalance" replaces "forward".
    ENABLE_DYNAMIC_REBALANCE = "streaming.partition.enable_dr"
    ENABLE_DYNAMIC_REBALANCE_DEFAULT = True

    # The maximum number of consecutive "dynamic rebalance" sends.
    DYNAMIC_REBALANCE_BATCH_SIZE = "streaming.partition.batch_size"
    DYNAMIC_REBALANCE_BATCH_SIZE_DEFAULT = 50

    # The switch for rescaling's update-context override
    UPDATE_CONTEXT_OVERRIDE_ENABLE = \
        "streaming.worker.update-context.override.enable"

    # Metrics
    METRIC_WORKER_SAMPLE_FREQUENCY = "streaming.metric.worker.sample.frequency"


class ConfigHelper(object):
    @staticmethod
    def is_save_checkpoint_async(conf):
        mode = ConfigHelper.get_save_checkpoint_mode(conf)
        return StreamingConstants.SAVE_CHECKPOINT_ASYNC == mode

    @staticmethod
    def is_save_checkpoint_async_py(conf):
        mode = ConfigHelper.get_save_checkpoint_mode_py(conf)
        return StreamingConstants.SAVE_CHECKPOINT_ASYNC_PY == mode

    @staticmethod
    def get_save_checkpoint_mode(conf):
        value = conf.get(StreamingConstants.SAVE_CHECKPOINT_MODE)
        if value is not None:
            return value
        return StreamingConstants.SAVE_CHECKPOINT_SYNC

    @staticmethod
    def get_save_checkpoint_mode_py(conf):
        value = conf.get(StreamingConstants.SAVE_CHECKPOINT_MODE_PY)
        if value is not None:
            return value
        return StreamingConstants.SAVE_CHECKPOINT_SYNC_PY

    @staticmethod
    def get_cp_pangu_cluster_name(conf):
        value = conf.get(StreamingConstants.CP_PANGU_CLUSTER_NAME)
        if value is not None:
            return value
        return StreamingConstants.CP_PANGU_CLUSTER_NAME_DEFAULT

    @staticmethod
    def get_cp_pangu_root_dir(conf):
        value = conf.get(StreamingConstants.CP_PANGU_ROOT_DIR)
        if value is not None:
            return value
        return StreamingConstants.CP_PANGU_ROOT_DIR_DEFAULT

    @staticmethod
    def get_cp_dfs_cluster_name(conf):
        value = conf.get(StreamingConstants.CP_DFS_CLUSTER_NAME)
        if value is not None:
            return value
        return StreamingConstants.CP_DFS_CLUSTER_NAME_DEFAULT

    @staticmethod
    def get_cp_dfs_root_dir(conf):
        value = conf.get(StreamingConstants.CP_DFS_ROOT_DIR)
        if value is not None:
            return value
        return StreamingConstants.CP_DFS_ROOT_DIR_DEFAULT

    @staticmethod
    def get_cp_pangu_user_mysql_url(conf):
        value = conf.get(StreamingConstants.CP_PANGU_USER_MYSQL_URL)
        if value is not None:
            return value
        return StreamingConstants.CP_PANGU_USER_MYSQL_URL_DEFAULT

    @staticmethod
    def get_cp_local_disk_root_dir(conf):
        value = conf.get(StreamingConstants.CP_LOCAL_DISK_ROOT_DIR)
        if value is not None:
            return value
        return StreamingConstants.CP_LOCAL_DISK_ROOT_DIR_DEFAULT

    @staticmethod
    def get_cp_state_backend_type(conf):
        value = conf.get(StreamingConstants.CP_STATE_BACKEND_TYPE)
        if value is not None:
            return value
        return StreamingConstants.CP_STATE_BACKEND_DEFAULT

    @staticmethod
    def get_metrics_url(conf):
        value = conf.get(StreamingConstants.METRICS_URL)
        if value is not None:
            return value

        if EnvUtil.is_online_env():
            return StreamingConstants.METRICS_URL_ONLINE
        else:
            return StreamingConstants.METRICS_URL_DEV

    @staticmethod
    def get_metrics_user_name(conf):
        value = conf.get(StreamingConstants.METRICS_USER_NAME)
        if value is not None:
            return value

        if EnvUtil.is_online_env():
            return StreamingConstants.METRICS_USER_NAME_ONLINE
        else:
            return StreamingConstants.METRICS_USER_NAME_DEV

    @staticmethod
    def get_metrics_type(conf):
        value = conf.get(StreamingConstants.METRICS_TYPE)
        if value is not None:
            return value
        return StreamingConstants.METRICS_TYPE_DEFAULT

    @staticmethod
    def get_buffer_pool_size(conf):
        if StreamingConstants.BUFFER_POOL_SIZE in conf:
            buffer_pool_size = int(conf[StreamingConstants.BUFFER_POOL_SIZE])
        else:
            queue_size = int(
                conf.get(StreamingConstants.QUEUE_SIZE,
                         StreamingConstants.QUEUE_SIZE_DEFAULT))
            buffer_pool_size = int(
                queue_size * StreamingConstants.BUFFER_POOL_QUEUE_RATIO)
        assert buffer_pool_size < 2**32 - 1
        return buffer_pool_size

    @staticmethod
    def get_buffer_pool_min_buffer_size(conf):
        if StreamingConstants.BUFFER_POOL_MIN_BUFFER_SIZE in conf:
            return int(conf[StreamingConstants.BUFFER_POOL_MIN_BUFFER_SIZE])
        else:
            max_buffers = int(
                conf.get(StreamingConstants.BUFFER_POOL_MAX_BUFFERS,
                         StreamingConstants.BUFFER_POOL_MAX_BUFFERS_DEFAULT))
            buffer_pool_size = ConfigHelper.get_buffer_pool_size(conf)
            min_buffer_size = buffer_pool_size // max_buffers
            assert min_buffer_size < 2**32 - 1
            return min_buffer_size
