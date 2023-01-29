class StreamingConstants:
    """
    streaming constants
    """

    # checkpoint
    JOB_WORKER_CONTEXT_KEY = "jobworker_context_"
    CONTEXT_DELIMITER = "_"
    GLOBAL_CONTEXT_DELIMITER = "_global_"
    PARTIAL_CONTEXT_DELIMITER = "_partial_"

    # health check
    HEALTH_CHECK_INTERVAL_SECS = 1

    # queue
    QUEUE_TYPE = "queue_type"
    MEMORY_QUEUE = "memory_queue"
    MOCK_QUEUE = "mock_queue"
    STREAMING_QUEUE = "streaming_queue"
    QUEUE_SIZE = "queue_size"
    QUEUE_SIZE_DEFAULT = 10**8
    QUEUE_PULL_TIMEOUT = "queue_pull_timeout"
    QUEUE_PULL_TIMEOUT_DEAULT = 300000
    BUFFER_POOL_SIZE = "streaming.queue.buffer-pool.size"
    BUFFER_POOL_QUEUE_RATIO = 1.1
    BUFFER_POOL_MIN_BUFFER_SIZE = "streaming.queue.buffer-pool.buffer.size.min"
    BUFFER_POOL_MAX_BUFFERS = "streaming.queue.buffer-pool.buffers.max"
    BUFFER_POOL_MAX_BUFFERS_DEFAULT = 5

    OPERATOR_TYPE = "operator_type"
    OPERATOR_COMMAND_FUNC_NAME = "func_name"
    OPERATOR_COMMAND_FUNC_ARGS = "func_args"

    # reliability level
    RELIABILITY_LEVEL_COMPATIBLE = "reliability_level"
    RELIABILITY_LEVEL = "streaming.reliability.level"
    EXACTLY_SAME = "EXACTLY_SAME"
    EXACTLY_ONCE = "EXACTLY_ONCE"
    AT_LEAST_ONCE = "AT_LEAST_ONCE"
    REQUEST_ROLLBACK_RETRY_TIMES = 3

    # checkpoint prefix key
    JOB_WORKER_OP_CHECKPOINT_PREFIX_KEY = "jobwk_op_"

    # cp mode
    SAVE_CHECKPOINT_MODE = "save_checkpoint_mode"
    SAVE_CHECKPOINT_SYNC = "save_checkpoint_sync"
    SAVE_CHECKPOINT_ASYNC = "save_checkpoint_async"
    SAVE_CHECKPOINT_MODE_PY = "save_checkpoint_mode_py"
    SAVE_CHECKPOINT_SYNC_PY = "save_checkpoint_sync_py"
    SAVE_CHECKPOINT_ASYNC_PY = "save_checkpoint_async_py"

    # state backend
    CP_STATE_BACKEND_TYPE = "cp_state_backend_type"
    CP_STATE_BACKEND_MEMORY = "cp_state_backend_memory"
    CP_STATE_BACKEND_PANGU = "cp_state_backend_pangu"
    CP_STATE_BACKEND_DFS = "cp_state_backend_dfs"
    CP_STATE_BACKEND_DEFAULT = CP_STATE_BACKEND_MEMORY

    # pangu
    CP_PANGU_CLUSTER_NAME = "cp_pangu_cluster_name"
    CP_PANGU_CLUSTER_NAME_DEFAULT = "pangu://pangu1_analyze_sata_em14_online"
    CP_PANGU_ROOT_DIR = "cp_pangu_root_dir"
    CP_PANGU_ROOT_DIR_DEFAULT = "/arc/streaming/python/"
    CP_PANGU_USER_MYSQL_URL = "cp_pangu_user_mysql_url"
    CP_PANGU_USER_MYSQL_URL_DEFAULT = "jdbc:mysql://10.210.178.137:3306/alipay_pangu_users?user=pangu_user&password=pangu_user&useUnicode=true&characterEncoding=UTF8"  # noqa: E501

    # dfs
    CP_DFS_CLUSTER_NAME = "cp_dfs_cluster_name"
    CP_DFS_CLUSTER_NAME_DEFAULT = "dfs://f-antq-test.aliyun-inc.test:10290"
    CP_DFS_ROOT_DIR = "cp_dfs_root_dir"
    CP_DFS_ROOT_DIR_DEFAULT = "/arc/streaming/python/"

    # local disk
    CP_LOCAL_DISK_ROOT_DIR = "cp_local_disk_root_dir"
    CP_LOCAL_DISK_ROOT_DIR_DEFAULT = "/tmp"

    # metrics
    METRIC_PREFIX = "alipay_stm."
    METRIC_WK_PREFIX = "alipay_stm.worker."
    METRIC_WK_BP_RATIO_PREFIX = "alipay_stm.worker_queue."
    METRICS_TYPE = "metrics_type"
    METRICS_PROMETHEUS = "prometheus"
    METRICS_KMONITOR = "kmonitor"
    METRICS_RAY_METRIC = "ray_metric"
    METRICS_LOCAL = "local"
    METRICS_TYPE_DEFAULT = METRICS_KMONITOR
    METRICS_LABEL_CATEGORY_SCOPE = "scope"
    METRICS_LABEL_JOB_NAME = "job_name"
    METRICS_LABEL_HOST_IP = "host"
    METRICS_LABEL_OP_NAME = "op_name"
    METRICS_LABEL_WORKER_ID = "worker_id"
    METRICS_LABEL_WORKER_NAME = "worker_name"
    METRICS_LABEL_PID = "pid"
    METRICS_LABEL_QUEUE_ID = "queue_id"
    METRICS_SCOPE_CATEGORY = "scope"
    METRICS_SCOPE_CATEGORY_MASTER = "0"
    METRICS_SCOPE_CATEGORY_WORKER = "1"
    METRICS_CATEGORY_COUNTER = 0
    METRICS_CATEGORY_GAUGE = 1
    METRICS_CATEGORY_METER = 2
    METRICS_WORKER_LABEL_SIZE = 6
    METRICS_REPORT_INTERVAL = 10
    METRICS_ENABLE_KEY = "METRICS_ENABLE"
    METRICS_ENABLE_DEFAULT = "True"  # a string

    # prometheus metrics
    METRICS_JOB = "streaming"
    METRICS_URL = "metrics_url"
    METRICS_URL_DEV = "http://10.101.82.5:7200/prom"
    METRICS_URL_ONLINE = "http://lookout-gw-pool.global.alipay.com:7200/prom"
    METRICS_USER_NAME = "metrics_user_name"
    METRICS_USER_NAME_DEV = "4Jf9GRMlXd8yXtCpJFEksOHLExhQrnel"
    METRICS_USER_NAME_ONLINE = "vmDR7d7qDqfBPlRYGe0amuFHg5wnOvGm"
    METRICS_USER_PASSWD = ""
    METRICS_STEP = "step"
    METRICS_APP = "app"
    METRICS_APP_NAME = "streaming-app"

    # kmonitor conf dir env key
    ENV_KMONITOR_CPP_CONF_DIR = "KMONITOR_CPP_CONF"

    # Alipay app online env
    ENV_ALIPAY_APP_ENV = "ALIPAY_APP_ENV"

    # job master/worker context
    JOB_NAME = "job_name"
    WORKER_ID = "worker_id"

    # log
    LOG_DIRS_ENV_KEY = "STREAMING_LOG_DIR"
    LOG_LEVEL_ENV_KEY = "STREAMING_RUNTIME_LOG_LEVEL"
    LOG_MAX_BYTES_ENV_KEY = "STREAMING_RUNTIME_LOG_MAX_BYTES"
    LOG_BACKUP_COUNT_ENV_KEY = "STREAMING_RUNTIME_LOG_BACKUP_COUNT"

    LOG_ROOT_DIR_DEFAULT = "/tmp/ray_streaming_logs/"
    LOG_LEVEL_STR_DEFAULT = "INFO"
    LOG_MAX_BYTES_DEFAULT = 500 * 1024 * 1024
    LOG_BACKUP_COUNT_DEFAULT = 5

    LOGGER_NAME_DEFAULT = "streaming_runtime_python"
    LOG_FILE_NAME_DEFAULT = "streaming_runtime_python.log"
    LOG_ENCODING = "utf-8"
    LOG_FORMAT = "%(asctime)-15s %(levelname)s %(module)s %(filename)s %(lineno)d %(process)d|%(message)s"  # noqa: E501
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
