from ray.ray_constants import env_integer
import os
import ray

DASHBOARD_AGENT_PORT_PREFIX = "DASHBOARD_AGENT_PORT_PREFIX:"
DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_SECONDS = 2

RETRY_REDIS_CONNECTION_TIMES = 10
UPDATE_NODES_INTERVAL_SECONDS = 5
UPDATE_AGENTS_INTERVAL_SECONDS = env_integer("UPDATE_AGENTS_INTERVAL_SECONDS",
                                             5)
CONNECT_REDIS_INTERNAL_SECONDS = 2
ORGANIZE_DATA_INTERVAL_SECONDS = env_integer("ORGANIZE_DATA_INTERVAL_SECONDS",
                                             5)
LOG_INFO_INTERVAL_SECONDS = env_integer("LOG_INFO_INTERVAL_SECONDS", 60 * 60)
REPORT_METRICS_TIMEOUT_SECONDS = env_integer("REPORT_METRICS_TIMEOUT_SECONDS",
                                             10)
REPORT_METRICS_INTERVAL_SECONDS = 10
DASHBOARD_AGENT_HTTP_PORT = "httpPort"
DASHBOARD_AGENT_GRPC_PORT = "grpcPort"
DASHBOARD_AGENT_TIMESTAMP = "timestamp"
# NodeUpdater
RESERVED_DEAD_NODES_COUNT_PER_NODE = 8
RETRY_GET_ALL_NODE_INFO_INTERVAL_SECONDS = 2
FULL_NODE_INFO_CHANNEL = "FULL_NODE_INFO"
# GRPC options
GLOBAL_GRPC_OPTIONS = (
    ("grpc.max_message_length", 512 * 1024 * 1024),
    ("grpc.max_send_message_length", 512 * 1024 * 1024),
    ("grpc.max_receive_message_length", 512 * 1024 * 1024),
    ("grpc.enable_http_proxy", 0),
)
# GCS check alive
GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR = env_integer(
    "GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR", 10)
GCS_CHECK_ALIVE_INTERVAL_SECONDS = env_integer(
    "GCS_CHECK_ALIVE_INTERVAL_SECONDS", 5)
GCS_RETRY_CONNECT_INTERVAL_SECONDS = env_integer(
    "GCS_RETRY_CONNECT_INTERVAL_SECONDS", 2)
# aiohttp_cache
DEFAULT_AIOHTTP_CACHE_TTL_SECONDS = env_integer(
    "DEFAULT_AIOHTTP_CACHE_TTL_SECONDS", 30)
DEFAULT_AIOHTTP_CACHE_MAX_SIZE = env_integer("DEFAULT_AIOHTTP_CACHE_MAX_SIZE",
                                             128)
AIOHTTP_CACHE_DISABLE_ENVIRONMENT_KEY = "RAY_DASHBOARD_NO_CACHE"
# Redis key
REDIS_KEY_DASHBOARD_RPC = "api_server_rpc"
REDIS_KEY_GCS_SERVER_ADDRESS = "GcsServerAddress"
REDIS_KEY_CLUSTER_UNIQUE_TOKEN = "ClusterUniqueToken"
# Redis check alive
REDIS_CHECK_ALIVE_CHANNEL = "_CHECK_ALIVE"
REDIS_CHECK_ALIVE_MAX_NUM_OF_UNRECEIVED_MESSAGES = env_integer(
    "REDIS_CHECK_ALIVE_MAX_NUM_OF_UNRECEIVED_MESSAGES", 10)
REDIS_CHECK_ALIVE_INTERVAL_SECONDS = env_integer(
    "REDIS_CHECK_ALIVE_INTERVAL_SECONDS", 5)
# Named signals
SIGNAL_GCS_RECONNECTED = "gcs_reconnected"
SIGNAL_NODE_INFO_FETCHED = "node_info_fetched"
SIGNAL_NODE_SUMMARY_FETCHED = "node_summary_fetched"
SIGNAL_JOB_INFO_FETCHED = "job_info_fetched"
SIGNAL_WORKER_INFO_FETCHED = "worker_info_fetched"
SIGNAL_DASHBOARD_RESTARTED = "dashboard_restarted"
# Log filename
DASHBOARD_LOG_FILENAME = "dashboard.log"
DASHBOARD_AGENT_LOG_FILENAME = "dashboard_agent.log"
# Default param for RotatingFileHandler
LOGGING_ROTATE_BYTES = 128 * 1024 * 1024  # maxBytes
LOGGING_ROTATE_BACKUP_COUNT = 1  # backupCount

DEFAULT_LOG_URL = "http://mpaasweb.jr.alipay.net/v2/index.htm#/logQuery?deployName=arconkube"  # noqa: E501

EVENT_AGENT_CHECK_JAVA_HS_ERR_LOG_DELAY_SECONDS = 5
EVENT_AGENT_CHECK_JAVA_HS_ERR_LOG_LINE_NUMBER = 30

# check signature
AIOHTTP_CHECK_SIGNATURE_KEY = "RAY_DASHBOARD_CHECK_SIGNATURE"

DASHBOARD_PUBLIC_KEY = os.path.join(
    os.path.dirname(ray.__file__), "new_dashboard/public_key.pem")

# Subscribing
RESTARTABLE_TASK_AUTO_RESTART_INTERVAL_SECONDS = 2

# Web access logging
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", "UNKOWN_CLUSTER")
WEB_ACCESS_LOG_FORMAT = "%a %t \"%r\" %s %b \"%{Referer}i\" " + \
    "\"%{User-Agent}i\" %D " + CLUSTER_NAME
