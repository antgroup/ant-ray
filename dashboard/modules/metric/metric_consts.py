import ray.ray_constants as ray_constants

# Default reporter interval value is 30 seconds.
METRIC_REPORT_INTERVAL_SECONDS = ray_constants.env_integer(
    "METRIC_REPORT_INTERVAL_SECONDS", 30)

CORE_WORKER_COMPONENT = "core_worker"

# Resouces Metrics
WORKER_ACQUIRED_MEM = "ray.core_worker.acquired_mem_bytes"

# System metrics: dashboard
DASHBOARD_CPU_PERCENT = "ray.dashboard.cpu_percent"
DASHBOARD_MEM_RSS = "ray.dashboard.mem_rss"
DASHBOARD_MEM_UTIL = "ray.dashboard.mem_util"

# Sytem metrics: gcs
GCS_CPU_PERCENT = "ray.gcs_server.cpu_percent"
GCS_MEM_RSS = "ray.gcs_server.mem_rss"
GCS_MEM_UTIL = "ray.gcs_server.mem_util"
