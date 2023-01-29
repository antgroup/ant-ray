import os
import ray

DEFAULT_EVENT_URL = "https://r.alipay.com/index.htm#/logQuery?deployName=arconkube&logFullPath=/home/admin/logs/ray-logs/logs/events/event*log&project=arconkube-eu95-prod&region=eu95"  # noqa: E501
DEFAULT_METRIC_URL = "https://dashboard-kepler.alipay.com"
DOWNLOAD_CONFIG_TIMEOUT_SECONDS = 60
DOWNLOAD_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
UPDATE_FRONTEND_FILENAME = "frontend.zip"
RAY_FEATURES_FILE = os.path.join(
    os.path.dirname(ray.__file__), "ray_features.yaml")
