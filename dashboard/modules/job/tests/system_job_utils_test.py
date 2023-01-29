import logging
import json

from ray.new_dashboard.modules.job.system_job_utils import (
    load_cluster_config_from_url, )

logger = logging.getLogger(__name__)


def test_load_cluster_config_from_url():
    MOCK_CONFIG_URL = "http://alipay-kepler-resource.cn-hangzhou.alipay.aliyun-inc.com/ray-cluster-config-dev/systemjob-test.yaml"  # noqa: E501
    cluster_config = load_cluster_config_from_url(MOCK_CONFIG_URL)
    print("cluster_config is ", cluster_config)
    assert "system_jobs" in cluster_config
    system_jobs_conf = json.loads(cluster_config["system_jobs"])

    assert system_jobs_conf["enabled"]
    assert system_jobs_conf["system_namespace"] == "4c8g"
    assert "jobs" in system_jobs_conf
    jobs_conf = system_jobs_conf["jobs"]
    assert len(jobs_conf) == 3
    assert "actor-optimizer" in jobs_conf
    assert "actor-insights" in jobs_conf
    assert "actor-observer" in jobs_conf
