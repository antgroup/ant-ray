import logging
import os
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.sdk import get_cluster_status, request_cluster_resources
from ray.autoscaler.v2.tests.util import MockEventLogger
from ray.cluster_utils import Cluster

logger = logging.getLogger(__name__)

DEFAULT_AUTOSCALING_CONFIG = {
    "cluster_name": "fake_multinode",
    "max_workers": 8,
    "provider": {
        "type": "fake_multinode",
    },
    "available_node_types": {
        "ray.head.default": {
            "resources": {
                "CPU": 0,
            },
            "max_workers": 0,
            "node_config": {},
        },
        "ray.worker.group1": {
            "resources": {
                "CPU": 4,
                "memory": 8 * 1024**3
            },
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
            "ray_node_type": "4c8g"
        },
        "ray.worker.group2": {
            "resources": {
                "CPU": 8,
                "memory": 16 * 1024**3
            },
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
            "ray_node_type": "8c16g"
        },
    },
    "head_node_type": "ray.head.default",
    "upscaling_speed": 0,
    "idle_timeout_minutes": 1,
}


@pytest.fixture(scope="function")
def make_autoscaler():
    ctx = {}

    def _make_autoscaler(config):
        head_node_kwargs = {
            "env_vars": {
                "RAY_CLOUD_INSTANCE_ID": FAKE_HEAD_NODE_ID,
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID,
                "RAY_NODE_TYPE_NAME": "ray.head.default",
            },
            "num_cpus": config["available_node_types"]["ray.head.default"]["resources"][
                "CPU"
            ],
        }
        cluster = Cluster(
            initialize_head=True, head_node_args=head_node_kwargs, connect=True
        )
        ctx["cluster"] = cluster

        mock_config_reader = MagicMock()
        gcs_address = cluster.address

        # Configs for the node provider
        config["provider"]["gcs_address"] = gcs_address
        config["provider"]["head_node_id"] = FAKE_HEAD_NODE_ID
        config["provider"]["launch_multiple"] = True
        os.environ["RAY_FAKE_CLUSTER"] = "1"
        os.environ["VIRTUAL_CLUSTER_ENABLED"] = "true"
        mock_config_reader.get_cached_autoscaling_config.return_value = (
            AutoscalingConfig(configs=config, skip_content_hash=True)
        )
        gcs_address = gcs_address
        gcs_client = GcsClient(gcs_address)

        event_logger = AutoscalerEventLogger(MockEventLogger(logger))

        autoscaler = Autoscaler(
            session_name="test",
            config_reader=mock_config_reader,
            gcs_client=gcs_client,
            event_logger=event_logger,
        )

        return autoscaler, cluster

    yield _make_autoscaler
    try:
        ray.shutdown()
        ctx["cluster"].shutdown()
    except Exception:
        logger.exception("Error during teardown")
        # Run ray stop to clean up everything
        subprocess.run(
            ["ray", "stop", "--force"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )


def test_basic_scaling(make_autoscaler):
    config = DEFAULT_AUTOSCALING_CONFIG
    autoscaler, cluster = make_autoscaler(DEFAULT_AUTOSCALING_CONFIG)
    gcs_address = autoscaler._gcs_client.address

    # Resource requests
    print("=================== Test scaling up constraint 1/2====================")
    request_cluster_resources(gcs_address, [{"CPU": 4}])

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 2
        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test scaling up again with tasks
    print("=================== Test scaling up with tasks ====================")

    @ray.remote
    def task():
        time.sleep(999)

    task.options(num_cpus=1).remote()
    task.options(num_cpus=0, num_gpus=1).remote()

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 3
        return True

    wait_for_condition(verify, retry_interval_ms=2000)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))