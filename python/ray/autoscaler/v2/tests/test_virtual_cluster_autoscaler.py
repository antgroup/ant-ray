import copy
import logging
import os
import subprocess
import sys
import tempfile
import time
from unittest.mock import MagicMock

import pytest

import ray
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray._raylet import GcsClient
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.sdk import (
    get_cluster_resource_state,
    get_cluster_status,
    request_cluster_resources
)
from ray.autoscaler.v2.tests.util import MockEventLogger
from ray.cluster_utils import Cluster
from ray.job_submission import JobStatus, JobSubmissionClient

import requests
from typing import Dict

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
        "1c2g": {
            "resources": {"CPU": 1},
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        },
        "2c4g": {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        },
    },
    "head_node_type": "ray.head.default",
    "upscaling_speed": 0,
    "idle_timeout_minutes": 0.2,  # ~12 sec
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
            "_system_config": {
                "enable_autoscaler_v2": True,
                "virtual_cluster_enabled": True,
            },
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


# Test autoscaler can get right cluster resource state (including virtual clusters).
def test_get_cluster_resource_state(make_autoscaler):
    config = copy.deepcopy(DEFAULT_AUTOSCALING_CONFIG)
    config["idle_timeout_minutes"] = 10
    autoscaler, cluster = make_autoscaler(config)

   