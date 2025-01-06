import logging
import os
import socket
import time

import pytest
import requests

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


def submit_job(driver_script, virtual_cluster_id):
    test_script_file = path / "test_script.py"
    with open(test_script_file, "w+") as file:
        file.write(driver_script)

    runtime_env = {"working_dir": tmp_dir}
    runtime_env = upload_working_dir_if_needed(
        runtime_env, tmp_dir, logger=logger
    )
    runtime_env = RuntimeEnv(**runtime_env).to_dict()

    job_id = head_client.submit_job(
        entrypoint="python test_script.py",
        entrypoint_memory=1,
        runtime_env=runtime_env,
        virtual_cluster_id=virtual_cluster_id,
        replica_sets={TEMPLATE_ID_PREFIX + str(i): 2},
    )

@pytest.mark.parametrize('create_virtual_cluster', [{
    'node_instances': [("1c2g", 2), ("2c4g", 2), ("8c16g", 4)],
    'virtual_cluster': {
        "VIRTUAL_CLUSTER_0": {
            "allocation_mode": "mixed",
            "replica_sets": {
                "1c2g": 2,
                "2c4g": 2,
            },
        },
        "VIRTUAL_CLUSTER_1": {
            "allocation_mode": "mixed",
            "replica_sets": {
                "8c16g": 4,
            },
        }
    }
}], indirect=True)
def test_read_data_in_certain_virtual_cluster(
    disable_aiohttp_cache, create_virtual_cluster
):
    pass
