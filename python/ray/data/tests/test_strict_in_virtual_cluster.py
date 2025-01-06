import logging
import sys
import time

import pytest
import requests

import ray
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.test_utils import (
    wait_for_condition,
)
from ray.cluster_utils import Cluster
from ray.dashboard.tests.conftest import *  # noqa
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import DataContext
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.data.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)


@ray.remote
class JobSignalActor:
    def __init__(self):
        self._ready = False
        self._data = None

    def is_ready(self):
        return self._ready

    def ready(self):
        self._ready = True

    def unready(self):
        self._ready = False

    def data(self, data=None):
        if data is not None:
            self._data = data
        return self._data


def submit_job_to_virtual_cluster(job_client, tmp_dir, driver_script, virtual_cluster_id):
    path = Path(tmp_dir)
    test_script_file = path / "test_script.py"
    with open(test_script_file, "w+") as file:
        file.write(driver_script)

    runtime_env = {"working_dir": tmp_dir}
    runtime_env = upload_working_dir_if_needed(
        runtime_env, tmp_dir, logger=logger
    )
    runtime_env = RuntimeEnv(**runtime_env).to_dict()

    job_id = job_client.submit_job(
        entrypoint="python test_script.py",
        entrypoint_memory=1,
        runtime_env=runtime_env,
        virtual_cluster_id=virtual_cluster_id,
    )
    return job_id


@pytest.mark.parametrize('create_virtual_cluster', [{
    'node_instances': [("1c2g", 2), ("2c4g", 2), ("8c16g", 4)],
    'virtual_cluster': {
        "VIRTUAL_CLUSTER_0": {
            "allocation_mode": "mixed",
            "replica_sets": {
                "1c2g": 2,
            },
        },
        "VIRTUAL_CLUSTER_1": {
            "allocation_mode": "mixed",
            "replica_sets": {
                "2c4g": 2,
            },
        },
        "VIRTUAL_CLUSTER_2": {
            "allocation_mode": "mixed",
            "replica_sets": {
                "8c16g": 4,
            },
        }
    }
}], indirect=True)
def test_auto_parallelism(create_virtual_cluster):
    cluster, job_client = create_virtual_cluster
    MiB = 1024 * 1024
    GiB = 1024 * MiB
    TEST_CASES = [
        (1024, (4, 8, 64)),         # avail_cpus * 2
        (10 * MiB, (10, 10, 64)),   # MAX_PARALLELISM, MAX_PARALLELISM, avail_cpus * 2
        (1 * GiB, (200, 200, 200)), # MIN_PARALLELISM, MIN_PARALLELISM, MIN_PARALLELISM
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        for i in range(3):  # 3 virtual clusters in total
            signal_actor_name = f"storage_actor_{i}"
            signal_actor = JobSignalActor.options(
                name=signal_actor_name, namespace="storage", num_cpus=0
            ).remote()
            ray.get(signal_actor.is_ready.remote())
            for data_size, expected_parallelism in TEST_CASES:
                driver_script = """
import ray
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import DataContext


ray.init(address="auto")
signal_actor = ray.get_actor("{signal_actor_name}", namespace="storage")
ray.get(signal_actor.ready.remote())
target_max_block_size = DataContext.get_current().target_max_block_size
class MockReader:
    def estimate_inmemory_data_size(self):
        return {data_size}


final_parallelism, _, _ = _autodetect_parallelism(
    parallelism=-1,
    target_max_block_size=target_max_block_size,
    ctx=DataContext.get_current(),
    datasource_or_legacy_reader=MockReader(),
)

ray.get(signal_actor.data.remote(final_parallelism))
ray.get(signal_actor.unready.remote())
"""
                driver_script = driver_script.format(
                    signal_actor_name=signal_actor_name,
                    data_size=data_size)
                submit_job_to_virtual_cluster(job_client, tmp_dir, driver_script, f"VIRTUAL_CLUSTER_{i}")
                wait_for_condition(
                    lambda: ray.get(signal_actor.is_ready.remote()), timeout=30
                )
                wait_for_condition(
                    lambda: not ray.get(signal_actor.is_ready.remote()), timeout=30
                )
                res = ray.get(signal_actor.data.remote())
                print(f"Driver detected parallelism: {res}, expect[{i}]: {expected_parallelism[i]}")
                wait_for_condition(
                    lambda: ray.get(signal_actor.data.remote()) == expected_parallelism[i], timeout=30
                )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
