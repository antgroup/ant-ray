# coding: utf-8
import os
import sys
import time
import pytest
from pathlib import Path
import ray


@ray.remote
class Counter(object):
    def __init__(self):
        self.value = 0
        ray.put(self.value)

    def increment(self):
        self.value += 1
        return self.value

    def get_counter(self):
        return self.value


def test_create_runtime_env_failed(ray_start_cluster):
    cluster = ray_start_cluster
    directory = os.path.dirname(os.path.realpath(__file__))
    setup_worker_path = os.path.join(directory, "mock_failure_setup_worker.py")
    cluster.add_node(
        num_cpus=1,
        setup_worker_path=setup_worker_path,
        _system_config={
            "worker_register_timeout_seconds": 5,
            "worker_register_timeout_max_retries": 4
        })
    job_config = ray.job_config.JobConfig(
        runtime_env={"env_vars": {
            "a": "b",
        }})
    ray.init(address=cluster.address, job_config=job_config)

    a1 = Counter.options().remote()
    obj_ref1 = a1.increment.remote()
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(obj_ref1, timeout=120)

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(a1.get_counter.remote(), timeout=15)

    a2 = Counter.options().remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(a2.get_counter.remote(), timeout=15)


def test_normal_actor_worker_failed(ray_start_cluster):
    cluster = ray_start_cluster
    directory = os.path.dirname(os.path.realpath(__file__))
    setup_worker_path = os.path.join(directory, "mock_failure_setup_worker.py")
    cluster.add_node(
        num_cpus=1,
        worker_path=setup_worker_path,
        _system_config={
            "worker_register_timeout_seconds": 5,
        })
    job_config = ray.job_config.JobConfig()
    ray.init(address=cluster.address, job_config=job_config)

    a1 = Counter.options().remote()
    obj_ref1 = a1.increment.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj_ref1, timeout=20)


def test_actor_fo(ray_start_cluster):
    cluster = ray_start_cluster
    directory = os.path.dirname(os.path.realpath(__file__))
    setup_worker_path = os.path.join(directory, "mock_failure_setup_worker.py")
    cluster.add_node(
        num_cpus=1,
        setup_worker_path=setup_worker_path,
        _system_config={
            "worker_register_timeout_seconds": 5,
        })
    mock_file = os.path.join(directory, "mock_file.txt")
    job_config = ray.job_config.JobConfig(
        runtime_env={"env_vars": {
            "mock_file": mock_file,
        }})
    ray.init(address=cluster.address, job_config=job_config)

    create_file(Path(mock_file))

    a1 = Counter.options(max_restarts=-1).remote()
    ray.get(a1.get_counter.remote(), timeout=10)
    os.remove(mock_file)
    ray.kill(a1, no_restart=False)
    time.sleep(1)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        assert ray.get(a1.get_counter.remote(), timeout=25) == 0
    create_file(Path(mock_file))
    assert ray.get(a1.get_counter.remote(), timeout=25) == 0
    os.remove(mock_file)


def create_file(p):
    if not p.parent.exists():
        p.parent.mkdir(parents=True)
    with p.open("w") as f:
        f.write("Test")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
