# coding: utf-8
import logging
import os
import sys
import time

import numpy as np
import pytest

import ray.cluster_utils
from ray.test_utils import (
    dicts_equal,
    wait_for_pid_to_exit,
)

import ray

logger = logging.getLogger(__name__)


def test_many_fractional_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=2, resources={"Custom": 2})

    @ray.remote
    def g():
        return 1

    @ray.remote
    def f(block, accepted_resources):
        true_resources = {
            resource: value[0][1]
            for resource, value in ray.worker.get_resource_ids().items()
        }
        if block:
            ray.get(g.remote())
        return dicts_equal(true_resources, accepted_resources)

    # Check that the resource are assigned correctly.
    result_ids = []
    for rand1, rand2, rand3 in np.random.uniform(size=(100, 3)):
        resource_set = {"CPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_cpus=rand1))

        resource_set = {"CPU": 1, "GPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_gpus=rand1))

        resource_set = {"CPU": 1, "Custom": int(rand1 * 10000) / 10000}
        result_ids.append(
            f._remote([False, resource_set], resources={"Custom": rand1}))

        resource_set = {
            "CPU": int(rand1 * 10000) / 10000,
            "GPU": int(rand2 * 10000) / 10000,
            "Custom": int(rand3 * 10000) / 10000
        }
        result_ids.append(
            f._remote(
                [False, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3}))
        result_ids.append(
            f._remote(
                [True, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3}))
    assert all(ray.get(result_ids))

    # Check that the available resources at the end are the same as the
    # beginning.
    stop_time = time.time() + 10
    correct_available_resources = False
    while time.time() < stop_time:
        available_resources = ray.available_resources()
        if ("CPU" in available_resources
                and ray.available_resources()["CPU"] == 2.0
                and "GPU" in available_resources
                and ray.available_resources()["GPU"] == 2.0
                and "Custom" in available_resources
                and ray.available_resources()["Custom"] == 2.0):
            correct_available_resources = True
            break
    if not correct_available_resources:
        assert False, "Did not get correct available resources."


def test_background_tasks_with_max_calls(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def g():
        time.sleep(.1)
        return 0

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return [g.remote()]

    nested = ray.get([f.remote() for _ in range(10)])

    # Should still be able to retrieve these objects, since f's workers will
    # wait for g to finish before exiting.
    ray.get([x[0] for x in nested])

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return os.getpid(), g.remote()

    nested = ray.get([f.remote() for _ in range(10)])
    while nested:
        pid, g_id = nested.pop(0)
        ray.get(g_id)
        del g_id
        wait_for_pid_to_exit(pid)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_fair_queueing(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            "fair_queueing_enabled": True,
            # Having parallel leases is slow in this case
            # because tasks are scheduled FIFO,
            # the more parallism we have,
            # the more workers we need to start to execute f and g tasks
            # before we can execute the first h task.
            "max_pending_lease_requests_per_scheduling_category": 1
        })

    # ANT-INTERNAL: with gcs scheduling enabled, the following tasks' cpu has
    # to be 1 (instead of 0.01 by default), which is also the default config
    # of raylet scheduling. Otherwise, a large number of workers will be
    # raised, which breaks the testing logic.
    @ray.remote(num_cpus=1)
    def h():
        return 0

    @ray.remote(num_cpus=1)
    def g():
        return ray.get(h.remote())

    @ray.remote(num_cpus=1)
    def f():
        return ray.get(g.remote())

    # This will never finish without fair queueing of {f, g, h}:
    # https://github.com/ray-project/ray/issues/3644
    # The time cost of this test is mostly about 50s, and sometimes it will
    # exceed 60s, resulting in timeout. So I temporarily changed its timeout
    # to 90s.
    ready, _ = ray.wait(
        [f.remote() for _ in range(1000)], timeout=90.0, num_returns=1000)
    assert len(ready) == 1000, len(ready)


def test_job_id_consistency(ray_start_regular):
    @ray.remote
    def foo():
        return "bar"

    @ray.remote
    class Foo:
        def ping(self):
            return "pong"

    @ray.remote
    def verify_job_id(job_id, new_thread):
        def verify():
            current_task_id = ray.runtime_context.get_runtime_context().task_id
            assert job_id == current_task_id.job_id()
            obj1 = foo.remote()
            assert job_id == obj1.job_id()
            obj2 = ray.put(1)
            assert job_id == obj2.job_id()
            a = Foo.remote()
            assert job_id == a._actor_id.job_id
            obj3 = a.ping.remote()
            assert job_id == obj3.job_id()

        if not new_thread:
            verify()
        else:
            exc = []

            def run():
                try:
                    verify()
                except BaseException as e:
                    exc.append(e)

            import threading
            t = threading.Thread(target=run)
            t.start()
            t.join()
            if len(exc) > 0:
                raise exc[0]

    job_id = ray.runtime_context.get_runtime_context().job_id
    ray.get(verify_job_id.remote(job_id, False))
    ray.get(verify_job_id.remote(job_id, True))


# ANT-INTERNAL
def test_ray_job_log_level(shutdown_only):
    import glob

    config = ray.init(
        num_cpus=2, job_config=ray.job_config.JobConfig(logging_level="DEBUG"))

    @ray.remote
    def worker():
        logger = logging.getLogger("worker")
        logger.debug("test_worker_debug_log")

    ray.get(worker.remote())

    logs_dir = config["logs_dir"]
    worker_log = glob.glob(
        os.path.join(logs_dir, "python-worker-01000000-*.log"))
    with open(worker_log[0], "r") as f:
        assert "test_worker_debug_log" in f.read()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
