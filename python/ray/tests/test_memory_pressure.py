from math import ceil
import os
import sys
import time

import pytest

import ray
from ray import test_utils

import numpy as np
from ray._private.utils import get_system_memory
from ray._private.utils import get_used_memory

memory_usage_threshold = 0.65
task_oom_retries = 1
memory_monitor_refresh_ms = 100
expected_worker_eviction_message = (
    "Task was killed due to the node running low on memory")


@pytest.fixture
def ray_with_memory_monitor(shutdown_only):
    addr = ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold": memory_usage_threshold,
            "memory_monitor_refresh_ms": memory_monitor_refresh_ms,
            "metrics_report_interval_ms": 100,
            "task_failure_entry_ttl_ms": 2 * 60 * 1000,
            "task_oom_retries": task_oom_retries,
            "min_memory_free_bytes": -1,
            "runtime_resource_scheduling_enabled": False,
        },
    )
    yield addr


@pytest.fixture
def ray_with_memory_monitor_no_oom_retry(shutdown_only):
    addr = ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold": memory_usage_threshold,
            "memory_monitor_refresh_ms": 100,
            "metrics_report_interval_ms": 100,
            "task_failure_entry_ttl_ms": 2 * 60 * 1000,
            "task_oom_retries": 0,
            "min_memory_free_bytes": -1,
            "runtime_resource_scheduling_enabled": False,
        },
    )
    yield addr


@ray.remote
def allocate_memory(
        allocate_bytes: int,
        num_chunks: int = 10,
        allocate_interval_s: float = 0,
        post_allocate_sleep_s: float = 0,
):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    time.sleep(post_allocate_sleep_s)
    return end - start


@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes: int, sleep_time_s: int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(allocate_bytes / 8)
        self.leaks.append(new_list)

        time.sleep(sleep_time_s / 1000)

    def get_worker_id(self):
        return ray._private.worker.global_worker.core_worker.get_worker_id(
        ).hex()

    def get_actor_id(self):
        return ray._private.worker.global_worker.core_worker.get_actor_id(
        ).hex()


def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
    used = get_used_memory()
    total = get_system_memory()
    bytes_needed = int(total * pct) - used
    assert bytes_needed > 0, "node has less memory than what is requested"
    return bytes_needed


def has_metric_tagged_with_value(addr, tag, value) -> bool:
    # ANT-INTERNAL
    return True


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_actor(ray_with_memory_monitor):
    leaker = Leaker.options(max_restarts=0, max_task_retries=0).remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold - 0.1)
    ray.get(
        leaker.allocate.remote(bytes_to_alloc, memory_monitor_refresh_ms * 3))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold + 0.1)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(
            leaker.allocate.remote(bytes_to_alloc,
                                   memory_monitor_refresh_ms * 3))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_restartable_actor_killed_by_memory_monitor_with_actor_error(
        ray_with_memory_monitor, ):
    leaker = Leaker.options(max_restarts=1, max_task_retries=1).remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold + 0.1)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(
            leaker.allocate.remote(bytes_to_alloc,
                                   memory_monitor_refresh_ms * 3))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_non_retryable_task_killed_by_memory_monitor_with_oom_error(
        ray_with_memory_monitor, ):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(allocate_memory.options(max_retries=0).remote(bytes_to_alloc))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_retryable_task_killed_by_memory_monitor_with_oom_error(
        ray_with_memory_monitor, ):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(allocate_memory.options(max_retries=1).remote(bytes_to_alloc))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_newest_worker(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold - 0.1)

    actor_ref = Leaker.options(name="actor").remote()
    ray.get(actor_ref.allocate.remote(bytes_to_alloc))

    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(
            allocate_memory.options(max_retries=0).remote(
                allocate_bytes=bytes_to_alloc))

    actors = ray.state.actors()
    assert len(actors) == 1
    assert actor_ref._actor_id.hex() in actors
    assert actors[actor_ref._actor_id.hex()][
        "State"] == ray.gcs_utils.ActorTableData.ALIVE


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_task_if_actor_submitted_task_first(
        ray_with_memory_monitor, ):
    actor_ref = Leaker.options(name="leaker1").remote()
    ray.get(actor_ref.allocate.remote(10))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold - 0.1)
    task_ref = allocate_memory.options(max_retries=0).remote(
        allocate_bytes=bytes_to_alloc,
        allocate_interval_s=0,
        post_allocate_sleep_s=1000)

    ray.get(actor_ref.allocate.remote(bytes_to_alloc))
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(task_ref)

    actors = ray.state.actors()
    assert len(actors) == 1
    assert actor_ref._actor_id.hex() in actors
    assert actors[actor_ref._actor_id.hex()][
        "State"] == ray.gcs_utils.ActorTableData.ALIVE


@pytest.mark.skip(reason="Actor oom death cause is not propagated.")
@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
async def test_actor_oom_logs_error(ray_with_memory_monitor):
    first_actor = Leaker.options(
        name="first_random_actor", max_restarts=0).remote()
    ray.get(first_actor.get_worker_id.remote())

    oom_actor = Leaker.options(
        name="the_real_oom_actor", max_restarts=0).remote()
    worker_id = ray.get(oom_actor.get_worker_id.remote())
    actor_id = ray.get(oom_actor.get_actor_id.remote())

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(
            oom_actor.allocate.remote(bytes_to_alloc,
                                      memory_monitor_refresh_ms * 3))

    state_api_client = test_utils.get_local_state_client()
    result = await state_api_client.get_all_worker_info(timeout=5, limit=10)
    verified = False
    for worker in result.worker_table_data:
        if worker.worker_address.worker_id.hex() == worker_id:
            assert expected_worker_eviction_message in worker.exit_detail
            verified = True
    assert verified

    result = await state_api_client.get_all_actor_info(timeout=5, limit=10)
    verified = False
    for actor in result.actor_table_data:
        if actor.actor_id.hex() == actor_id:
            assert actor.death_cause
            assert actor.death_cause.actor_died_error_context
            assert (expected_worker_eviction_message in
                    actor.death_cause.actor_died_error_context.error_message)
            verified = True
    assert verified

    # TODO(clarng): verify log info once state api can dump log info


@pytest.mark.skip(reason="Actor oom death cause is not propagated.")
@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
async def test_task_oom_logs_error(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(
            allocate_memory.options(max_retries=0).remote(
                allocate_bytes=bytes_to_alloc,
                allocate_interval_s=0,
                post_allocate_sleep_s=1000,
            ))

    state_api_client = test_utils.get_local_state_client()
    result = await state_api_client.get_all_worker_info(timeout=5, limit=10)
    verified = False
    for worker in result.worker_table_data:
        if worker.exit_detail:
            assert expected_worker_eviction_message in worker.exit_detail
        verified = True
    assert verified

    # TODO(clarng): verify task info once state_api_client.get_task_info
    # returns the crashed task.
    # TODO(clarng): verify log info once state api can dump log info


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_task_oom_no_oom_retry_fails_immediately(
        ray_with_memory_monitor_no_oom_retry, ):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)

    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(
            allocate_memory.options(max_retries=1).remote(
                allocate_bytes=bytes_to_alloc, post_allocate_sleep_s=100))


@pytest.mark.skip(reason="oom only retry is not supported.")
@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_task_oom_only_uses_oom_retry(ray_with_memory_monitor, ):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)

    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(
            allocate_memory.options(max_retries=-1).remote(
                allocate_bytes=bytes_to_alloc, post_allocate_sleep_s=100))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_newer_task_not_retriable_kill_older_retriable_task_first(
        ray_with_memory_monitor, ):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold - 0.1)

    retriable_task_ref = allocate_memory.options(max_retries=1).remote(
        allocate_bytes=bytes_to_alloc, post_allocate_sleep_s=5)

    actor_ref = Leaker.options(name="actor", max_restarts=0).remote()
    non_retriable_actor_ref = actor_ref.allocate.remote(bytes_to_alloc)

    ray.get(non_retriable_actor_ref)
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(retriable_task_ref)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_put_object_task_usage_slightly_below_limit_does_not_crash():
    ray.init(
        num_cpus=1,
        object_store_memory=2 << 30,
        _system_config={
            "memory_monitor_refresh_ms": 50,
            "memory_usage_threshold": 0.98,
        })

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.9)
    print(bytes_to_alloc)
    ray.get(
        allocate_memory.options(max_retries=0).remote(
            allocate_bytes=bytes_to_alloc, ),
        timeout=90,
    )

    entries = int((1 << 30) / 8)
    obj_ref = ray.put(np.random.rand(entries))
    ray.get(obj_ref)

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.9)
    print(bytes_to_alloc)
    ray.get(
        allocate_memory.options(max_retries=0).remote(
            allocate_bytes=bytes_to_alloc, ),
        timeout=90,
    )
    ray.shutdown()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_legacy_memory_monitor_disabled_by_oom_killer():
    os.environ["RAY_MEMORY_MONITOR_ERROR_THRESHOLD"] = "0.5"
    ray.init(
        _system_config={
            "memory_monitor_refresh_ms": 50,
            "memory_usage_threshold": 0.9,
            "min_memory_free_bytes": -1,
        })
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.7)
    leaker = Leaker.options(max_restarts=0, max_task_retries=0).remote()
    ray.get(leaker.allocate.remote(bytes_to_alloc))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.8)
    ray.get(
        leaker.allocate.remote(allocate_bytes=bytes_to_alloc, sleep_time_s=10))
    ray.shutdown()


# ANT-INTERNAL below


@pytest.fixture
def ray_with_memory_monitor_and_runtime_scheduling(shutdown_only):
    addr = ray.init(
        num_cpus=4,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold": memory_usage_threshold,
            "memory_monitor_refresh_ms": memory_monitor_refresh_ms,
            "metrics_report_interval_ms": 100,
            "task_failure_entry_ttl_ms": 2 * 60 * 1000,
            "task_oom_retries": task_oom_retries,
            "min_memory_free_bytes": -1,
            "runtime_resource_scheduling_enabled": True,
        },
        job_config=ray.job_config.JobConfig(total_memory_mb=100000),
    )
    yield addr


@pytest.fixture
def enable_runtime_resource_scheduling():
    os.environ["RAY_runtime_resource_scheduling_enabled"] = "true"
    os.environ["RAY_runtime_resources_calculation_interval_s"] = "1"
    os.environ["REPORTER_UPDATE_INTERVAL_MS"] = "1000"
    os.environ["RAY_num_candidate_nodes_for_scheduling"] = "1"
    os.environ["RAY_memory_monitor_refresh_ms"] = "100"
    yield
    del os.environ["RAY_runtime_resource_scheduling_enabled"]
    del os.environ["RAY_runtime_resources_calculation_interval_s"]
    del os.environ["REPORTER_UPDATE_INTERVAL_MS"]
    del os.environ["RAY_num_candidate_nodes_for_scheduling"]
    del os.environ["RAY_memory_monitor_refresh_ms"]


def test_memory_pressure_kill_worker_with_most_violation(
        enable_runtime_resource_scheduling,
        ray_with_memory_monitor_and_runtime_scheduling):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold - 0.1)
    # Create the first actor with smaller memory requirement.
    actor_1 = Leaker.options(memory=int(bytes_to_alloc / 2)).remote()
    # The first actor's runtime resource is 2 times larger than the
    #  requirement (wait for `REPORTER_UPDATE_INTERVAL_MS` to make
    # sure the runtime stats have been reported to local raylet).
    ray.get(actor_1.allocate.remote(bytes_to_alloc, sleep_time_s=1000))

    # Create the second actor without runtime resource violation.
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold + 0.1)
    actor_2 = Leaker.options(memory=bytes_to_alloc).remote()
    ray.get(
        actor_2.allocate.remote(bytes_to_alloc, memory_monitor_refresh_ms * 3))

    # When oom happens, the first actor will be killed because it
    # has resource violation.
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(actor_1.get_worker_id.remote())

    actors = ray.state.actors()
    assert len(actors) == 2
    assert actors[actor_1._actor_id.hex()][
        "State"] == ray.gcs_utils.ActorTableData.DEAD
    assert actors[actor_2._actor_id.hex()][
        "State"] == ray.gcs_utils.ActorTableData.ALIVE


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
