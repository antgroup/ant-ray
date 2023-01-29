# coding: utf-8
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import random
import signal
import sys
import threading
import time

import numpy as np
import pytest

import ray.cluster_utils
import ray.test_utils

from ray.test_utils import client_test_enabled
from ray.test_utils import RayTestTimeoutException
from ray.test_utils import SignalActor
from ray.test_utils import wait_for_pid_to_exit
from ray.ray_constants import (
    gcs_task_scheduling_enabled, )
from ray import state

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


# issue https://github.com/ray-project/ray/issues/7105
@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_internal_free(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Sampler:
        def sample(self):
            return [1, 2, 3, 4, 5]

        def sample_big(self):
            return np.zeros(1024 * 1024)

    sampler = Sampler.remote()

    # Free deletes from in-memory store.
    obj_ref = sampler.sample.remote()
    ray.get(obj_ref)
    ray.internal.free(obj_ref)
    with pytest.raises(Exception):
        ray.get(obj_ref)

    # Free deletes big objects from plasma store.
    big_id = sampler.sample_big.remote()
    ray.get(big_id)
    ray.internal.free(big_id)
    time.sleep(1)  # wait for delete RPC to propagate
    with pytest.raises(Exception):
        ray.get(big_id)


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    @ray.remote
    def g(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.wait([input_list[0]])

    @ray.remote
    def h(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.get(input_list[0])

    # Make sure that multiple wait requests involving the same object ref
    # all return.
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ref all
    # return.
    x = f.remote(1)
    ray.get([h.remote([x]), h.remote([x])])


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_caching_functions_to_run(shutdown_only):
    # Test that we export functions to run on all workers before the driver
    # is connected.
    def f(worker_info):
        sys.path.append(1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def f(worker_info):
        sys.path.append(2)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def g(worker_info):
        sys.path.append(3)

    ray.worker.global_worker.run_function_on_all_workers(g)

    def f(worker_info):
        sys.path.append(4)

    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.init(num_cpus=1)

    @ray.remote
    def get_state():
        time.sleep(1)
        return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

    res1 = get_state.remote()
    res2 = get_state.remote()
    assert ray.get(res1) == (1, 2, 3, 4)
    assert ray.get(res2) == (1, 2, 3, 4)

    # Clean up the path on the workers.
    def f(worker_info):
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()

    ray.worker.global_worker.run_function_on_all_workers(f)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_running_function_on_all_workers(ray_start_regular):
    def f(worker_info):
        sys.path.append("fake_directory")

    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path1():
        return sys.path

    assert "fake_directory" == ray.get(get_path1.remote())[-1]

    # the function should only run on the current driver once.
    assert sys.path[-1] == "fake_directory"
    if len(sys.path) > 1:
        assert sys.path[-2] != "fake_directory"

    def f(worker_info):
        sys.path.pop(-1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    # Create a second remote function to guarantee that when we call
    # get_path2.remote(), the second function to run will have been run on
    # the worker.
    @ray.remote
    def get_path2():
        return sys.path

    assert "fake_directory" not in ray.get(get_path2.remote())


@pytest.mark.skip("See profiling.py.")
def test_profiling_api(ray_start_2_cpus):
    @ray.remote
    def f():
        with ray.profiling.profile(
                "custom_event", extra_data={"name": "custom name"}):
            pass

    ray.put(1)
    object_ref = f.remote()
    ray.wait([object_ref])
    ray.get(object_ref)

    # Wait until all of the profiling information appears in the profile
    # table.
    timeout_seconds = 20
    start_time = time.time()
    while True:
        profile_data = ray.timeline()
        event_types = {event["cat"] for event in profile_data}
        expected_types = [
            "task",
            "task:deserialize_arguments",
            "task:execute",
            "task:store_outputs",
            "wait_for_function",
            "ray.get",
            "ray.put",
            "ray.wait",
            "submit_task",
            "fetch_and_run_function",
            # TODO (Alex) :https://github.com/ray-project/ray/pull/9346
            # "register_remote_function",
            "custom_event",  # This is the custom one from ray.profile.
        ]

        if all(expected_type in event_types
               for expected_type in expected_types):
            break

        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for information in "
                "profile table. Missing events: {}.".format(
                    set(expected_types) - set(event_types)))

        # The profiling information only flushes once every second.
        time.sleep(1.1)


def test_wait_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"RemoteResource": 1})
    def f():
        return

    # Make sure we have enough workers on the remote nodes to execute some
    # tasks.
    tasks = [f.remote() for _ in range(10)]
    start = time.time()
    ray.get(tasks)
    end = time.time()

    # Submit some more tasks that can only be executed on the remote nodes.
    tasks = [f.remote() for _ in range(10)]
    # Sleep for a bit to let the tasks finish.
    time.sleep((end - start) * 2)
    _, unready = ray.wait(tasks, num_returns=len(tasks), timeout=0)
    # All remote tasks should have finished.
    assert len(unready) == 0


@pytest.mark.skip(reason="TODO(ekl)")
def test_object_transfer_dump(ray_start_cluster):
    cluster = ray_start_cluster

    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10**9)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return

    # These objects will live on different nodes.
    object_refs = [
        f._remote(args=[1], resources={str(i): 1}) for i in range(num_nodes)
    ]

    # Broadcast each object from each machine to each other machine.
    for object_ref in object_refs:
        ray.get([
            f._remote(args=[object_ref], resources={str(i): 1})
            for i in range(num_nodes)
        ])

    # The profiling information only flushes once every second.
    time.sleep(1.1)

    transfer_dump = ray.state.object_transfer_timeline()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes**2
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_receive"
    }) == num_nodes
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_send"
    }) == num_nodes


def test_identical_function_names(ray_start_regular):
    # Define a bunch of remote functions and make sure that we don't
    # accidentally call an older version.

    num_calls = 200

    @ray.remote
    def f():
        return 1

    results1 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 2

    results2 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 3

    results3 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 4

    results4 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 5

    results5 = [f.remote() for _ in range(num_calls)]

    assert ray.get(results1) == num_calls * [1]
    assert ray.get(results2) == num_calls * [2]
    assert ray.get(results3) == num_calls * [3]
    assert ray.get(results4) == num_calls * [4]
    assert ray.get(results5) == num_calls * [5]

    @ray.remote
    def g():
        return 1

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 2

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 3

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 4

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 5

    result_values = ray.get([g.remote() for _ in range(num_calls)])
    assert result_values == num_calls * [5]


def test_illegal_api_calls(ray_start_regular):

    # Verify that we cannot call put on an ObjectRef.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.put(x)
    # Verify that we cannot call get on a regular value.
    with pytest.raises(Exception):
        ray.get(3)


@pytest.mark.skipif(
    client_test_enabled(), reason="grpc interaction with releasing resources")
def test_multithreading(ray_start_2_cpus):
    # This test requires at least 2 CPUs to finish since the worker does not
    # release resources when joining the threads.

    def run_test_in_multi_threads(test_case, num_threads=10, num_repeats=25):
        """A helper function that runs test cases in multiple threads."""

        def wrapper():
            for _ in range(num_repeats):
                test_case()
                time.sleep(random.randint(0, 10) / 1000.0)
            return "ok"

        executor = ThreadPoolExecutor(max_workers=num_threads)
        futures = [executor.submit(wrapper) for _ in range(num_threads)]
        for future in futures:
            assert future.result() == "ok"

    @ray.remote
    def echo(value, delay_ms=0):
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        return value

    def test_api_in_multi_threads():
        """Test using Ray api in multiple threads."""

        @ray.remote
        class Echo:
            def echo(self, value):
                return value

        # Test calling remote functions in multiple threads.
        def test_remote_call():
            value = random.randint(0, 1000000)
            result = ray.get(echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_remote_call)

        # Test multiple threads calling one actor.
        actor = Echo.remote()

        def test_call_actor():
            value = random.randint(0, 1000000)
            result = ray.get(actor.echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_call_actor)

        # Test put and get.
        def test_put_and_get():
            value = random.randint(0, 1000000)
            result = ray.get(ray.put(value))
            assert value == result

        run_test_in_multi_threads(test_put_and_get)

        # Test multiple threads waiting for objects.
        num_wait_objects = 10
        objects = [
            echo.remote(i, delay_ms=10) for i in range(num_wait_objects)
        ]

        def test_wait():
            ready, _ = ray.wait(
                objects,
                num_returns=len(objects),
                timeout=1000.0,
            )
            assert len(ready) == num_wait_objects
            assert ray.get(ready) == list(range(num_wait_objects))

        run_test_in_multi_threads(test_wait, num_repeats=1)

    # Run tests in a driver.
    test_api_in_multi_threads()

    # Run tests in a worker.
    @ray.remote
    def run_tests_in_worker():
        test_api_in_multi_threads()
        return "ok"

    assert ray.get(run_tests_in_worker.remote()) == "ok"

    # Test actor that runs background threads.
    @ray.remote
    class MultithreadedActor:
        def __init__(self):
            self.lock = threading.Lock()
            self.thread_results = []

        def background_thread(self, wait_objects):
            try:
                # Test wait
                ready, _ = ray.wait(
                    wait_objects,
                    num_returns=len(wait_objects),
                    timeout=1000.0,
                )
                assert len(ready) == len(wait_objects)
                for _ in range(20):
                    num = 10
                    # Test remote call
                    results = [echo.remote(i) for i in range(num)]
                    assert ray.get(results) == list(range(num))
                    # Test put and get
                    objects = [ray.put(i) for i in range(num)]
                    assert ray.get(objects) == list(range(num))
                    time.sleep(random.randint(0, 10) / 1000.0)
            except Exception as e:
                with self.lock:
                    self.thread_results.append(e)
            else:
                with self.lock:
                    self.thread_results.append("ok")

        def spawn(self):
            wait_objects = [echo.remote(i, delay_ms=10) for i in range(10)]
            self.threads = [
                threading.Thread(
                    target=self.background_thread, args=(wait_objects, ))
                for _ in range(20)
            ]
            [thread.start() for thread in self.threads]

        def join(self):
            [thread.join() for thread in self.threads]
            assert self.thread_results == ["ok"] * len(self.threads)
            return "ok"

    actor = MultithreadedActor.remote()
    actor.spawn.remote()
    ray.get(actor.join.remote()) == "ok"


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_wait_makes_object_local(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    @ray.remote
    class Foo:
        def method(self):
            return np.zeros(1024 * 1024)

    a = Foo.remote()

    # Test get makes the object local.
    x_id = a.method.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ray.get(x_id)
    assert ray.worker.global_worker.core_worker.object_exists(x_id)

    # Test wait makes the object local.
    x_id = a.method.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ok, _ = ray.wait([x_id])
    assert len(ok) == 1
    assert ray.worker.global_worker.core_worker.object_exists(x_id)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_future_resolution_skip_plasma(ray_start_cluster):
    cluster = ray_start_cluster
    # Disable worker caching so worker leases are not reused; set object
    # inlining size threshold and enable storing of small objects in in-memory
    # object store so the borrowed ref is inlined.
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 100 * 1024,
            "put_small_object_in_memory_store": True,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin_head": 1})
    def f(x):
        return x + 1

    @ray.remote(resources={"pin_worker": 1})
    def g(x):
        borrowed_ref = x[0]
        f_ref = f.remote(borrowed_ref)
        # borrowed_ref should be inlined on future resolution and shouldn't be
        # in Plasma.
        assert ray.worker.global_worker.core_worker.object_exists(
            borrowed_ref, memory_store_only=True)
        return ray.get(f_ref) * 2

    one = ray.put(1)
    g_ref = g.remote([one])
    assert ray.get(g_ref) == 4


# === ANT-INTERNAL below ===


@pytest.mark.parametrize("args", [[5, 20], [5, 3]])
@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_actor_distribution_balance(ray_start_cluster, args):
    cluster = ray_start_cluster

    node_count = args[0]
    actor_count = args[1]

    for i in range(node_count):
        system_config = {}
        if i == 0:
            system_config = {"num_candidate_nodes_for_scheduling": 1}
        cluster.add_node(memory=1024**3, _system_config=system_config)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=100 * 1024**2)
    class Foo:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    actor_distribution = {}
    actor_list = [Foo.remote() for _ in range(actor_count)]
    for actor in actor_list:
        node_id = ray.get(actor.method.remote())
        if node_id not in actor_distribution.keys():
            actor_distribution[node_id] = []
        actor_distribution[node_id].append(actor)

    if node_count >= actor_count:
        assert len(actor_distribution) == actor_count
        for node_id, actors in actor_distribution.items():
            assert len(actors) == 1
    else:
        assert len(actor_distribution) == node_count
        for node_id, actors in actor_distribution.items():
            assert len(actors) <= int(actor_count / node_count)


def test_schedule_actor_and_normal_task(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(memory=1024**3)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=600 * 1024**2)
    class Foo:
        def method(self):
            return 2

    @ray.remote(memory=600 * 1024**2)
    def fun(singal1, signal_actor2):
        signal_actor2.send.remote()
        ray.get(singal1.wait.remote())
        return 1

    singal1 = SignalActor.remote()
    signal2 = SignalActor.remote()

    o1 = fun.remote(singal1, signal2)
    # Make sure the normal task is executing.
    ray.get(signal2.wait.remote())

    # The normal task is blocked now.
    # Try to create actor and make sure this actor is not created for the time
    # being.
    foo = Foo.remote()
    o2 = foo.method.remote()
    ready_list, remaining_list = ray.wait([o2], timeout=2)
    assert len(ready_list) == 0 and len(remaining_list) == 1

    # Send a signal to unblock the normal task execution.
    ray.get(singal1.send.remote())

    # Check the result of normal task.
    assert ray.get(o1) == 1

    # Make sure the actor is created.
    assert ray.get(o2) == 2


def test_schedule_many_actors_and_normal_tasks(ray_start_cluster):
    cluster = ray_start_cluster

    node_count = 10
    actor_count = 50
    each_actor_task_count = 50
    normal_task_count = 1000
    node_memory_mb = 2 * 1024  # 2G
    node_memory_bytes = node_memory_mb * 1024 * 1024

    for _ in range(node_count):
        cluster.add_node(memory=node_memory_bytes)

    ray.init(
        address=cluster.address,
        job_config=ray.job_config.JobConfig(total_memory_mb=0.8 * node_count *
                                            node_memory_mb))
    cluster.wait_for_nodes()

    @ray.remote(memory=100 * 1024**2)
    class Foo:
        def method(self):
            return 2

    @ray.remote(memory=100 * 1024**2)
    def fun():
        return 1

    normal_task_object_list = [fun.remote() for _ in range(normal_task_count)]
    actor_list = [Foo.remote() for _ in range(actor_count)]
    actor_object_list = [
        actor.method.remote() for _ in range(each_actor_task_count)
        for actor in actor_list
    ]
    for object in ray.get(actor_object_list):
        assert object == 2

    for object in ray.get(normal_task_object_list):
        assert object == 1


# This tests step-pack schedule policy.
@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_step_pack_schedule_policy(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        memory=800 * 1024**2,
        _system_config={
            "gcs_schedule_pack_step": 0.6,
            "num_candidate_nodes_for_scheduling": 1
        })
    # cluster.add_node(memory=800 * 1024**2)
    cluster.add_node(memory=800 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=200 * 1024**2)
    class SmallActor:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    sa1 = SmallActor.remote()
    o1 = sa1.method.remote()
    ready_list, remaining_list = ray.wait([o1], timeout=5)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    sa2 = SmallActor.remote()
    o2 = sa2.method.remote()
    ready_list, remaining_list = ray.wait([o2], timeout=5)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    @ray.remote(memory=700 * 1024**2)
    class LargeActor:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    la = LargeActor.remote()
    o3 = la.method.remote()
    ready_list, remaining_list = ray.wait([o3], timeout=5)
    assert len(ready_list) == 1 and len(remaining_list) == 0


# This tests whether RequestWorkerLeaseReply carries normal task resources
# when the request is rejected.
@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_worker_lease_reply_with_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        memory=2000 * 1024**2,
        _system_config={
            "gcs_resource_report_poll_period_ms": 1000000,
            "runtime_resource_scheduling_enabled": False,
            "num_candidate_nodes_for_scheduling": 1
        })
    node2 = cluster.add_node(memory=1000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=1500 * 1024**2)
    def fun(signal):
        signal.send.remote()
        time.sleep(30)
        return 0

    signal = SignalActor.remote()
    fun.remote(signal)
    # Make sure that the `fun` is running.
    ray.get(signal.wait.remote())

    @ray.remote(memory=800 * 1024**2)
    class Foo:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    foo1 = Foo.remote()
    o1 = foo1.method.remote()
    ready_list, remaining_list = ray.wait([o1], timeout=5)
    # If RequestWorkerLeaseReply carries normal task resources,
    # GCS will then schedule foo1 to node2. Otherwise,
    # GCS would keep trying to schedule foo1 to
    # node1 and getting rejected.
    assert len(ready_list) == 1 and len(remaining_list) == 0
    assert ray.get(o1) == node2.unique_id


@ray.remote
class SmallActor:
    def __init__(self):
        self._vec = []

    def mem(self, len):
        self._vec = [0] * int(len / 8)

    def node(self):
        return ray.worker.global_worker.node.unique_id

    def pid(self):
        return os.getpid()


@pytest.fixture
def enable_runtime_resource_scheduling():
    os.environ["RAY_runtime_resource_scheduling_enabled"] = "true"
    os.environ["RAY_runtime_resources_calculation_interval_s"] = "1"
    os.environ["REPORTER_UPDATE_INTERVAL_MS"] = "1000"
    os.environ["RAY_num_candidate_nodes_for_scheduling"] = "1"
    yield
    del os.environ["RAY_runtime_resource_scheduling_enabled"]
    del os.environ["RAY_runtime_resources_calculation_interval_s"]
    del os.environ["REPORTER_UPDATE_INTERVAL_MS"]
    del os.environ["RAY_num_candidate_nodes_for_scheduling"]


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_runtime_resource_leak(enable_runtime_resource_scheduling,
                               ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(memory=1000 * 1024**2)
    cluster.add_node(memory=1000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    original_node_resources = state.state.get_cluster_resources()

    oa = SmallActor.options(memory=500 * 1024**2).remote()
    oa_pid = oa.pid.remote()
    ready_list, remaining_list = ray.wait([oa_pid], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    ua = SmallActor.options(memory=500 * 1024**2).remote()
    ua_pid = ua.pid.remote()
    ready_list, remaining_list = ray.wait([ua_pid], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    # Consume 800MB (overloading) memory in oa.
    oa.mem.remote(800 * 1024**2)
    # Consume 100MB (underloading) memory in ua.
    ua.mem.remote(100 * 1024**2)

    # Let the agents report runtime resources.
    time.sleep(5)

    node_resources_at_runtime = state.state.get_cluster_resources()
    runtime_available_memory = []
    for _, resources in node_resources_at_runtime.items():
        runtime_available_memory.append(
            resources["resources_available"]["memory"])
    # These two nodes should have different runtime available memory.
    assert runtime_available_memory[0] != runtime_available_memory[1]

    ray.kill(oa)
    ray.kill(ua)
    wait_for_pid_to_exit(ray.get(oa_pid))
    wait_for_pid_to_exit(ray.get(ua_pid))
    node_resources_after_actor_kill = state.state.get_cluster_resources()
    assert dict(node_resources_after_actor_kill) == dict(
        original_node_resources)


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_runtime_resource_leak_with_pg(enable_runtime_resource_scheduling,
                                       ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(memory=1000 * 1024**2)
    cluster.add_node(memory=1000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    original_node_resources = state.state.get_cluster_resources()

    pg = ray.util.placement_group(
        name="test_pg",
        strategy="STRICT_SPREAD",
        bundles=[{
            "CPU": 0.01,
            "memory": 500 * 1024**2
        }, {
            "CPU": 0.01,
            "memory": 500 * 1024**2
        }])
    assert pg.wait(100)

    node_resources_with_pg = state.state.get_cluster_resources()

    oa = SmallActor.options(
        memory=400 * 1024**2,
        placement_group=pg,
        placement_group_bundle_index=0).remote()
    oa_pid = oa.pid.remote()
    ready_list, remaining_list = ray.wait([oa_pid], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    ua = SmallActor.options(
        memory=400 * 1024**2,
        placement_group=pg,
        placement_group_bundle_index=1).remote()
    ua_pid = ua.pid.remote()
    ready_list, remaining_list = ray.wait([ua_pid], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    # Consume 800MB (overloading) memory in oa.
    oa.mem.remote(800 * 1024**2)
    # Consume 100MB (underloading) memory in ua.
    ua.mem.remote(100 * 1024**2)

    # Let the agents report runtime resources.
    time.sleep(5)

    node_resources_at_runtime = state.state.get_cluster_resources()
    runtime_available_memory = []
    runtime_bundle_available_memory = []
    runtime_bundle_total_memory = []
    for _, resources in node_resources_at_runtime.items():
        resources_total = resources["resources_total"]
        resources_available = resources["resources_available"]
        runtime_available_memory.append(resources_available["memory"])
        for resource_label, val in resources_available.items():
            if resource_label.startswith(
                    "memory_group_0_") or resource_label.startswith(
                        "memory_group_1_"):
                runtime_bundle_available_memory.append(val)
                runtime_bundle_total_memory.append(
                    resources_total[resource_label])
    # These two nodes should have different runtime available memory.
    assert runtime_available_memory[0] != runtime_available_memory[1]
    # The PG bundle resources should be the same as the original configuration.
    assert runtime_bundle_available_memory[
        0] == runtime_bundle_available_memory[1]
    assert runtime_bundle_total_memory[0] == runtime_bundle_total_memory[1]

    ray.kill(oa)
    ray.kill(ua)
    wait_for_pid_to_exit(ray.get(oa_pid))
    wait_for_pid_to_exit(ray.get(ua_pid))
    node_resources_after_actor_kill = state.state.get_cluster_resources()
    assert dict(node_resources_after_actor_kill) == dict(
        node_resources_with_pg)

    ray.util.remove_placement_group(pg)
    while True:
        table = ray.util.placement_group_table(pg)
        if "state" not in table or table["state"] == "REMOVED":
            break
        time.sleep(1)
    node_resources_after_pg_removal = state.state.get_cluster_resources()
    assert dict(node_resources_after_pg_removal) == dict(
        original_node_resources)


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_runtime_scheduling(enable_runtime_resource_scheduling,
                            ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(memory=1000 * 1024**2)
    cluster.add_node(memory=1000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    sa = SmallActor.options(memory=1000 * 1024**2).remote()
    sa_node = sa.node.remote()
    ready_list, remaining_list = ray.wait([sa_node], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    oa = SmallActor.options(memory=100 * 1024**2).remote()
    oa_node = oa.node.remote()
    ready_list, remaining_list = ray.wait([oa_node], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    assert ray.get(sa_node) != ray.get(oa_node)

    # Consume 800MB memory (overloading) in oa.
    oa.mem.remote(800 * 1024**2)

    time.sleep(5)
    ta = SmallActor.options(memory=300 * 1024**2).remote()
    ta_node = ta.node.remote()
    ready_list, remaining_list = ray.wait([ta_node], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    # ta would not be co-located with oa because of
    # runtime overloading. Meanwhile, ta is allowed
    # being co-located with sa with runtime overcommit enabled.
    assert ray.get(ta_node) == ray.get(sa_node)


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_runtime_scheduling_with_pg(enable_runtime_resource_scheduling,
                                    ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(memory=1000 * 1024**2)
    cluster.add_node(memory=1000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    placement_group = ray.util.placement_group(
        name="pg_name",
        strategy="STRICT_SPREAD",
        bundles=[{
            "CPU": 0.01,
            "memory": 1000 * 1024**2
        }, {
            "CPU": 0.01,
            "memory": 100 * 1024**2
        }])
    assert placement_group.wait(1000)

    sa = SmallActor.options(
        memory=1000 * 1024**2,
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    sa_node = sa.node.remote()
    ready_list, remaining_list = ray.wait([sa_node], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    oa = SmallActor.options(
        memory=100 * 1024**2,
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    oa_node = oa.node.remote()
    ready_list, remaining_list = ray.wait([oa_node], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    assert ray.get(sa_node) != ray.get(oa_node)

    # Consume 800MB memory (overloading) in oa.
    oa.mem.remote(800 * 1024**2)

    time.sleep(5)
    ta = SmallActor.options(memory=300 * 1024**2, ).remote()
    ta_node = ta.node.remote()
    ready_list, remaining_list = ray.wait([ta_node], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    # ta would not be co-located with oa because of
    # runtime overloading. Meanwhile, ta is allowed
    # being co-located with sa with runtime overcommit enabled.
    assert ray.get(ta_node) == ray.get(sa_node)


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_node_overcommit_ratio(enable_runtime_resource_scheduling,
                               ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        memory=1000 * 1024**2, _system_config={"node_overcommit_ratio": 1.5})
    cluster.add_node(memory=1000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    placement_group = ray.util.placement_group(
        name="pg_name",
        strategy="SPREAD",
        bundles=[{
            "CPU": 0.01,
            "memory": 1000 * 1024**2
        }, {
            "CPU": 0.01,
            "memory": 800 * 1024**2
        }])
    assert placement_group.wait(1000)

    actor1 = SmallActor.options(
        memory=1000 * 1024**2,
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    pos1 = actor1.node.remote()
    ready_list, remaining_list = ray.wait([pos1], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    actor2 = SmallActor.options(
        memory=800 * 1024**2,
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    pos2 = actor2.node.remote()
    ready_list, remaining_list = ray.wait([pos2], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    assert ray.get(pos1) != ray.get(pos2)

    # Consume tiny memory in actor1.
    actor1.mem.remote(10 * 1024**2)
    # Consume double memory in actor2.
    actor2.mem.remote(20 * 1024**2)

    time.sleep(5)

    node_resources_at_runtime = state.state.get_cluster_resources()
    runtime_available_memory = []
    for _, resources in node_resources_at_runtime.items():
        resources_available = resources["resources_available"]
        runtime_available_memory.append(resources_available["memory"])
    # These two nodes should have different runtime available memory.
    assert runtime_available_memory[0] != runtime_available_memory[1]

    actor3 = SmallActor.options(memory=600 * 1024**2, ).remote()
    pos3 = actor3.node.remote()
    ready_list, remaining_list = ray.wait([pos3], timeout=30)
    assert len(ready_list) == 1 and len(remaining_list) == 0

    # Based on runtime resources, actor3 can be placed at both nodes. But
    # colocating with actor1 would exceed the node overcommit ratio
    # ((1000 + 600)/1000 > 1.5), so it is placed at the other node (even though
    # actor2 consumes more runtime resources).
    assert ray.get(pos3) == ray.get(pos2)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_running_function_on_all_workers_job_isolation():
    import contextlib
    from ray.cluster_utils import Cluster
    cluster = Cluster()
    head_node = cluster.add_node(node_ip_address="127.0.0.1", num_cpus=6)

    @contextlib.contextmanager
    def _job_context():
        address_info = ray.init(head_node.address)

        yield address_info
        # The code after the yield will run as teardown code.
        ray.shutdown()

    for i in range(2):
        with _job_context():

            def f(worker_info):
                sys.path.append(f"fake_directory{i}")

            ray.worker.global_worker.run_function_on_all_workers(f)

            @ray.remote
            def get_path1():
                # Make the remote function generates unique function ids
                # in different jobs.
                print(i)
                return sys.path

            get_path = ray.get(get_path1.remote())
            assert f"fake_directory{i}" == get_path[-1], get_path
            assert sum("fake_directory" in p for p in get_path) == 1, get_path

    test_paths = [p for p in sys.path if p.startswith("fake_directory")]
    # Only fake_directory0 and fake_directory1 in sys.path
    assert len(test_paths) == 2
    assert len(set(test_paths)) == 2


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_pending_actor_sched(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(memory=2000 * 1024**2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=1200 * 1024**2)
    class Blocker:
        def fun(self):
            return 0

    blocker = Blocker.remote()
    # Make sure that the `blocker` is running.
    ray.get(blocker.fun.remote())

    @ray.remote(num_cpus=0.5, memory=1200 * 1024**2)
    class Foo:
        def method(self, index):
            return index

    foo1 = Foo.remote()
    o1 = foo1.method.remote(1)
    ready_list, remaining_list = ray.wait([o1], timeout=2)
    assert len(ready_list) == 0 and len(remaining_list) == 1

    Foo2 = Foo.options(num_cpus=0.1, memory=1600 * 1024**2)
    foo2 = Foo2.remote()
    o2 = foo2.method.remote(2)
    ready_list, remaining_list = ray.wait([o2], timeout=2)
    assert len(ready_list) == 0 and len(remaining_list) == 1

    Foo3 = Foo.options(num_cpus=0.2, memory=1600 * 1024**2)
    foo3 = Foo3.remote()
    o3 = foo3.method.remote(3)
    ready_list, remaining_list = ray.wait([o3], timeout=2)
    assert len(ready_list) == 0 and len(remaining_list) == 1

    # Release the blocker.
    ray.kill(blocker)

    # Check the scheduling results of the pending actors.
    # Because pending actors are ordered in decreasing order of memory in GCS,
    # foo3 is firstly scheduled, then foo2, and finally foo1.
    ready_list, remaining_list = ray.wait([o1, o2, o3], timeout=5)
    assert len(ready_list) == 1 and len(remaining_list) == 2 and ray.get(
        ready_list[0]) == 3
    ray.kill(foo3)

    ready_list, remaining_list = ray.wait([o1, o2], timeout=5)
    assert len(ready_list) == 1 and len(remaining_list) == 1 and ray.get(
        ready_list[0]) == 2
    ray.kill(foo2)

    ready_list, remaining_list = ray.wait([o1], timeout=5)
    assert len(ready_list) == 1 and len(remaining_list) == 0 and ray.get(
        ready_list[0]) == 1
    ray.kill(foo1)


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "_system_config": {
            "num_candidate_nodes_for_scheduling": 1
        },
        "memory": 1 * 1024**2
    }],
    indirect=True)
def test_pending_actor_sched_with_pg_dependency(ray_start_cluster_head):
    cluster = ray_start_cluster_head

    @ray.remote(num_cpus=2, memory=50 * 1024)
    class Actor:
        def __init__(self, index):
            self.index = index

        def value(self):
            return self.index

    # Try to create a PG with CPU, memory and GPU requirements.
    pg_1 = ray.util.placement_group(
        name="pg_1",
        strategy="PACK",
        bundles=[{
            "CPU": 2,
            "memory": 50 * 1024**2,
            "GPU": 1
        }])
    actor_1 = Actor.options(
        placement_group=pg_1, placement_group_bundle_index=0).remote(1)

    # Try to create a PG with only CPU and memory requirements.
    pg_2 = ray.util.placement_group(
        name="pg_2",
        strategy="PACK",
        bundles=[{
            "CPU": 2,
            "memory": 50 * 1024**2
        }])
    actor_2 = Actor.options(
        placement_group=pg_2, placement_group_bundle_index=0).remote(2)

    # The two PGs and two actors are all pending. Now add a node that
    # will make pg_2 and actor_2 schedulable.
    cluster.add_node(num_cpus=2, memory=50 * 1024**2)
    cluster.wait_for_nodes()

    # The two actors require the same CPU and memory, but both have
    # PG dependency. So they will not have the same scheduling class,
    # neither shall actor_1 block actor_2.
    ready_list, _ = ray.wait(
        [actor_1.value.remote(),
         actor_2.value.remote()], timeout=3)
    # actor_2 has been sucessfully scheduled.
    assert len(ready_list) == 1 and ray.get(ready_list[0]) == 2

    # Now pg_1 should be scheduled with a new node (with GPU resource) added.
    cluster.add_node(num_cpus=2, num_gpus=1, memory=50 * 1024**2)
    cluster.wait_for_nodes()
    ready_list, _ = ray.wait([actor_1.value.remote()], timeout=3)
    # actor_1 has been sucessfully scheduled.
    assert len(ready_list) == 1 and ray.get(ready_list[0]) == 1


# === ANT-INTERNAL above ===

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
