"""
This file defines the common pytest fixtures used in current directory.
"""
import os
from contextlib import contextmanager
import pytest
import subprocess
import json

import ray
from ray.cluster_utils import Cluster
from ray.test_utils import init_error_pubsub

from ray.ray_constants import (
    gcs_task_scheduling_enabled, )


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def get_default_fixure_system_config():
    system_config = {
        "object_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
        "object_store_full_delay_ms": 100,
    }
    return system_config


def get_default_fixture_ray_kwargs():
    system_config = get_default_fixure_system_config()
    ray_kwargs = {
        "num_cpus": 1,
        "object_store_memory": 150 * 1024 * 1024,
        "dashboard_port": None,
        "namespace": "",
        "_system_config": system_config,
    }
    return ray_kwargs


@contextmanager
def _ray_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs)

    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_with_dashboard(request):
    param = getattr(request, "param", {})

    with _ray_start(
            num_cpus=4, resources={"MEM": 8}, include_dashboard=True,
            **param) as address_info:
        yield address_info


# The following fixture will start ray with 0 cpu.
@pytest.fixture
def ray_start_no_cpu(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=0, **param) as res:
        yield res


# The following fixture will start ray with 1 cpu.
@pytest.fixture
def ray_start_regular(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture(scope="module")
def ray_start_regular_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture(
    scope="module", params=[{
        "local_mode": True
    }, {
        "local_mode": False
    }])
def ray_start_shared_local_modes(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture
def ray_start_2_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_10_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=10, **param) as res:
        yield res


@contextmanager
def _ray_start_cluster(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    num_nodes = 0
    do_init = False
    job_config = None
    include_dashboard = False
    # num_nodes & do_init are not arguments for ray.init, so delete them.
    if "num_nodes" in kwargs:
        num_nodes = kwargs["num_nodes"]
        del kwargs["num_nodes"]
    if "do_init" in kwargs:
        do_init = kwargs["do_init"]
        del kwargs["do_init"]
    elif num_nodes > 0:
        do_init = True
    if "_job_config" in kwargs:
        job_config = kwargs["_job_config"]
        del kwargs["_job_config"]
    if "include_dashboard" in kwargs:
        include_dashboard = kwargs["include_dashboard"]
        del kwargs["include_dashboard"]
    init_kwargs.update(kwargs)
    namespace = init_kwargs.pop("namespace")
    cluster = Cluster()
    remote_nodes = []
    for i in range(num_nodes):
        if i > 0 and "_system_config" in init_kwargs:
            del init_kwargs["_system_config"]
        remote_nodes.append(cluster.add_node(**init_kwargs))
        # We assume driver will connect to the head (first node),
        # so ray init will be invoked if do_init is true
        if len(remote_nodes) == 1 and do_init:
            ray.init(
                address=cluster.address,
                namespace=namespace,
                job_config=job_config,
                include_dashboard=include_dashboard
            ) if job_config else ray.init(
                address=cluster.address, namespace=namespace)
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


# This fixture will start a cluster with empty nodes.
@pytest.fixture
def ray_start_cluster(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(**param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_init(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_head(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=1, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_2_nodes(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_head_with_dashboard(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(
            do_init=True, num_nodes=1, include_dashboard=True, **param) as res:
        yield res


@pytest.fixture
def ray_start_object_store_memory(request):
    # Start the Ray processes.
    store_size = request.param
    system_config = get_default_fixure_system_config()
    init_kwargs = {
        "num_cpus": 1,
        "_system_config": system_config,
        "object_store_memory": store_size,
    }
    ray.init(**init_kwargs)
    yield store_size
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def call_ray_start(request):
    parameter = getattr(
        request, "param", "ray start --head --num-cpus=1 --min-worker-port=0 "
        "--max-worker-port=0 --port 0")
    command_args = parameter.split(" ")
    out = ray._private.utils.decode(
        subprocess.check_output(command_args, stderr=subprocess.STDOUT))
    # Get the redis address from the output.
    redis_substring_prefix = "--address='"
    address_location = (
        out.find(redis_substring_prefix) + len(redis_substring_prefix))
    address = out[address_location:]
    address = address.split("'")[0]

    yield address

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"])


@pytest.fixture
def call_ray_stop_only():
    yield
    subprocess.check_call(["ray", "stop"])


@pytest.fixture
def enable_pickle_debug():
    os.environ["RAY_PICKLE_VERBOSE_DEBUG"] = "1"
    yield
    del os.environ["RAY_PICKLE_VERBOSE_DEBUG"]


@pytest.fixture()
def two_node_cluster():
    system_config = {
        "object_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    }
    cluster = ray.cluster_utils.Cluster(
        head_node_args={"_system_config": system_config})
    for _ in range(2):
        remote_node = cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    yield cluster, remote_node

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture()
def error_pubsub():
    p = init_error_pubsub()
    yield p
    p.close()


@pytest.fixture()
def log_pubsub():
    p = ray.worker.global_worker.redis_client.pubsub(
        ignore_subscribe_messages=True)
    log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
    p.psubscribe(log_channel)
    yield p
    p.close()


"""
Object spilling test fixture
"""
# -- Smart open param --
bucket_name = "object-spilling-test"

# -- File system param --
spill_local_path = "/tmp/spill"

# -- Spilling configs --
file_system_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": spill_local_path
    }
}

buffer_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": spill_local_path,
        "buffer_size": 1_000_000
    },
}

# Since we have differet protocol for a local external storage (e.g., fs)
# and distributed external storage (e.g., S3), we need to test both cases.
# This mocks the distributed fs with cluster utils.
mock_distributed_fs_object_spilling_config = {
    "type": "mock_distributed_fs",
    "params": {
        "directory_path": spill_local_path
    }
}
smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {
        "uri": f"s3://{bucket_name}/"
    }
}

unstable_object_spilling_config = {
    "type": "unstable_fs",
    "params": {
        "directory_path": spill_local_path,
    }
}

slow_object_spilling_config = {
    "type": "slow_fs",
    "params": {
        "directory_path": spill_local_path,
    }
}

pangu_dfs_object_spilling_config = {
    "type": "pangu_dfs",
    "params": {
        "cluster_addr": "dfs://f-pythonsdk-test.sh.aliyun-inc.com:10290/",
        "directory_path": spill_local_path,
    }
}


def create_object_spilling_config(request, tmp_path):
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    if (request.param["type"] == "filesystem"
            or request.param["type"] == "mock_distributed_fs"):
        request.param["params"]["directory_path"] = str(temp_folder)
    return json.dumps(request.param), temp_folder


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ])
def object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        mock_distributed_fs_object_spilling_config
    ])
def multi_node_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function", params=[
        unstable_object_spilling_config,
    ])
def unstable_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function", params=[
        slow_object_spilling_config,
    ])
def slow_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function", params=[
        pangu_dfs_object_spilling_config,
    ])
def pangu_dfs_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


skiplist = {
    ("test_cancel", "test_recursive_cancel[True]"),  # flaky
    ("test_cancel", "test_recursive_cancel[False]"),  # flaky
    ("test_reference_counting_2", "*"),  # unstable
    ("test_job", "test_job_gc_with_detached_actor"),
    # The unit of timestamp is seconds internally.
    ("test_job", "test_job_timestamps"),
    ("test_global_state", "test_uses_resources"),
    ("test_global_state", "test_add_remove_cluster_resources"),
    ("test_global_state", "test_replenish_resources"),
    ("test_command_runner", "*"),  # slow
    ("test_experimental_client", "*"),  # slow
    ("test_cli", "*"),  # slow
    # unstable because heartbeat timeout is too short
    ("test_reconstruction", "test_reconstruction_stress"),
    # unstable: brpc log prints to stderr
    ("test_failure_2", "test_gcs_server_failiure_report[ray_start_regular0]"),
    # flaky: Segmentation fault
    ("test_failure_2", "test_fate_sharing[False-False]"),
    ("test_failure_2", "test_fate_sharing[False-True]"),
    ("test_failure_2", "test_fate_sharing[True-False]"),
    ("test_failure_2", "test_fate_sharing[True-True]"),
    # unstable: execution time from a few seconds to tens of seconds
    # ("test_failure_2", "test_async_actor_task_retries[ray_start_regular0]"),
    ("test_dask_scheduler", "test_ray_dask_persist"),  # flaky
    ("test_ray_debugger", "*"),  # flaky
    # Raylet reconnecting GCS is disabled in GitHub. We re-enable it.
    ("test_multi_node", "test_remote_raylet_cleanup"),
    # Some GCS/redis related metrics are shared between components.
    # Especially, core worker adds some global tags for all used metrics.
    # It will conflict with other components due to
    # https://github.com/ray-project/ray/issues/10634.
    # Note that the global tags are different from the community version in
    # Ant.
    ("test_metrics_agent", "test_metrics_export_end_to_end"),

    # TO_BE_SOLVED: Skip flakey tests
    # Flakey in community version (commit: 3f6c23e3cc)
    ("test_advanced", "test_future_resolution_skip_plasma"),
    ("test_cancel", "test_comprehensive[True]"),
    ("test_cancel", "test_comprehensive[False]"),
    ("test_cancel", "test_fast[True]"),
    ("test_cancel", "test_fast[False]"),
    # Not important
    ("test_client", "test_stdout_log_stream"),
    # Not important
    ("test_memstat", "test_memory_used_output"),
    # Not important
    ("test_output", "test_worker_stdout"),
    # Skip rllib test
    ("test_client_library_integration", "test_rllib_integration"),
    # Depends on GitHub and DockerHub
    ("test_runtime_env_complicated", "test_experimental_package_github"),
    # Depends on conda
    ("test_client_proxy", "test_proxy_manager_bad_startup"),
    # Depends on ray namespace
    ("test_client_proxy", "test_multiple_clients_use_different_drivers"),
    # Autoscaler tests
    ("test_autoscaler", "*"),
    ("test_autoscaler_yaml", "*"),
    ("test_autoscaling_policy", "*"),
    ("test_resource_demand_scheduler", "*"),
    # Depends on S3 and is broken. Fixed in ray-1.6.0 in community.
    ("test_runtime_env", "test_get_wheel_filename"),
    # In Ant version, GcsActorManager::OnJobFinished destroys detached actors.
    ("test_client_builder", "test_namespace"),
    # We don't support keep detached actor alive after the driver being dead.
    ("test_multi_node_2", "test_detached_actor_autoscaling"),
    # Appending script dir to sys.path is not supported in Ray client fow now.
    ("test_runtime_env", "test_util_without_job_config"),
    # Auto-Run for Client is unsupported.
    # https://github.com/ray-project/ray/pull/18457
    ("test_scheduling_2", "test_placement_group_scheduling_strategy[True]"),
    # Very time consuming.
    ("test_scheduling_2", "test_node_affinity_scheduling_strategy[True]"),
    ("test_scheduling_2", "test_node_affinity_scheduling_strategy[False]"),
}

gcs_scheduling_skiplist = {
    ("test_actor_advanced", "test_actor_resource_demand"),
    ("test_actor_advanced", "test_kill_pending_actor_with_no_restart_false"),
    ("test_actor_resources", "test_lifetime_and_transient_resources"),
    ("test_advanced_2", "test_two_custom_resources"),
    # Need initial workers.
    ("test_advanced_3", "test_invalid_unicode_in_worker_log"),
    ("test_basic_3", "test_many_fractional_resources"),
    # Need initial workers.
    ("test_event",
     "test_event_file_in_worker_pool_child_process_signaled_callback"),
    ("test_event",
     "test_event_file_in_worker_pool_child_process_kill_by_raylet"),
    # Need initial workers.
    ("test_failure", "test_failed_function_to_run"),
    ("test_failure", "test_warning_all_tasks_blocked"),
    ("test_failure", "test_warning_actor_waiting_on_actor"),
    ("test_failure_2", "test_warning_for_infeasible_tasks"),
    ("test_failure_2", "test_warning_for_infeasible_zero_cpu_actor"),
    # Flaky, the order of actor and normal task is non-deterministic.
    ("test_failure", "test_warning_task_waiting_on_actor"),
    ("test_global_gc", "test_global_gc_actors"),
    # Need initial workers.
    ("test_global_state", "test_global_state_worker_table"),
    ("test_queue", "test_custom_resources"),
    ("test_multi_node_2", "test_heartbeats_single[ray_start_cluster_head0]"),
    ("test_multi_node_2", "test_heartbeats_single[ray_start_cluster_head1]"),
    # These tests can't run with PG default memory, so we add it here.
    # We add a redundant test to make sure
    # these tests running well in gcs scheduling.
    ("test_multi_node_3", "test_run_driver_twice"),
    ("test_placement_group", "test_placement_group_table[True]"),
    ("test_placement_group", "test_placement_group_table[False]"),
    ("test_placement_group", "test_mini_integration[True]"),
    ("test_placement_group", "test_mini_integration[False]"),
    (
        "test_placement_group",
        "test_create_placement_group_during_gcs_server_restart[ray_start_cluster_head0]"  # noqa: E501
    ),
    ("test_placement_group",
     "test_actor_scheduling_not_block_with_placement_group"),
    # requires default num_cpus == 1
    ("test_object_spilling_2",
     "test_no_release_during_plasma_fetch[object_spilling_config0]"),
    # Raylet reconnecting GCS is disabled in GitHub. We re-enable it.
    ("test_multi_node", "test_remote_raylet_cleanup"),
    # Need initial workers.
    ("test_multi_tenancy", "test_initial_workers"),
    ("test_multi_tenancy",
     "test_worker_registration_failure_after_driver_exit"),
    # Skip because dynamic resource is not supported anymore.
    ("test_actor_failures", "test_actor_restart_without_task"),
    # Skip because dynamic resource is not supported anymore.
    ("test_actor_failures", "test_ray_wait_dead_actor[ray_start_cluster0]"),
    # Skip because dynamic resource is not supported anymore.
    ("test_reference_counting", "test_actor_creation_task"),
    # requires default num_cpus == 1
    ("test_scheduling", "test_load_balancing"),
    ("test_scheduling", "test_load_balancing_with_dependencies[False]"),
    # new features for new_scheduler, it should be reopen
    # when new scheduler is integrated with gcs scheduler.
    ("test_scheduling", "test_spillback_waiting_task_on_oom"),
    # placement group lacks of memory resource.
    ("test_client_library_integration", "test_serve_handle"),
}

raylet_scheduling_skiplist = {
    ("test_placement_group_gcs_scheduling", "*"),
    ("test_scheduling_actor_affinity_strategy", "*"),
    ("test_runtime_env_failed", "*"),
    ("test_client_library_integration", "test_serve_handle"),
}


def pytest_collection_modifyitems(config, items):

    skip_mark = pytest.mark.skip(reason="This case is unneeded or flaky.")
    for item in items:
        # bazel test will give such module name:
        # python.ray.tests.test_client_builder, so we need to substr firstly.
        name = item.module.__name__
        pos = name.rfind(".")
        if pos != -1:
            name = name[pos + 1:]
        if ((name, "*") in skiplist or (name, item.name) in skiplist):
            item.add_marker(skip_mark)
            continue
        if gcs_task_scheduling_enabled():
            if ((name, "*") in gcs_scheduling_skiplist) \
                or ((name, item.name)
                    in gcs_scheduling_skiplist):
                item.add_marker(skip_mark)
        else:
            if ((name, "*") in raylet_scheduling_skiplist
                    or (name, item.name) in raylet_scheduling_skiplist):
                item.add_marker(skip_mark)
