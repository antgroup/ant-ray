import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.state
from ray.test_utils import (generate_system_config_map, get_other_nodes,
                            run_string_as_driver, wait_for_condition,
                            generate_config_map)
import ray.cluster_utils

MB = 1024 * 1024


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


def test_placement_group_table(ray_start_cluster):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4, memory=100 * MB)
    ray.init(address=cluster.address)

    # Originally placement group creation should be pending because
    # there are no resources.
    name = "name"
    strategy = "PACK"
    bundles = [{
        "CPU": 2,
        "GPU": 1,
        "memory": 50 * MB
    }, {
        "CPU": 2,
        "memory": 50 * MB
    }]
    placement_group = ray.util.placement_group(
        name=name, strategy=strategy, bundles=bundles)
    result = ray.util.placement_group_table(placement_group)
    assert result["name"] == name
    assert result["strategy"] == strategy
    for i in range(len(bundles)):
        assert bundles[i] == result["bundles"][i]
    assert result["state"] == "PENDING"

    # Now the placement group should be scheduled.
    cluster.add_node(num_cpus=5, num_gpus=1, memory=100 * MB)

    cluster.wait_for_nodes()
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    ray.get(actor_1.value.remote())

    result = ray.util.placement_group_table(placement_group)
    assert result["state"] == "CREATED"

    # Add two more placement group for placement group table test.
    second_strategy = "SPREAD"
    bundles = [{
        "CPU": 2,
        "GPU": 1,
        "memory": 50 * MB
    }, {
        "CPU": 2,
        "memory": 50 * MB
    }]
    ray.util.placement_group(
        name="second_placement_group",
        strategy=second_strategy,
        bundles=bundles)
    bundles = [{
        "CPU": 2,
        "GPU": 1,
        "memory": 50 * MB
    }, {
        "CPU": 2,
        "memory": 50 * MB
    }]
    ray.util.placement_group(
        name="third_placement_group",
        strategy=second_strategy,
        bundles=bundles)

    placement_group_table = ray.util.placement_group_table()
    assert len(placement_group_table) == 3

    true_name_set = {"name", "second_placement_group", "third_placement_group"}
    get_name_set = set()

    for _, placement_group_data in placement_group_table.items():
        get_name_set.add(placement_group_data["name"])

    assert true_name_set == get_name_set


@pytest.mark.skip(reason="Skip this ut because it's flaky.")
def test_mini_integration(ray_start_cluster):
    # Create bundles as many as number of gpus in the cluster.
    # Do some random work and make sure all resources are properly recovered.

    cluster = ray_start_cluster

    num_nodes = 5
    per_bundle_gpus = 2
    gpu_per_node = 4
    total_gpus = num_nodes * per_bundle_gpus * gpu_per_node
    per_node_gpus = per_bundle_gpus * gpu_per_node

    bundles_per_pg = 2
    total_num_pg = total_gpus // (bundles_per_pg * per_bundle_gpus)

    [
        cluster.add_node(num_cpus=2, num_gpus=per_bundle_gpus * gpu_per_node)
        for _ in range(num_nodes)
    ]
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0, num_gpus=1, memory=50 * MB)
    def random_tasks():
        import time
        import random
        sleep_time = random.uniform(0.1, 0.2)
        time.sleep(sleep_time)
        return True

    pgs = []
    pg_tasks = []
    # total bundle gpu usage = bundles_per_pg * total_num_pg * per_bundle_gpus
    # Note this is half of total
    for index in range(total_num_pg):
        pgs.append(
            ray.util.placement_group(
                name=f"name{index}",
                strategy="PACK",
                bundles=[{
                    "GPU": per_bundle_gpus,
                    "memory": 100 * MB
                } for _ in range(bundles_per_pg)]))

    # Schedule tasks.
    for i in range(total_num_pg):
        pg = pgs[i]
        pg_tasks.append([
            random_tasks.options(
                placement_group=pg,
                placement_group_bundle_index=bundle_index).remote()
            for bundle_index in range(bundles_per_pg)
        ])

    # Make sure tasks are done and we remove placement groups.
    num_removed_pg = 0
    pg_indexes = [2, 3, 1, 7, 8, 9, 0, 6, 4, 5]
    while num_removed_pg < total_num_pg:
        index = pg_indexes[num_removed_pg]
        pg = pgs[index]
        assert all(ray.get(pg_tasks[index]))
        ray.util.remove_placement_group(pg)
        num_removed_pg += 1

    @ray.remote(num_cpus=2, num_gpus=per_node_gpus, memory=50 * MB)
    class A:
        def ping(self):
            return True

    # Make sure all resources are properly returned by scheduling
    # actors that take up all existing resources.
    actors = [A.remote() for _ in range(num_nodes)]
    assert all(ray.get([a.ping.remote() for a in actors]))


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_config_map(
            num_heartbeats_timeout=20,
            ping_gcs_rpc_server_max_retries=60,
            job_config=ray.job_config.JobConfig(total_memory_mb=10000))
    ],
    indirect=True)
def test_create_placement_group_during_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=20, memory=10000 * MB)
    cluster.wait_for_nodes()

    # Create placement groups during gcs server restart.
    placement_groups = []
    for i in range(0, 100):
        placement_group = ray.util.placement_group([{
            "CPU": 0.1,
            "memory": 50 * MB
        }, {
            "CPU": 0.1,
            "memory": 50 * MB
        }])
        placement_groups.append(placement_group)

    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    for i in range(0, 100):
        assert placement_groups[i].wait(10000)


def test_placement_group_memory_resource_validate(ray_start_cluster):
    cluster = ray_start_cluster
    ray.init(address=cluster.address)

    error_count = 0
    try:
        ray.util.placement_group(
            name="name", strategy="PACK", bundles=[{
                "memory": 0.1,
            }])
    except ValueError:
        error_count = error_count + 1
    assert error_count == 1


def test_placement_group_remove_bundles_api_basic(ray_start_cluster):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, memory=100 * MB)
    ray.init(address=cluster.address)

    # Create a infeasible placement group first.
    infeasible_placement_group = ray.util.placement_group(
        name="name",
        strategy="PACK",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }, {
            "CPU": 4,
            "memory": 50 * MB
        }])
    assert not infeasible_placement_group.wait(3)
    # Make sure the remove bundles request will fail since it is pending now.
    with pytest.raises(
            ray.exceptions.RaySystemError,
            match="the placement group is in scheduling now"):
        infeasible_placement_group.remove_bundles([1])

    # Remove the infeasible placement group.
    ray.util.remove_placement_group(infeasible_placement_group)

    def is_placement_group_removed():
        table = ray.util.placement_group_table(infeasible_placement_group)
        if "state" not in table:
            return False
        return table["state"] == "REMOVED"

    wait_for_condition(is_placement_group_removed)

    # Create another placement group that can be scheduled.
    placement_group = ray.util.placement_group(
        name="name",
        strategy="PACK",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }, {
            "CPU": 2,
            "memory": 50 * MB
        }])
    # Wait its creation done.
    assert placement_group.wait(5)

    # Schedule a normal task to let the core worker
    # register a bundles changed listener.
    @ray.remote(num_cpus=2, memory=50 * MB)
    def dummy_task():
        return True

    ray.get(
        dummy_task.options(
            placement_group=placement_group,
            placement_group_bundle_index=0).remote())

    # Remove the second bundle.
    placement_group.remove_bundles([1])
    assert placement_group.wait(5)

    # Validate the metadata information.
    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 2
    assert table["state"] == "CREATED"
    assert table["bundles_status"][0] == "VALID"
    assert table["bundles_status"][1] == "INVALID"

    # Make sure the actor creation will fail as the second bundle has
    # been removed. We need this test to make sure the core worker will
    # receive the bundles changed event and refresh the local bundles view.
    with pytest.raises(
            ray.exceptions.RaySystemError, match="Invalid bundle index"):
        Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=1).remote()


def test_placement_group_add_bundles_api_basic(ray_start_cluster):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, memory=100 * MB)
    ray.init(address=cluster.address)

    # Create a infeasible placement group first.
    infeasible_placement_group = ray.util.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 8,
            "memory": 50 * MB
        }])
    assert not infeasible_placement_group.wait(4)
    # Make sure the add bundles request will fail since it is pending now.
    with pytest.raises(
            ray.exceptions.RaySystemError,
            match="the placement group is in scheduling now"):
        infeasible_placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])

    # Remove the infeasible placement group.
    ray.util.remove_placement_group(infeasible_placement_group)

    def is_placement_group_removed():
        table = ray.util.placement_group_table(infeasible_placement_group)
        if "state" not in table:
            return False
        return table["state"] == "REMOVED"

    wait_for_condition(is_placement_group_removed)

    # Create a feasible placement group now.
    placement_group = ray.util.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }])

    # Wait for the placement group to create successfully.
    assert placement_group.wait(5)

    placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])
    # Wait for the add new bundles operation to finish.
    assert placement_group.wait(5)

    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 2
    assert table["state"] == "CREATED"

    # Schedule an actor through the new bundle index.
    actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    ray.get(actor.value.remote())


def test_placement_group_add_and_remove_bundles(ray_start_cluster):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, memory=100 * MB)
    ray.init(address=cluster.address)

    # Create a feasible placement group now.
    placement_group = ray.util.placement_group(
        name="name", strategy="PACK", bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }])
    # Wait for the placement group to create successfully.
    assert placement_group.wait(5)

    placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])
    # Wait for the add new bundles operation to create done.
    assert placement_group.wait(5)
    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 2
    assert table["state"] == "CREATED"

    # Remove the second bundle.
    placement_group.remove_bundles([1])
    assert placement_group.wait(5)

    # Add the third bundle.
    placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])
    assert placement_group.wait(5)

    # Validate the metadata information.
    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 3
    assert table["state"] == "CREATED"
    assert table["bundles_status"][0] == "VALID"
    assert table["bundles_status"][1] == "INVALID"
    assert table["bundles_status"][2] == "VALID"


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_resizing_api_during_gcs_server_restart_insufficient_resource(
        ray_start_cluster_head):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2, memory=50 * MB)
    cluster.wait_for_nodes()

    placement_group = ray.util.placement_group(
        name="name",
        strategy="SPREAD",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }])
    assert placement_group.wait(5)
    placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])

    # Restart gcs server.
    # Note: The state of the placement group must be `UPDATING` now
    # since we just have one node that resource is 2 CPU.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    cluster.add_node(num_cpus=2, memory=50 * MB)
    cluster.wait_for_nodes()

    # Validate metadata information.
    assert placement_group.wait(20)
    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 2
    assert table["state"] == "CREATED"
    assert table["bundles_status"][0] == "VALID"
    assert table["bundles_status"][1] == "VALID"

    # Schedule an actor with the first bundle.
    actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()

    ray.get(actor.value.remote())
    ray.shutdown()


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_resizing_api_during_gcs_server_restart_sufficient_resource(
        ray_start_cluster_head):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=4, memory=100 * MB)
    cluster.wait_for_nodes()

    placement_group = ray.util.placement_group(
        name="name",
        strategy="SPREAD",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }])
    assert placement_group.wait(5)
    placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])

    # Restart gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Validate metadata information.
    assert placement_group.wait(20)
    table = ray.util.placement_group_table(placement_group)
    assert len(list(table["bundles"].values())) == 2
    assert table["state"] == "CREATED"
    assert table["bundles_status"][0] == "VALID"
    assert table["bundles_status"][1] == "VALID"

    # Schedule an actor with the second bundle.
    actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    ray.get(actor.value.remote())
    ray.shutdown()


def test_detached_pg_cleanup_when_job_dead(ray_start_cluster):
    cluster = ray_start_cluster
    for _ in range(2):
        cluster.add_node(num_cpus=3, memory=200 * MB)
    cluster.wait_for_nodes()
    info = ray.init(address=cluster.address)
    global_placement_group_name = "named_placement_group"

    # Create a detached placement group with name.
    driver_code = f"""
import ray

ray.init(address="{info["redis_address"]}")

pg = ray.util.placement_group(
        [{{"CPU": 1, "memory": 50 * {MB}}} for _ in range(2)],
        strategy="STRICT_SPREAD",
        name="{global_placement_group_name}",
        lifetime="detached")
assert pg.wait(100)

ray.shutdown()
    """

    run_string_as_driver(driver_code)

    # Wait until the driver is reported as dead by GCS.
    def is_job_done():
        jobs = ray.state.jobs()
        for job in jobs:
            if job["IsDead"]:
                return True
        return False

    wait_for_condition(is_job_done)

    # Get the named placement group and make sure it isn't exist.
    with pytest.raises(
            ValueError, match="Failed to look up placement group with name"):
        ray.util.get_placement_group(global_placement_group_name)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_config_map(
            num_heartbeats_timeout=20,
            ping_gcs_rpc_server_max_retries=60,
            job_config=ray.job_config.JobConfig(total_memory_mb=100),
            enable_job_quota=True)
    ],
    indirect=True)
def test_named_placement_group_clean_insufficient_total_memory(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2, memory=100 * MB)
    cluster.wait_for_nodes()
    placement_group_name = "named_placement_group"

    # Get the named placement group and make sure it isn't exist.
    with pytest.raises(
            ValueError, match="Failed to look up placement group with name"):
        ray.util.get_placement_group(placement_group_name)

    # Create a placement group that required resource bigger that total
    # memory MB and make sure it will throw a system error.
    with pytest.raises(
            ray.exceptions.RaySystemError,
            match="as the job resource requirements are not enough"):
        ray.util.placement_group(
            [{
                "CPU": 1,
                "memory": 200 * MB
            }], name=placement_group_name)

    # Get the named placement group and make sure it will not crash the GCS.
    with pytest.raises(
            ValueError, match="Failed to look up placement group with name"):
        ray.util.get_placement_group(placement_group_name)


def test_remove_bundles_correct_when_packing(ray_start_cluster):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, memory=100 * MB)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="PACK",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }, {
            "CPU": 2,
            "memory": 50 * MB
        }])
    # Wait for the placement group to create successfully.
    assert placement_group.wait(5)

    actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    destroying_actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    # Make sure that the worker has been leased successfully.
    ray.get(actor.value.remote())
    ray.get(destroying_actor.value.remote())

    # Remove the second bundle.
    placement_group.remove_bundles([1])
    assert placement_group.wait(5)

    # Make sure that remove the second bundle will not affert the first bundle.
    ray.get(actor.value.remote())

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(destroying_actor.value.remote())


def test_cluster_resource_released_when_removing_bundles(ray_start_cluster):
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=2, memory=50 * MB)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }, {
            "CPU": 2,
            "memory": 50 * MB
        }])
    assert placement_group.wait(15)
    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())

    # Remove the first bundle.
    placement_group.remove_bundles([0])
    assert placement_group.wait(5)

    # Make sure the new actor scheduling successfully.
    actor_3 = Actor.options().remote()
    ray.get(actor_3.value.remote())


def test_return_resource_correct_when_removing_placement_group(
        ray_start_cluster):
    @ray.remote(num_cpus=1, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1, memory=50 * MB)
    ray.init(address=cluster.address)

    # Create two placement groups each require 1 CPU that
    # will occupy whold resources of one node.
    pgs = []
    for _ in range(2):
        pgs.append(
            ray.util.placement_group(
                strategy="STRICT_SPREAD",
                bundles=[{
                    "CPU": 1,
                    "memory": 50 * MB
                }]))

    # Create an actor at each placement groups and
    # make sure they will be created successfully.
    actors = []
    for pg in pgs:
        assert pg.wait(5)
        actor = Actor.options(
            placement_group=pg, placement_group_bundle_index=0).remote()
        ray.get(actor.value.remote())
        actors.append(actor)

    # Remove the first actor and placement group then create
    # a new placement group and a actor and make sure
    # they will be created successfully.
    ray.kill(actors[0])
    ray.util.remove_placement_group(pgs[0])

    new_pg = ray.util.placement_group(
        strategy="STRICT_SPREAD", bundles=[{
            "CPU": 1,
            "memory": 50 * MB
        }])
    assert new_pg.wait(10)

    new_actor = Actor.options(
        placement_group=new_pg, placement_group_bundle_index=0).remote()
    ray.get(new_actor.value.remote())


def test_cluster_resource_released_when_adding_bundles(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=2, memory=50 * MB)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }, {
            "CPU": 2,
            "memory": 50 * MB
        }])
    assert placement_group.wait(5)

    placement_group.remove_bundles([0])
    assert placement_group.wait(5)

    placement_group.add_bundles([{"CPU": 2, "memory": 50 * MB}])
    assert placement_group.wait(5)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_pending_actor_created_successful_when_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.wait_for_nodes()

    # Create a placement group and make sure it will
    # pending as the current resource is not sufficient.
    placement_group = ray.util.placement_group([{
        "CPU": 2,
        "memory": 50 * MB
    }, {
        "CPU": 2,
        "memory": 50 * MB
    }])
    assert not placement_group.wait(5)

    table = ray.util.placement_group_table(placement_group)
    assert table["state"] == "PENDING"

    # Create an actor using the above placement group,
    # so it will be pending either.
    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()

    # Restart the gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Add new nodes to make the above placement group create successfully.
    cluster.add_node(num_cpus=2, memory=200 * MB)
    cluster.add_node(num_cpus=2, memory=200 * MB)
    cluster.wait_for_nodes()

    # pg should be created successfully.
    assert placement_group.wait(5)

    # actor should be schedulered either.
    ray.get(actor.value.remote())


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=10, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_bundle_recreated_when_raylet_fo_after_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2, memory=50 * MB)
    cluster.wait_for_nodes()

    # Create one placement group and make sure its creation successfully.
    placement_group = ray.util.placement_group([{"CPU": 2, "memory": 50 * MB}])
    assert placement_group.wait(10)
    table = ray.util.placement_group_table(placement_group)
    assert table["state"] == "CREATED"

    # Restart gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Restart the raylet.
    cluster.remove_node(get_other_nodes(cluster, exclude_head=True)[-1])
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Schedule an actor and make sure its creaton successfully.
    actor = Increase.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()

    assert ray.get(actor.method.remote(1), timeout=5) == 3


def test_task_successfully_restart_when_remove_bundles(ray_start_cluster):
    @ray.remote
    class Counter:
        def __init__(self):
            self._value = 0

        def increase(self):
            self._value += 1

        def value(self):
            return self._value

    @ray.remote(num_cpus=2, memory=50 * MB)
    class Actor(object):
        def __init__(self, counter):
            self._counter = counter
            ray.get(counter.increase.remote())

        def value(self):
            return ray.get(self._counter.value.remote())

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=5, memory=150 * MB)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_PACK",
        bundles=[{
            "CPU": 2,
            "memory": 50 * MB
        }, {
            "CPU": 2,
            "memory": 50 * MB
        }])
    # Wait for the placement group to create successfully.
    assert placement_group.wait(5)

    counter = Counter.remote()

    def wait_counter_value(value):
        return ray.get(counter.value.remote()) == value

    actor_1 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0).remote(counter)
    actor_2 = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=1,
        max_restarts=-1).remote(counter)

    # Make sure that the worker has been leased successfully.
    wait_for_condition(lambda: wait_counter_value(2))

    # Remove the fist bundle.
    placement_group.remove_bundles([0])
    assert placement_group.wait(5)

    # Make sure that remove the first bundle will
    # not affert the second bundle firstly.
    assert ray.get(actor_2.value.remote()) == 2

    def is_actor_dead(actor):
        try:
            ray.get(actor.value.remote())
        except ray.exceptions.RayActorError:
            return True
        return False

    wait_for_condition(lambda: is_actor_dead(actor_1))

    # Now, kill the second actor and make sure it will successfully restart.
    ray.kill(actor_2, no_restart=False)
    wait_for_condition(lambda: wait_counter_value(3))
    assert ray.get(actor_2.value.remote()) == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
