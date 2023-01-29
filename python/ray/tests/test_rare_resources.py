import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys

import ray
import ray.test_utils
import ray.cluster_utils
from ray.util.rare_resources import RareResourceType
from ray.util.rare_resources import RareResourceValue
from ray.ray_constants import (
    gcs_task_scheduling_enabled, )


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_rare_resources(ray_start_cluster):
    config = {
        "rare_resource_scheduling_enabled": True,
    }
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config=config,
        num_cpus=1,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        })
    cluster.add_node(
        num_cpus=1,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        })
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote
    class Actor1:
        def method(self):
            return 1

    @ray.remote
    class Actor2:
        def method(self):
            return 1

    @ray.remote
    class Actor3:
        def method(self):
            return 1

    a1 = Actor1.options(num_cpus=1).remote()
    a2 = Actor2.options(
        num_cpus=1,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        }).remote()
    a3 = Actor3.options(
        num_cpus=1,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        }).remote()

    assert ray.get(a1.method.remote()) == 1
    assert ray.get(a2.method.remote()) == 1
    assert ray.get(a3.method.remote()) == 1


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize("args", [True, False])
def test_enable_or_disable_rare_resources_scheduling(ray_start_cluster, args):
    rare_resource_scheduling_enabled = args

    cluster = ray_start_cluster
    config = {
        "rare_resource_scheduling_enabled": rare_resource_scheduling_enabled,
    }
    cluster.add_node(
        _system_config=config,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        })
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote
    class Actor:
        def method(self):
            return 1

    a = Actor.remote()
    if rare_resource_scheduling_enabled:
        ready_ids, _ = ray.wait([a.method.remote()], timeout=2)
        assert len(ready_ids) == 0
    else:
        assert ray.get(a.method.remote()) == 1


MB = 1024 * 1024


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_rare_resources_scheduling_with_pg(ray_start_cluster):
    cluster = ray_start_cluster
    config = {
        "rare_resource_scheduling_enabled": True,
    }
    cluster.add_node(
        _system_config=config,
        num_cpus=4,
        resources={RareResourceType.BIG_MEMORY: 2})
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote
    class Actor:
        def method(self):
            return 1

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_PACK",
        bundles=[{
            "CPU": 1,
            "memory": 50 * MB,
        }, {
            "CPU": 1,
            "memory": 50 * MB,
        }, {
            "CPU": 1,
            "memory": 50 * MB,
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY,
        }, {
            "CPU": 1,
            "memory": 50 * MB,
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY,
        }])
    assert placement_group.wait(10000)

    a1 = Actor.options(
        memory=50 * MB,
        placement_group=placement_group,
        placement_group_bundle_index=0).remote()
    assert ray.get(a1.method.remote()) == 1

    a2 = Actor.options(
        memory=50 * MB,
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    assert ray.get(a2.method.remote()) == 1

    a3 = Actor.options(
        memory=50 * MB,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        },
        placement_group=placement_group,
        placement_group_bundle_index=2).remote()
    assert ray.get(a3.method.remote()) == 1

    a4 = Actor.options(
        memory=50 * MB,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        },
        placement_group=placement_group,
        placement_group_bundle_index=3).remote()
    assert ray.get(a4.method.remote()) == 1


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_rare_resources_with_normal_tasks(ray_start_cluster):
    config = {
        "rare_resource_scheduling_enabled": True,
    }
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config=config,
        num_cpus=1,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        })
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote
    def func():
        return 1

    ready_ids, _ = ray.wait([func.remote()], timeout=2)
    assert len(ready_ids) == 0

    object = func.options(resources={
        RareResourceType.BIG_MEMORY: RareResourceValue.DEFAULT_VALUE_BIG_MEMORY
    }).remote()
    assert ray.get(object) == 1


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_rare_resources_using_most_resource_scorer(ray_start_cluster):
    config = {
        "rare_resource_scheduling_enabled": True,
        "num_candidate_nodes_for_scheduling": 1
    }
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config=config,
        memory=1024 * MB,
        resources={RareResourceType.BIG_MEMORY: 100})
    cluster.add_node(
        memory=2048 * MB, resources={RareResourceType.BIG_MEMORY: 100})
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote
    class Actor:
        def method(self):
            return ray.worker.global_worker.current_node_id

    actors = [
        Actor.options(
            memory=100 * MB,
            resources={
                RareResourceType.BIG_MEMORY: RareResourceValue.
                DEFAULT_VALUE_BIG_MEMORY
            }).remote() for _ in range(10)
    ]

    node_ids = {}
    for actor in actors:
        node_id = ray.get(actor.method.remote())
        if node_id not in node_ids:
            node_ids[node_id] = {}
    assert len(node_ids) == 1

    a = Actor.options(
        memory=2000 * MB,
        resources={
            RareResourceType.BIG_MEMORY: RareResourceValue.
            DEFAULT_VALUE_BIG_MEMORY
        }).remote()
    node_id = ray.get(a.method.remote())
    assert node_id not in node_ids


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
