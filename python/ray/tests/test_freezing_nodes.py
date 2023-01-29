import ray
import sys
import logging
import pytest
from ray.cluster_utils import Cluster
from ray.test_utils import wait_for_condition
from ray.util.placement_group import placement_group
from ray.exceptions import RayActorError
from ray.ray_constants import gcs_task_scheduling_enabled
logger = logging.getLogger(__name__)


def test_single_node_task(shutdown_only):
    cluster = Cluster()
    node = cluster.add_node(node_name="node1")

    ray.init(cluster.address)

    @ray.remote
    def test_func():
        return True

    def is_node_frozen():
        ready_list, unready_list = ray.wait([test_func.remote()], timeout=5)
        return len(ready_list) == 0

    # freeze this node, task can't be executed
    ray.internal.freeze_nodes([node._node_name])
    wait_for_condition(is_node_frozen)

    obj = test_func.remote()
    ready_list, unready_list = ray.wait([obj], timeout=5)
    # remote object won't be ready
    assert len(ready_list) == 0

    # unfreeze this node, task can be executed
    ray.internal.unfreeze_nodes([node._node_name])
    ready_list, unready_list = ray.wait([obj], timeout=5)
    assert len(ready_list) == 1


def test_two_nodes_task(shutdown_only):
    cluster = Cluster()
    node1 = cluster.add_node(node_name="node1")
    assert node1._node_name == "node1"
    node2 = cluster.add_node(node_name="node2")
    assert node2._node_name == "node2"

    ray.init(address=cluster.address)

    @ray.remote
    def get_node_name():
        return ray.worker._global_node._node_name

    def are_nodes_frozen():
        ready_list, unready_list = ray.wait(
            [get_node_name.remote()], timeout=5)
        return len(ready_list) == 0

    # freeze all nodes
    ray.internal.freeze_nodes([node1._node_name, node2._node_name])
    wait_for_condition(are_nodes_frozen)

    # unfreeze node2, task should be executed in node2
    ray.internal.unfreeze_nodes([node2._node_name])
    for i in range(10):
        ready_list, unready_list = ray.wait([get_node_name.remote()])
        assert len(unready_list) == 0
        assert ray.get(ready_list)[0] == node2._node_name

    # freeze all nodes
    ray.internal.freeze_nodes([node1._node_name, node2._node_name])
    wait_for_condition(are_nodes_frozen)

    # unfreeze node1, task should be executed in node1
    ray.internal.unfreeze_nodes([node1._node_name])
    for i in range(10):
        ready_list, unready_list = ray.wait([get_node_name.remote()])
        assert len(unready_list) == 0
        assert ray.get(ready_list)[0] == node1._node_name


def test_single_node_actor(shutdown_only):
    cluster = Cluster()
    node = cluster.add_node(node_name="node1")

    ray.init(cluster.address)

    @ray.remote
    class TestActor:
        def test_func(self):
            return True

    def is_node_frozen():
        actor = TestActor.remote()
        ready_list, unready_list = ray.wait(
            [actor.test_func.remote()], timeout=5)
        return len(ready_list) == 0

    # freeze this node, actor can't be scheduled
    ray.internal.freeze_nodes([node._node_name])
    wait_for_condition(is_node_frozen)

    actor2 = TestActor.remote()
    # since actor can't be scheduled, actor task won't be executed.
    obj = actor2.test_func.remote()
    ready_list, unready_list = ray.wait([obj], timeout=5)
    assert len(ready_list) == 0

    # unfreeze this node, actor can be scheduled since then.
    ray.internal.unfreeze_nodes([node._node_name])
    ready_list, unready_list = ray.wait([obj], timeout=5)
    assert len(ready_list) == 1


def test_two_nodes_actor(shutdown_only):
    cluster = Cluster()
    node1 = cluster.add_node(node_name="node1")
    assert node1._node_name == "node1"
    node2 = cluster.add_node(node_name="node2")
    assert node2._node_name == "node2"

    ray.init(address=cluster.address)

    @ray.remote
    class TestActor:
        def get_node_name(self):
            return ray.worker._global_node._node_name

    def are_nodes_frozen():
        actor = TestActor.remote()
        ready_list, unready_list = ray.wait(
            [actor.get_node_name.remote()], timeout=5)
        return len(ready_list) == 0

    # freeze all nodes
    ray.internal.freeze_nodes([node1._node_name, node2._node_name])
    wait_for_condition(are_nodes_frozen)

    # unfreeze node1, actor should be scheduled in node1
    ray.internal.unfreeze_nodes([node1._node_name])
    for i in range(5):
        actor = TestActor.remote()
        ready_list, unready_list = ray.wait([actor.get_node_name.remote()])
        assert len(unready_list) == 0
        assert ray.get(ready_list)[0] == node1._node_name

    # freeze all nodes
    ray.internal.freeze_nodes([node1._node_name, node2._node_name])
    wait_for_condition(are_nodes_frozen)

    # unfreeze node2, actor should be scheduled in node2
    ray.internal.unfreeze_nodes([node2._node_name])
    for i in range(5):
        actor = TestActor.remote()
        ready_list, unready_list = ray.wait([actor.get_node_name.remote()])
        assert len(unready_list) == 0
        assert ray.get(ready_list)[0] == node2._node_name


MB = 1024 * 1024


def test_placement_group(shutdown_only):
    cluster = Cluster()
    node1 = cluster.add_node(
        node_name="node1", num_cpus=4, resources={"extra_resource": 2})
    ray.init(address=cluster.address)

    # Create a placement group
    bundle1 = {"extra_resource": 2, "CPU": 2, "memory": 50 * 1024 * 1024}
    pg = placement_group([bundle1], strategy="STRICT_PACK")
    assert pg.wait(5)

    # Then freeze the node
    ray.internal.freeze_nodes([node1._node_name])

    # Actor can't be scheduled to this placement group
    @ray.remote(resources={"extra_resource": 1})
    class ExResActor:
        def __init__(self):
            pass

        def ping(self):
            return True

    actor = ExResActor.options(placement_group=pg).remote()
    ready_list, unready_list = ray.wait([actor.ping.remote()], timeout=5)
    assert len(ready_list) == 0


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="PG migration doesn't support raylet scheduleing now.")
def test_placement_group_migration(shutdown_only):
    cluster = Cluster()
    node1 = cluster.add_node(node_name="node1", num_cpus=4)
    node2 = cluster.add_node(node_name="node2", num_cpus=4)
    ray.init(address=cluster.address)

    # Create a placement group
    bundle1 = {"CPU": 2, "memory": 50 * 1024 * 1024}
    pg = placement_group([bundle1], strategy="STRICT_PACK")
    assert pg.wait(5)

    # Create a actor using this bundle
    @ray.remote(max_restarts=3)
    class TestActor:
        def get_node_name(self):
            return ray.worker._global_node._node_name

    actor = TestActor.options(placement_group=pg).remote()
    ready_list, unready_list = ray.wait(
        [actor.get_node_name.remote()], timeout=10)
    assert len(ready_list) == 1
    node_name_first = ray.get(ready_list[0])
    assert node_name_first == node1._node_name \
        or node_name_first == node2._node_name

    # Then freeze this node, and kill the actor
    # The PG will be migrated to the other node.
    # The actor will restart to the other node.
    ray.internal.freeze_nodes([node_name_first])
    ray.kill(actor, no_restart=False)

    def wait_for_alive():
        try:
            res = ray.get(actor.get_node_name.remote())
            return res is not None and res != node_name_first
        except RayActorError:
            return False

    # Wait for actor to restart
    wait_for_condition(wait_for_alive, timeout=10)

    # Check the new node name
    ready_list, unready_list = ray.wait(
        [actor.get_node_name.remote()], timeout=10)
    assert len(ready_list) == 1
    node_name_second = ray.get(ready_list[0])
    assert node_name_second == node1._node_name \
        or node_name_second == node2._node_name
    assert node_name_second != node_name_first


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="PG migration doesn't support raylet scheduleing now.")
def test_placement_group_migration_when_new_node_added(shutdown_only):
    cluster = Cluster()
    node1 = cluster.add_node(node_name="node1", num_cpus=4)
    ray.init(address=cluster.address)

    # Create a placement group
    bundle1 = {"CPU": 2, "memory": 50 * 1024 * 1024}
    pg = placement_group([bundle1], strategy="STRICT_PACK")
    assert pg.wait(5)

    # Then freeze the node
    ray.internal.freeze_nodes([node1._node_name])

    # Add a new node, the PG will be migrated to the new node.
    node2 = cluster.add_node(node_name="node2", num_cpus=4)

    # Then create a actor using this PG, it should be scheduled to node2
    @ray.remote
    class TestActor:
        def get_node_name(self):
            return ray.worker._global_node._node_name

    actor = TestActor.options(placement_group=pg).remote()
    ready_list, unready_list = ray.wait(
        [actor.get_node_name.remote()], timeout=10)
    assert len(ready_list) == 1
    assert ray.get(ready_list[0]) == node2._node_name


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
