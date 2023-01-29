import pytest
import sys
import time

import ray
import ray.experimental.internal_kv as internal_kv
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.scheduling_strategies import (NodeAffinitySchedulingStrategy)


@ray.remote
def get_node_id():
    return ray.get_runtime_context().node_id


@ray.remote(num_cpus=1)
def long_time_task():
    while internal_kv._internal_kv_exists("long_time_flag"):
        time.sleep(0.1)
    return "OK"


@ray.remote(num_cpus=1)
class Actor:
    def get_node_id(self):
        return ray.get_runtime_context().node_id


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_task_node_affinity(monkeypatch, ray_start_cluster, connect_to_client):
    monkeypatch.setenv("RAY_num_heartbeats_timeout", "4")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8, resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=8, resources={"worker": 1})
    cluster.wait_for_nodes()

    with connect_to_client_or_not(connect_to_client):
        head_node_id = ray.get(
            get_node_id.options(num_cpus=0, resources={
                "head": 1
            }).remote())
        worker_node_id = ray.get(
            get_node_id.options(num_cpus=0, resources={
                "worker": 1
            }).remote())

        assert worker_node_id == ray.get(
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    worker_node_id, soft=False)).remote())
        assert head_node_id == ray.get(
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    head_node_id, soft=False)).remote())

        # Doesn't fail when the node doesn't exist since soft is true.
        ray.get(
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.NodeID.from_random().hex(), soft=True)).remote())

        # Doesn't fail when the node is infeasible since soft is true.
        assert worker_node_id == ray.get(
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    head_node_id, soft=True),
                resources={
                    "worker": 1
                },
            ).remote())

        # Fail when the node doesn't exist.
        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            ray.get(
                get_node_id.options(
                    max_retries=0,
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        ray.NodeID.from_random().hex(), soft=False)).remote())

        # Fail when the node is infeasible.
        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            ray.get(
                get_node_id.options(
                    max_retries=0,
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        head_node_id, soft=False),
                    resources={
                        "not_exist": 1
                    },
                ).remote())

        crashed_worker_node = cluster.add_node(
            num_cpus=8, resources={"crashed_worker": 1})
        cluster.wait_for_nodes()
        crashed_worker_node_id = ray.get(
            get_node_id.options(num_cpus=0, resources={
                "crashed_worker": 1
            }).remote())

        @ray.remote(
            max_retries=-1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                crashed_worker_node_id, soft=True),
        )
        def crashed_get_node_id():
            if ray.get_runtime_context().node_id == crashed_worker_node_id:
                internal_kv._internal_kv_put("crashed_get_node_id",
                                             "crashed_worker_node_id")
                while True:
                    time.sleep(1)
            else:
                return ray.get_runtime_context().node_id

        r = crashed_get_node_id.remote()
        while not internal_kv._internal_kv_exists("crashed_get_node_id"):
            time.sleep(0.1)
        cluster.remove_node(crashed_worker_node, allow_graceful=True)
        assert ray.get(r) in {head_node_id, worker_node_id}


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_actor_node_affinity(monkeypatch, ray_start_cluster,
                             connect_to_client):
    monkeypatch.setenv("RAY_num_heartbeats_timeout", "4")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8, resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=8, resources={"worker": 1})
    cluster.wait_for_nodes()

    with connect_to_client_or_not(connect_to_client):
        head_node_id = ray.get(
            get_node_id.options(num_cpus=0, resources={
                "head": 1
            }).remote())
        worker_node_id = ray.get(
            get_node_id.options(num_cpus=0, resources={
                "worker": 1
            }).remote())

        actor = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                worker_node_id, soft=False)).remote()
        assert worker_node_id == ray.get(actor.get_node_id.remote())

        actor = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                head_node_id, soft=False)).remote()
        assert head_node_id == ray.get(actor.get_node_id.remote())

        # Wait until the target node becomes available.
        worker_actor = Actor.options(resources={"worker": 1}).remote()
        assert worker_node_id == ray.get(worker_actor.get_node_id.remote())
        actor = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                worker_node_id, soft=True),
            resources={
                "worker": 1
            },
        ).remote()
        del worker_actor
        assert worker_node_id == ray.get(actor.get_node_id.remote())

        # Doesn't fail when the node doesn't exist since soft is true.
        actor = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.NodeID.from_random().hex(), soft=True)).remote()
        assert ray.get(actor.get_node_id.remote())

        # Doesn't fail when the node is infeasible since soft is true.
        actor = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                head_node_id, soft=True),
            resources={
                "worker": 1
            },
        ).remote()
        assert worker_node_id == ray.get(actor.get_node_id.remote())

        # Fail when the node doesn't exist.
        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.NodeID.from_random().hex(), soft=False)).remote()
            ray.get(actor.get_node_id.remote())

        # Fail when the node is infeasible.
        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    worker_node_id, soft=False),
                resources={
                    "not_exist": 1
                },
            ).remote()
            ray.get(actor.get_node_id.remote())


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_task_multi_node_affinity(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # anti-affinity
        node_id_set = set()
        object_ref = get_node_id.options(num_cpus=1).remote()
        node_id_1 = ray.get(object_ref, timeout=5)
        node_id_set.add(node_id_1)

        object_ref = get_node_id.options(
            num_cpus=1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [node_id_1], soft=False, anti_affinity=True)).remote()
        node_id_2 = ray.get(object_ref, timeout=5)
        node_id_set.add(node_id_2)
        assert len(node_id_set) == 2

        object_ref = get_node_id.options(
            num_cpus=1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [node_id_1.hex(), node_id_2.hex()],
                soft=False,
                anti_affinity=True)).remote()
        node_id_3 = ray.get(object_ref, timeout=5)
        node_id_set.add(node_id_3)
        assert len(node_id_set) == 3

        # affinity
        object_ref = get_node_id.options(
            num_cpus=1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [node_id_1], soft=False, anti_affinity=False)).remote()
        assert ray.get(object_ref, timeout=5) == node_id_1

        # affinity can't set multi nodes.
        with pytest.raises(ray.exceptions.RaySystemError):
            object_ref = get_node_id.options(
                num_cpus=1,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    [node_id_2.hex(), node_id_3.hex()],
                    soft=False,
                    anti_affinity=False)).remote()

        # does not exist node
        object_ref = get_node_id.options(
            max_retries=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [ray.NodeID.from_random().hex()],
                soft=False,
                anti_affinity=True)).remote()
        assert ray.get(object_ref, timeout=5) in node_id_set

        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            object_ref = get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    [ray.NodeID.from_random().hex()],
                    soft=False,
                    anti_affinity=False)).remote()
            ray.get(object_ref, timeout=5)

        object_ref = get_node_id.options(
            max_retries=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [ray.NodeID.from_random().hex(), node_id_1, node_id_2],
                soft=False,
                anti_affinity=True)).remote()
        assert ray.get(object_ref, timeout=5) == node_id_3


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_actor_multi_node_affinity(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        # anti-affinity
        node_id_set = set()
        actor_1 = Actor.remote()
        node_id_1 = ray.get(actor_1.get_node_id.remote(), timeout=5)
        node_id_set.add(node_id_1)

        actor_2 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [node_id_1], soft=False, anti_affinity=True)).remote()
        node_id_2 = ray.get(actor_2.get_node_id.remote(), timeout=5)
        node_id_set.add(node_id_2)
        assert len(node_id_set) == 2

        actor_3 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [node_id_1.hex(), node_id_2.hex()],
                soft=False,
                anti_affinity=True)).remote()
        node_id_3 = ray.get(actor_3.get_node_id.remote(), timeout=5)
        node_id_set.add(node_id_3)
        assert len(node_id_set) == 3

        # affinity
        actor_4 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [node_id_1], soft=False, anti_affinity=False)).remote()
        assert ray.get(actor_4.get_node_id.remote(), timeout=5) == node_id_1

        # affinity can't set multi nodes.
        with pytest.raises(ray.exceptions.RaySystemError):
            Actor.options(
                num_cpus=1,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    [node_id_2.hex(), node_id_3.hex()],
                    soft=False,
                    anti_affinity=False)).remote()

        # does not exist node
        actor_6 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [ray.NodeID.from_random().hex()],
                soft=False,
                anti_affinity=True)).remote()
        assert ray.get(actor_6.get_node_id.remote(), timeout=5) in node_id_set

        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor_7 = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    [ray.NodeID.from_random().hex()],
                    soft=False,
                    anti_affinity=False)).remote()
            ray.get(actor_7.get_node_id.remote(), timeout=5)

        actor_8 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                [ray.NodeID.from_random().hex(), node_id_1, node_id_2],
                soft=False,
                anti_affinity=True)).remote()
        assert ray.get(actor_8.get_node_id.remote(), timeout=5) == node_id_3


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_node_affinity_resources_unavailable(ray_start_cluster,
                                             connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 2
    for i in range(num_nodes):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        actor_1 = Actor.remote()
        node_id_1 = ray.get(actor_1.get_node_id.remote(), timeout=5)

        actor_2 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id_1, soft=False)).remote()
        assert node_id_1 == ray.get(actor_2.get_node_id.remote(), timeout=5)

        # Insufficient resources for the first node
        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor_3 = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_1, soft=False)).remote()
            ray.get(actor_3.get_node_id.remote(), timeout=5)

        # Insufficient resources for the first node
        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            object_ref = get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_1, soft=False)).remote()
            ray.get(object_ref, timeout=5)

        node_id_2 = ray.get(
            get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_1, soft=True, anti_affinity=False)).remote())
        assert node_id_2 != node_id_1

        # Insufficient resources when actor anti-affinity
        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor_4 = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_2, soft=False, anti_affinity=True)).remote()
            ray.get(actor_4.get_node_id.remote(), timeout=5)

        # Insufficient resources when task anti-affinity
        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            object_ref = get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_2, soft=False, anti_affinity=True)).remote()
            ray.get(object_ref, timeout=5)

        # Schedule to another node when insufficient resources and soft
        actor_5 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id_1, soft=True, anti_affinity=False)).remote()
        assert ray.get(actor_5.get_node_id.remote(), timeout=5) == node_id_2


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_node_affinity_task_and_actor_resources_unavailable(
        ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 2
    for i in range(num_nodes):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):

        node_id_1 = ray.get(get_node_id.remote(), timeout=5)

        internal_kv._internal_kv_put("long_time_flag", "1")
        ref_1 = long_time_task.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id_1, soft=False)).remote()
        actor_2 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id_1, soft=False)).remote()
        assert node_id_1 == ray.get(actor_2.get_node_id.remote(), timeout=5)

        # Insufficient resources for the first node
        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor_3 = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_1, soft=False)).remote()
            ray.get(actor_3.get_node_id.remote(), timeout=5)

        # Insufficient resources for the first node
        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            object_ref = get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_1, soft=False)).remote()
            ray.get(object_ref, timeout=5)

        # soft affinity to another node
        node_id_2 = ray.get(
            get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_1, soft=True, anti_affinity=False)).remote())
        assert node_id_2 != node_id_1
        actor_5 = Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id_1, soft=True, anti_affinity=False)).remote()
        assert ray.get(actor_5.get_node_id.remote(), timeout=5) == node_id_2

        # Anti-affinity Insufficient resources for the first node
        with pytest.raises(ray.exceptions.ActorUnschedulableError):
            actor_6 = Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_2, soft=False, anti_affinity=True)).remote()
            ray.get(actor_6.get_node_id.remote(), timeout=5)

        # Anti-affinity Insufficient resources for the first node
        with pytest.raises(ray.exceptions.TaskUnschedulableError):
            object_ref = get_node_id.options(
                max_retries=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id_2, soft=False, anti_affinity=True)).remote()
            ray.get(object_ref, timeout=5)

        internal_kv._internal_kv_del("long_time_flag")
        assert ray.get(ref_1, timeout=5) == "OK"
        object_ref = get_node_id.options(
            max_retries=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id_1, soft=False)).remote()
        ray.get(object_ref, timeout=5)


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_node_affinity_exception(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 2
    for i in range(num_nodes):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        with pytest.raises(ray.exceptions.RaySystemError):
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    [], soft=False)).remote()

        with pytest.raises(ray.exceptions.RaySystemError):
            Actor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    [], soft=False, anti_affinity=True)).remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
