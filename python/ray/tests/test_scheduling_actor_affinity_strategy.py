import pytest
import sys

import ray
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray.util.scheduling_strategies import (ActorAffinityOperator,
                                            ActorAffinitySchedulingStrategy,
                                            ActorAffinityMatchExpression)
from ray.test_utils import (
    run_string_as_driver_nonblocking, )


@ray.remote(num_cpus=1)
class Actor(object):
    def __init__(self):
        self.n = 0

    def value(self):
        return self.n


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_actor_affinity(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=3)
    ray.init(address=cluster.address)

    with connect_to_client_or_not(connect_to_client):
        actor_1 = Actor.options(labels={
            "location": "dc_1",
            "version": "1"
        }).remote()
        ray.get(actor_1.value.remote(), timeout=10)
        actor_2 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression(
                    "location", ActorAffinityOperator.IN, ["dc_1"], False)
            ])).remote()
        actor_3 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression(
                    "location", ActorAffinityOperator.NOT_IN, ["dc_1"], False)
            ])).remote()
        actor_4 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression(
                    "location", ActorAffinityOperator.EXISTS, [], False)
            ])).remote()
        actor_5 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression(
                    "location", ActorAffinityOperator.DOES_NOT_EXIST, [],
                    False),
                ActorAffinityMatchExpression("actor_id",
                                             ActorAffinityOperator.NOT_IN,
                                             [actor_3._actor_id.hex()], False)
            ])).remote()
        ray.get(actor_2.value.remote(), timeout=10)
        ray.get(actor_3.value.remote(), timeout=10)
        ray.get(actor_4.value.remote(), timeout=10)
        ray.get(actor_5.value.remote(), timeout=10)

        assert getActorNodeID(actor_1) == getActorNodeID(actor_2)
        assert getActorNodeID(actor_1) == getActorNodeID(actor_4)

        assert getActorNodeID(actor_1) != getActorNodeID(actor_3)
        assert getActorNodeID(actor_1) != getActorNodeID(actor_5)

        actor_other_node = Actor.options(labels={
            "location": "dc_2",
            "version": "1"
        }).remote()
        ray.get(actor_other_node.value.remote(), timeout=10)
        assert getActorNodeID(actor_1) != getActorNodeID(actor_other_node)
        actor_2_1 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression("location", ActorAffinityOperator.
                                             IN, ["dc_1", "dc_2"], False)
            ])).remote()
        ray.get(actor_2_1.value.remote(), timeout=10)
        assert getActorNodeID(actor_2_1) == getActorNodeID(actor_other_node)
        actor_2_2 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression("location", ActorAffinityOperator.
                                             NOT_IN, ["dc_1", "dc_2"], False)
            ])).remote()
        ray.get(actor_2_2.value.remote(), timeout=10)
        assert getActorNodeID(actor_2_2) != getActorNodeID(actor_other_node)
        assert getActorNodeID(actor_2_2) != getActorNodeID(actor_1)

        actor_2_3 = Actor.options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression("location", ActorAffinityOperator.
                                             IN, ["dc_1", "dc_2"], False)
            ])).remote()
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.get(actor_2_3.value.remote(), timeout=3)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 2,
    }], indirect=True)
def test_soft_actor_affinity(ray_start_cluster):
    actors = []
    for i in range(4):
        actor = Actor.options(
            labels={
                "location": "dc-1",
                "version": "1.0"
            },
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression(
                    "location", ActorAffinityOperator.IN, ["dc-1"], True)
            ])).remote()
        actors.append(actor)
    for i in range(4):
        ray.get(actors[i].value.remote(), timeout=10)
    for i in range(1, 3):
        assert getActorNodeID(actors[0]) == getActorNodeID(actors[i])

    actor_5 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("location", ActorAffinityOperator.IN,
                                         ["dc-1"], True)
        ])).remote()
    ray.get(actor_5.value.remote(), timeout=10)
    assert getActorNodeID(actors[0]) != getActorNodeID(actor_5)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 4,
    }], indirect=True)
def test_soft_actor_affinity_NOT_IN(ray_start_cluster):
    actors = []
    for i in range(4):
        actor = Actor.options(
            labels={
                "actor_type": "spread",
            },
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression("actor_type",
                                             ActorAffinityOperator.NOT_IN,
                                             ["spread"], True)
            ])).remote()
        actors.append(actor)
    for i in range(4):
        ray.get(actors[i].value.remote(), timeout=10)
    node_id_set = set()
    for i in range(0, 4):
        node_id_set.add(getActorNodeID(actors[i]))
    assert len(node_id_set) == 4
    actor_1 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.NOT_IN, ["spread"], True)
        ])).remote()
    ray.get(actor_1.value.remote(), timeout=10)
    node_id_set.add(getActorNodeID(actor_1))
    assert len(node_id_set) == 4


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 3,
        "num_nodes": 2,
    }], indirect=True)
def test_actor_affinity_strict_case(ray_start_cluster):
    actor_1 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.IN, ["pack"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_1.value.remote(), timeout=3)

    actor_2 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.EXISTS, ["pack"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_2.value.remote(), timeout=3)

    actor_3 = Actor.options(
        labels={
            "actor_type": "pack"
        },
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.NOT_IN, ["pack"], False)
        ])).remote()
    ray.get(actor_3.value.remote(), timeout=10)

    ray.get(actor_1.value.remote(), timeout=10)
    ray.get(actor_2.value.remote(), timeout=10)
    assert getActorNodeID(actor_1) == getActorNodeID(actor_3)
    assert getActorNodeID(actor_2) == getActorNodeID(actor_3)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 3,
    }], indirect=True)
def test_actor_affinity_many_expression(ray_start_cluster):
    # prepare 3 actor spread 3 node
    actor_1 = Actor.options(labels={
        "actor_type": "spread",
        "version": "1"
    }).remote()
    ray.get(actor_1.value.remote(), timeout=10)
    actor_2 = Actor.options(
        labels={
            "actor_type": "spread",
            "version": "2",
            "special": "yes"
        },
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.DOES_NOT_EXIST, [], False)
        ])).remote()
    ray.get(actor_2.value.remote(), timeout=10)
    actor_3 = Actor.options(
        labels={
            "actor_type": "pack",
            "version": "3"
        },
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.DOES_NOT_EXIST, [], False)
        ])).remote()
    ray.get(actor_3.value.remote(), timeout=10)
    assert getActorNodeID(actor_1) != getActorNodeID(actor_2)
    assert getActorNodeID(actor_2) != getActorNodeID(actor_3)
    assert getActorNodeID(actor_1) != getActorNodeID(actor_3)

    # affinity actor_2
    actor_4 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.NOT_IN, ["pack"], False),
            ActorAffinityMatchExpression("version", ActorAffinityOperator.IN,
                                         ["2", "3"], False),
            ActorAffinityMatchExpression(
                "special", ActorAffinityOperator.EXISTS, [], False),
        ])).remote()
    ray.get(actor_4.value.remote(), timeout=10)
    assert getActorNodeID(actor_4) == getActorNodeID(actor_2)

    actor_5 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.IN, ["pack"], False),
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.NOT_IN, ["1"], False),
            ActorAffinityMatchExpression(
                "special", ActorAffinityOperator.EXISTS, [], True),
        ])).remote()
    ray.get(actor_5.value.remote(), timeout=10)
    assert getActorNodeID(actor_5) == getActorNodeID(actor_3)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 4,
    }], indirect=True)
def test_actor_affinity_when_actor_killed(ray_start_cluster):
    actor_1 = Actor.options(labels={"type": "pack"}).remote()
    ray.get(actor_1.value.remote(), timeout=10)
    actor_2 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("type", ActorAffinityOperator.IN,
                                         ["pack"], False)
        ])).remote()
    ray.get(actor_2.value.remote(), timeout=10)
    assert getActorNodeID(actor_1) == getActorNodeID(actor_2)
    ray.kill(actor_1)
    actor_3 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("type", ActorAffinityOperator.IN,
                                         ["pack"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_3.value.remote(), timeout=3)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 2,
    }], indirect=True)
def test_invalid_label_key(ray_start_cluster):
    with pytest.raises(ray.exceptions.RaySystemError):
        actor_1 = Actor.options(labels={"actor_id": "new actor id"}).remote()
    actor_1 = Actor.options().remote()
    ray.get(actor_1.value.remote(), timeout=10)
    actor_2 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("actor_id", ActorAffinityOperator.IN,
                                         [actor_1._actor_id.hex()], False)
        ])).remote()
    ray.get(actor_2.value.remote(), timeout=10)
    assert getActorNodeID(actor_1) == getActorNodeID(actor_2)


def test_namespace_isolation(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=3)

    namespace_1 = "namespace_1"
    proc = create_other_job_actor(cluster.address, namespace_1)
    assert proc.poll() is None
    ray.init(address=cluster.address, namespace=namespace_1)

    actor_2 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.IN, ["pack"], False)
        ])).remote()
    ray.get(actor_2.value.remote(), timeout=10)

    actor_3 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.EXISTS, [], False)
        ])).remote()
    ray.get(actor_3.value.remote(), timeout=10)
    ray.shutdown()

    ray.init(address=cluster.address)
    actor_1 = ray.get_actor("Pinger", namespace_1)
    assert ray.get(actor_1.ping.remote(), timeout=10)
    actor_2 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.IN, ["pack"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_2.value.remote(), timeout=3)

    actor_3 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression(
                "actor_type", ActorAffinityOperator.EXISTS, [], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_3.value.remote(), timeout=3)
    ray.shutdown()
    proc.kill()


def create_other_job_actor(address, namespace):
    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray
import time
ray.init(address="{}", namespace="{}")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(
    name="Pinger", lifetime="detached", labels={}).remote()
ray.get(actor.ping.remote())
while(True):
    time.sleep(3)
    """

    # Start a detached actor in a different namespace.
    return run_string_as_driver_nonblocking(
        driver_template.format(address, namespace,
                               "{\"actor_type\": \"pack\"}"))


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 6,
        "num_nodes": 2,
    }], indirect=True)
def test_pending_actors(ray_start_cluster):
    actor_1 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("location", ActorAffinityOperator.IN,
                                         ["dc-1"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_1.value.remote(), timeout=3)

    actor_2 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("location", ActorAffinityOperator.IN,
                                         ["dc-2"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_2.value.remote(), timeout=3)

    actor_3 = Actor.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("location", ActorAffinityOperator.IN,
                                         ["dc-3"], False)
        ])).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_3.value.remote(), timeout=3)

    actor_1_3 = Actor.options(labels={"location": "dc-3"}).remote()
    ray.get(actor_1_3.value.remote(), timeout=3)
    ray.get(actor_3.value.remote(), timeout=3)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_1.value.remote(), timeout=2)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_2.value.remote(), timeout=2)

    actor_1_2 = Actor.options(labels={"location": "dc-2"}).remote()
    ray.get(actor_1_2.value.remote(), timeout=3)
    ray.get(actor_2.value.remote(), timeout=3)

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_1.value.remote(), timeout=2)

    actor_1_1 = Actor.options(labels={"location": "dc-1"}).remote()
    ray.get(actor_1_1.value.remote(), timeout=3)
    ray.get(actor_1.value.remote(), timeout=3)


@pytest.mark.parametrize("connect_to_client", [True, False])
def test_actor_affinity_exceptions(ray_start_cluster, connect_to_client):
    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=3)
    ray.init(address=cluster.address)

    @ray.remote
    def normal_task():
        return "OK"

    with connect_to_client_or_not(connect_to_client):
        # test empty match expression
        with pytest.raises(ray.exceptions.RaySystemError):
            Actor.options(
                scheduling_strategy=ActorAffinitySchedulingStrategy(
                    [])).remote()

        # normal task don't support actor affinity schedduling strategy
        with pytest.raises(ValueError):
            normal_task.options(
                scheduling_strategy=ActorAffinitySchedulingStrategy([
                    ActorAffinityMatchExpression(
                        "location", ActorAffinityOperator.IN, ["dc-1"], False)
                ])).remote()


def getActorNodeID(actor):
    return ray.state.actors().get(actor._actor_id.hex())["Address"]["NodeID"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
