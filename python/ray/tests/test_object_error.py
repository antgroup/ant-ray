import pytest
import ray

@pytest.mark.parametrize(
    "actor_resources",
    [
        dict(
            zip(["owner", "creator", "borrower"], [{
                f"node{i}": 1
            } for i in _])) for _ in [
                [1, 2, 3],  # None of them is on the same node.
            ]
    ])
def test_owner_assign_when_put(ray_start_cluster, actor_resources):
    cluster_node_config = [{
        "num_cpus": 1,
        "resources": {
            f"node{i+1}": 10
        }
    } for i in range(3)]
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    @ray.remote(resources=actor_resources["creator"], num_cpus=0)
    class Creator:
        def gen_object_ref(self, data="test"):
            a = ray.put(data)
            return [ray.put([a])]

    @ray.remote(resources=actor_resources["borrower"], num_cpus=0)
    class Borrower:
        def get_object(self, actor):
            refs = ray.get(actor.gen_object_ref.remote())
            print(ray.get(refs[0]), refs[0])
            del refs
            # from time import sleep
            # sleep(5)

    creator = Creator.remote()
    borrower = Borrower.remote()

    ray.get(borrower.get_object.remote(creator))

