import pytest
import sys
import time
import ray

import ray.cluster_utils
from ray.test_utils import (
    get_other_nodes, )

MB = 1024 * 1024


@pytest.mark.parametrize("repeat", list(range(3)))
def test_placement_group_failover(ray_start_cluster, repeat):
    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 6
    nodes = []
    for _ in range(num_nodes):
        nodes.append(cluster.add_node(num_cpus=1))
    ray.init(address=cluster.address)

    bundles = [{"CPU": 1, "memory": 100 * MB} for _ in range(num_nodes)]
    placement_group = ray.util.placement_group(
        name="name", strategy="STRICT_SPREAD", bundles=bundles)
    assert placement_group.wait(10000)
    other_nodes = get_other_nodes(cluster, exclude_head=True)
    other_nodes_num = len(other_nodes)
    for i in range(other_nodes_num):
        cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    for node in other_nodes:
        cluster.remove_node(node)
    time.sleep(1)

    for i in range(num_nodes):
        actor = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=i).remote()
        object_ref = actor.value.remote()
        ray.get(object_ref, timeout=5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
