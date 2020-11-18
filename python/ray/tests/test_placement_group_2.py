import pytest
import sys

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
from ray.test_utils import generate_system_config_map
import ray.cluster_utils


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_create_placement_group_after_gcs_server_restarts(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create placement group 1 successfully.
    placement_group1 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    ray.get(placement_group1.ready(), timeout=2)
    table = ray.util.placement_group_table(placement_group1)
    assert table["state"] == "CREATED"

    # Restart gcs server.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    # Create placement group 2 successfully.
    placement_group2 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    ray.get(placement_group2.ready(), timeout=2)
    table = ray.util.placement_group_table(placement_group2)
    assert table["state"] == "CREATED"

    # Create placement group 3.
    # Status is `PENDING` because the cluster resource is insufficient.
    placement_group3 = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(placement_group3.ready(), timeout=2)
    table = ray.util.placement_group_table(placement_group3)
    assert table["state"] == "PENDING"


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_create_actor_with_placement_group_after_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Create a placement group.
    placement_group = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])

    # Create an actor that occupies resources after gcs server restart.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()
    actor_2 = Increase.options(
        placement_group=placement_group,
        placement_group_bundle_index=1).remote()
    assert ray.get(actor_2.method.remote(1)) == 3


@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_system_config_map(
            num_heartbeats_timeout=20, ping_gcs_rpc_server_max_retries=60)
    ],
    indirect=True)
def test_create_placement_group_during_gcs_server_restart(
        ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=20)
    cluster.wait_for_nodes()

    # Create placement groups during gcs server restart.
    placement_groups = []
    for i in range(0, 100):
        placement_group = ray.util.placement_group([{
            "CPU": 0.1
        }, {
            "CPU": 0.1
        }])
        placement_groups.append(placement_group)

    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    for i in range(0, 10):
        ray.get(placement_groups[i].ready(), timeout=2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
