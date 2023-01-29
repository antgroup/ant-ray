import ray
import pytest
import sys
from ray.job_config import JobConfig


@pytest.mark.parametrize("node_number,global_owner_number", [(1, 1), (1, 3),
                                                             (3, 1), (3, 3)])
@pytest.mark.parametrize("ray_namespace", [None, "test"])
def test_global_owner_in_signal_node_with_one_global_owner(
        ray_start_cluster, node_number, global_owner_number, ray_namespace):
    cluster = ray_start_cluster
    for _ in range(node_number):
        cluster.add_node()
    ray.init(
        namespace=ray_namespace,
        job_config=JobConfig(global_owner_number=global_owner_number),
    )

    ray.state.state._check_connected()
    for index in range(global_owner_number):
        actor = ray.get_actor(f"__global_owner_{index}", "")
        ray.get(actor.warmup.remote())
        address = ray.gcs_utils.ActorTableData.FromString(
            ray.state.state.global_state_accessor.get_actor_info(
                actor._actor_id)).address
        assert len(address.worker_id) != 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
