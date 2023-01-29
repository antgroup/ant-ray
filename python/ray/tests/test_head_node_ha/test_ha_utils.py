import time
import ray
import ray.ray_constants as ray_constants
from ray.ha import is_service_available, waiting_for_server_stopped


def test_until_disconnect_with_address(shutdown_only):
    ray.init()
    redis_client = ray.worker.global_worker.redis_client
    gcs_address = str(
        redis_client.get(ray_constants.GCS_ADDRESS_KEY), encoding="utf-8")
    ip, port = gcs_address.split(":")
    assert is_service_available(ip, port)
    max_time = 3
    is_disconnect, user_time = waiting_for_server_stopped(
        gcs_address, max_time)
    assert not is_disconnect
    assert abs(user_time - max_time) < 0.5
    ray.shutdown()
    time.sleep(2)
    is_disconnect, user_time = waiting_for_server_stopped(
        gcs_address, max_time)
    assert is_disconnect
    assert user_time < 1
    assert not is_service_available(ip, port)


def test_is_service_available_exception():
    assert not is_service_available("aaaaa", 0)


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
