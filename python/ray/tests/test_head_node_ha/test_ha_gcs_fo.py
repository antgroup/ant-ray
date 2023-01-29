import ray
import ray.ray_constants as ray_constants
import time


@ray.remote
class Counter:
    def __init__(self):
        self.no = 0

    def get_inc(self):
        self.no += 1
        return self.no


@ray.remote
def task():
    return True


def test_raylet_discovery_gcs():
    ray.init()
    redis_client = ray.worker.global_worker.redis_client
    counter = Counter.remote()
    assert ray.get(counter.get_inc.remote()) == 1
    gcs_server = redis_client.get(ray_constants.GCS_ADDRESS_KEY)
    redis_client.set(ray_constants.GCS_ADDRESS_KEY, "127.0.0.1:0")
    time.sleep(4)
    redis_client.set(ray_constants.GCS_ADDRESS_KEY, gcs_server)
    for i in range(2, 10):
        assert ray.get(task.remote())
        assert ray.get(counter.get_inc.remote()) == i
        time.sleep(0.2)


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
