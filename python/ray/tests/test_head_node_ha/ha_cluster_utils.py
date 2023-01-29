import time
import os
import ray._private.services
from ray.resource_spec import ResourceSpec


# This class is used to test head node ha feature locally.
class HaCluster:
    _redis_password = "5241590000000000"
    _head_process_list = []

    def start_redis(self):
        resource = ResourceSpec().resolve(False)
        self._redis_address, self._redis_shards, self.redis_process = \
            ray._private.services.start_redis(
              "127.0.0.1", [(None, None), (None, None)],
              resource,
              num_redis_shards=1,
              password=self._redis_password,
              fate_share=True)
        print("redis address:" + self._redis_address)
        redis_ip_address, redis_port = self._redis_address.split(":")
        ray._private.services.wait_for_redis_to_start(
            redis_ip_address, redis_port, self._redis_password)
        print("Success to start redis.")

    def start_two_head(self):
        self.start_head_process()
        time.sleep(2)
        self.start_head_process()
        while True:
            dead_process = []
            for process in self._head_process_list:
                if not (process.process.poll() is None):
                    print("A head node deaded. pid:" +
                          str(process.process.pid))
                    dead_process.append(process)
                    self._head_process_list.remove(process)

            for i in range(len(dead_process)):
                self.start_head_process()

            time.sleep(1)

    def start_head_process(self):
        print("Start a head node...")
        command = [
            "ray", "start", "--head", "--as-daemon",
            "--external-redis-addresses=" + self._redis_address,
            "--redis-password=" + self._redis_password
        ]
        os.environ["RAY_ENABLE_HEAD_HA"] = "true"
        process_info = ray._private.services.start_ray_process(
            command, "HEAD_CLI_PROCESSS", True)
        self._head_process_list.append(process_info)


if __name__ == "__main__":
    ha_cluster = HaCluster()
    ha_cluster.start_redis()
    ha_cluster.start_two_head()
