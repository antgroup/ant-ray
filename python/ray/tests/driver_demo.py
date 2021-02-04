import ray
import time

ray.init(address="auto", _redis_password='5241590000000000')

@ray.remote
def slow_hi():
    time.sleep(5)
    return "hihihi"


@ray.remote
class TargetActor:
    def __init__(self, obj):
        self.str = obj

    def say_hi(self):
        return self.str


obj = slow_hi.remote()
target_actor = TargetActor._remote(lifetime="detached", name="target_actor", args=[obj])

time.sleep(10)
ray.shutdown()

