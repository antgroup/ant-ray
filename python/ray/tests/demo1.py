import ray
import time

ray.init(address="auto", _redis_password='5241590000000000')

@ray.remote
class SlowActor:
    def __init__(self):
        time.sleep(100)

@ray.remote
class A:
    def __init__(self, handle):
        self.handle = handle


slow = SlowActor.options(name="slow").remote()
a = A.options(name="a").remote(slow)

time.sleep(100)

