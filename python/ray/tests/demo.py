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


@ray.remote
class Worker1:
    def run(self):
        obj = slow_hi.remote()
        target_actor = TargetActor._remote(lifetime="detached", name="target_actor", args=[obj])
        return "running"

w1 = Worker1._remote(lifetime="detached", name="w1")
print(ray.get(w1.run.remote()))
ray.kill(w1, no_restart=True)


# try to use `target_actor`
while True:
    try:
        target_handle = ray.get_actor("target_actor")
        break
    except ValueError:
        continue

print(ray.get(target_handle.say_hi.remote()))

