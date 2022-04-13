import time
import os
import sys
import tempfile
import subprocess

import ray
from ray.job_config import JobConfig
import ray._private.gcs_utils as gcs_utils
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
    wait_for_num_actors,
)


###########################
"""
要实现的：
1. driver退出的rpc使用markdriver exited
2. gcs收到driver exited的时候检查是否有detached actors, 没有的话，则退出。
3. 是否要保证这个actors没有泄漏,例如刚收到driver exited,再收到detached actors。
4. kill job接口,mark failed接口。
5. 测试，要保证测到状态机的多条路径。
"""
###########################


# RUNNING -> DRIVER_EXITED -> FINISHED
def test_driver_exited_state():
    pass

def test_job_gc_with_detached_actor(call_ray_start):
    address = call_ray_start

    ray.init(address=address, namespace="test")
    driver = """
import ray

ray.init(address="{}", namespace="test")

@ray.remote
class Actor:
    def __init__(self):
        pass

    def value(self):
        return 1

_ = Actor.options(lifetime="detached", name="DetachedActor").remote()
# Make sure the actor is created before the driver exits.
ray.get(_.value.remote())
""".format(
        address
    )

    p = run_string_as_driver_nonblocking(driver)
    # Wait for actor to be created
    wait_for_num_actors(1, gcs_utils.ActorTableData.ALIVE)

    actor_table = ray.state.actors()
    assert len(actor_table) == 1

    job_table = ray.state.jobs()
    assert len(job_table) == 2  # dash

    # Kill the driver process.
    p.kill()
    p.wait()

    detached_actor = ray.get_actor("DetachedActor")
    assert ray.get(detached_actor.value.remote()) == 1



if __name__ == "__main__":
    import pytest

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
