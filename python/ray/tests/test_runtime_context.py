# This is the tests in out internal Ray only.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import ray
import os
import signal
import time


def test_was_current_actor_reconstructed():
    ray.init()

    @ray.remote(max_restarts=10)
    class A(object):
        def get_was_reconstructed(self):
            return ray._get_runtime_context().was_current_actor_reconstructed

        def get_pid(self):
            return os.getpid()

        # The following methods is to apply the checkpointable interface.
        def should_checkpoint(self, checkpoint_context):
            return False

        def save_checkpoint(self, actor_id, checkpoint_id):
            pass

        def load_checkpoint(self, actor_id, available_checkpoints):
            pass

        def checkpoint_expired(self, actor_id, checkpoint_id):
            pass

    a = A.remote()
    # `was_reconstructed` should be False when it's called in ctor.
    assert ray.get(a.get_was_reconstructed.remote()) is False
    # `was_reconstructed` should be False when it's called in a remote method
    # and the actor never fails.
    # assert ray.get(a.update_was_reconstructed.remote()) is False
    pid = ray.get(a.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    time.sleep(2)
    # These 2 methods should be return True because this actor failed and
    # restored.
    assert ray.get(a.get_was_reconstructed.remote()) is True
    # assert ray.get(a.update_was_reconstructed.remote()) is True
    ray.shutdown()


def test_get_context_dict(ray_start_regular):
    context_dict = ray.get_runtime_context().get()
    assert context_dict["node_id"] is not None
    assert context_dict["job_id"] is not None
    assert "actor_id" not in context_dict
    assert "task_id" not in context_dict

    @ray.remote
    class Actor:
        def check(self, node_id, job_id):
            context_dict = ray.get_runtime_context().get()
            assert context_dict["node_id"] == node_id
            assert context_dict["job_id"] == job_id
            assert context_dict["actor_id"] is not None
            assert context_dict["task_id"] is not None
            assert context_dict["actor_id"] != "not an ActorID"

    a = Actor.remote()
    ray.get(a.check.remote(context_dict["node_id"], context_dict["job_id"]))

    @ray.remote
    def task(node_id, job_id):
        context_dict = ray.get_runtime_context().get()
        assert context_dict["node_id"] == node_id
        assert context_dict["job_id"] == job_id
        assert context_dict["task_id"] is not None
        assert "actor_id" not in context_dict

    ray.get(task.remote(context_dict["node_id"], context_dict["job_id"]))
