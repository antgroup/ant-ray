import os
import sys
import logging
import requests
import time
import traceback
import redis
import tempfile
import zipfile
import shutil

from ray import ray_constants
import ray
import pytest
from pathlib import Path
from ray.ray_constants import (
    gcs_task_scheduling_enabled, )
from ray.new_dashboard.modules.actor import actor_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
    kill_actor_and_wait_for_failure,
)

logger = logging.getLogger(__name__)


def test_actor_groups(disable_aiohttp_cache, fast_organize_data,
                      fast_reporter_update, fast_update_nodes,
                      ray_start_with_dashboard):
    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    foo_actors = [Foo.remote(4), Foo.remote(5)]
    infeasible_actor = InfeasibleActor.remote()  # noqa
    results = [actor.do_task.remote() for actor in foo_actors]  # noqa
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    timeout_seconds = 15
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/logical/actor_groups")
            response.raise_for_status()
            actor_groups_resp = response.json()
            assert actor_groups_resp["result"] is True, actor_groups_resp[
                "msg"]
            actor_groups = actor_groups_resp["data"]["actorGroups"]
            assert "Foo" in actor_groups
            summary = actor_groups["Foo"]["summary"]
            # 2 __init__ tasks and 2 do_task tasks
            # assert summary["numExecutedTasks"] == 4
            assert summary["stateToCount"]["ALIVE"] == 2

            entries = actor_groups["Foo"]["entries"]
            foo_entry = entries[0]
            assert type(foo_entry["gpus"]) is list
            assert "timestamp" in foo_entry
            assert "actorConstructor" in foo_entry
            assert "actorClass" in foo_entry
            assert "actorId" in foo_entry
            assert "ipAddress" in foo_entry
            assert len(entries) == 2
            assert "InfeasibleActor" in actor_groups

            entries = actor_groups["InfeasibleActor"]["entries"]
            assert "requiredResources" in entries[0]
            assert "GPU" in entries[0]["requiredResources"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_actors(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    foo_actors = [Foo.remote(4), Foo.remote(5)]
    infeasible_actor = InfeasibleActor.remote()  # noqa
    results = [actor.do_task.remote() for actor in foo_actors]  # noqa
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            resp = requests.get(f"{webui_url}/logical/actors")
            resp_json = resp.json()
            resp_data = resp_json["data"]
            actors = resp_data["actors"]
            assert len(actors) == 3
            one_entry = list(actors.values())[0]
            assert "jobId" in one_entry
            # TO_BE_SOLVED: ActorTableData proto message missing task_spec
            # field.
            # assert "taskSpec" in one_entry
            # assert "functionDescriptor" in one_entry["taskSpec"]
            # assert type(one_entry["taskSpec"]["functionDescriptor"]) is dict
            assert "address" in one_entry
            assert type(one_entry["address"]) is dict
            assert "state" in one_entry
            assert "name" in one_entry
            assert "numRestarts" in one_entry
            assert "pid" in one_entry
            all_pids = {entry["pid"] for entry in actors.values()}
            assert 0 in all_pids  # The infeasible actor
            assert len(all_pids) > 1
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_kill_actor(disable_aiohttp_cache, fast_organize_data,
                    fast_reporter_update, fast_update_nodes,
                    ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray.worker.show_in_dashboard("test")
            return os.getpid()

    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    def actor_killed(pid):
        """Check For the existence of a unix pid."""
        try:
            os.kill(pid, 0)
        except OSError:
            return True
        else:
            return False

    def get_actor():
        resp = requests.get(f"{webui_url}/logical/actor_groups")
        resp.raise_for_status()
        actor_groups_resp = resp.json()
        assert actor_groups_resp["result"] is True, actor_groups_resp["msg"]
        actor_groups = actor_groups_resp["data"]["actorGroups"]
        actor = actor_groups["Actor"]["entries"][0]
        return actor

    def kill_actor_using_dashboard(actor):
        resp = requests.get(
            webui_url + "/logical/kill_actor",
            params={
                "actorId": actor["actorId"],
                "ipAddress": actor["ipAddress"],
                "port": actor["port"]
            })
        resp.raise_for_status()
        resp_json = resp.json()
        assert resp_json["result"] is True, "msg" in resp_json

    start = time.time()
    last_exc = None
    while time.time() - start <= 10:
        try:
            actor = get_actor()
            kill_actor_using_dashboard(actor)
            last_exc = None
            break
        except (KeyError, AssertionError) as e:
            last_exc = e
            time.sleep(.1)
    assert last_exc is None


@pytest.mark.skip(reason="This case does not work with GCS task scheduling.")
def test_actor_pubsub(disable_aiohttp_cache, ray_start_with_dashboard):
    timeout = 5
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    address_info = ray_start_with_dashboard
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    client = redis.StrictRedis(
        host=address[0],
        port=int(address[1]),
        password=ray_constants.REDIS_DEFAULT_PASSWORD)

    p = client.pubsub(ignore_subscribe_messages=True)
    p.psubscribe(ray.gcs_utils.RAY_ACTOR_PUBSUB_PATTERN)

    @ray.remote
    class DummyActor:
        def __init__(self):
            pass

    # Create a dummy actor.
    a = DummyActor.remote()

    def handle_pub_messages(client, msgs, timeout, expect_num):
        start_time = time.time()
        while time.time() - start_time < timeout and len(msgs) < expect_num:
            msg = client.get_message()
            if msg is None:
                time.sleep(0.01)
                continue
            pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(msg["data"])
            actor_data = ray.gcs_utils.ActorTableData.FromString(
                pubsub_msg.data)
            msgs.append(actor_data)

    msgs = []
    handle_pub_messages(p, msgs, timeout, 2)

    # Assert we received published actor messages with state
    # DEPENDENCIES_UNREADY and ALIVE.
    assert len(msgs) == 2

    # Kill actor.
    ray.kill(a)
    handle_pub_messages(p, msgs, timeout, 3)

    # Assert we received published actor messages with state DEAD.
    assert len(msgs) == 3

    def actor_table_data_to_dict(message):
        return dashboard_utils.message_to_dict(
            message, {
                "actorId", "parentId", "jobId", "workerId", "rayletId",
                "actorCreationDummyObjectId", "callerId", "taskId",
                "parentTaskId", "sourceActorId", "placementGroupId"
            },
            including_default_value_fields=False)

    # TODO(WangTao) we removed task spec from ActorTableData internally.
    # Won't check "taskSpec" here. Wait to sync to community.
    non_state_keys = ("actorId", "jobId")
    for msg in msgs:
        actor_data_dict = actor_table_data_to_dict(msg)
        # DEPENDENCIES_UNREADY is 0, which would not be keeped in dict. We
        # need check its original value.
        if msg.state == 0:
            assert len(actor_data_dict) > 5
            for k in non_state_keys:
                assert k in actor_data_dict
        # For status that is not DEPENDENCIES_UNREADY, only states fields will
        # be published.
        elif actor_data_dict["state"] in ("ALIVE", "DEAD"):
            assert actor_data_dict.keys() == {
                "state", "address", "timestamp", "pid"
            }
        elif actor_data_dict["state"] == "PENDING_CREATION":
            assert actor_data_dict.keys() == {"state", "address", "timestamp"}
        else:
            raise Exception("Unknown state: {}".format(
                actor_data_dict["state"]))


def test_nil_node(enable_test_module, disable_aiohttp_cache,
                  ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    infeasible_actor = InfeasibleActor.remote()  # noqa

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            resp = requests.get(f"{webui_url}/logical/actors")
            resp_json = resp.json()
            resp_data = resp_json["data"]
            actors = resp_data["actors"]
            assert len(actors) == 1
            response = requests.get(webui_url + "/test/dump?key=node_actors")
            response.raise_for_status()
            result = response.json()
            assert actor_consts.NIL_NODE_ID in result["data"]["nodeActors"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_actors_info(disable_aiohttp_cache, fast_reporter_update,
                     fast_pop_dead_actor, ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = set(ray.get(actor_pids))

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    def check_job_actors_before_kill():
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == len(actors):
            return True
        else:
            return False

    wait_for_condition(check_job_actors_before_kill, timeout=10)

    # Kill actor
    ray.kill(actors[0])

    data = {}

    def check_job_actors_after_kill():
        nonlocal data
        time.sleep(1)
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == len(actors) - 1:
            return True
        else:
            return False

    wait_for_condition(check_job_actors_after_kill, timeout=10)

    assert len(data["data"]["actors"]) == len(actors) - 1
    assert actors[0]._ray_actor_id.hex() not in data["data"]["actors"]
    assert actors[1]._ray_actor_id.hex() in data["data"]["actors"]


def test_pop_dead_actor_interval(disable_aiohttp_cache, fast_reporter_update,
                                 fast_pop_dead_actor_with_five_second_interval,
                                 ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = set(ray.get(actor_pids))

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    def check_job_actors_before_kill():
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == len(actors):
            return True
        else:
            return False

    wait_for_condition(check_job_actors_before_kill, timeout=10)

    def check_job_actors_after_kill():
        time.sleep(1)
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == 1:
            return True
        else:
            return False

    # Kill actor
    ray.kill(actors[0])

    # It will pop dead actor after 5 seconds
    # with pytest.raises(RuntimeError) as error:
    # wait_for_condition(check_job_actors_after_kill, timeout=4)
    # assert error.value.args[
    # 0] == "The condition wasn't met before the timeout expired."
    # time.sleep(1)

    # Just for trigger actors status update
    _trigger_actor = Actor.remote()

    response = requests.get(webui_url + "/actors?view=summary")
    response.raise_for_status()
    data = response.json()
    assert 2 == len(data["data"]["actors"])
    assert actors[0]._ray_actor_id.hex() not in data["data"]["actors"]
    assert actors[1]._ray_actor_id.hex() in data["data"]["actors"]
    assert _trigger_actor._ray_actor_id.hex() in data["data"]["actors"]


def test_dead_actor_limit(disable_aiohttp_cache, fast_reporter_update,
                          zero_dead_actors, ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = set(ray.get(actor_pids))

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    def check_job_actors_before_kill():
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == len(actors):
            return True
        else:
            return False

    wait_for_condition(check_job_actors_before_kill, timeout=10)

    def check_job_actors_after_kill():
        time.sleep(1)
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == 1:
            return True
        else:
            return False

    # Kill actor
    ray.kill(actors[0])

    wait_for_condition(check_job_actors_after_kill, timeout=4)
    time.sleep(1)

    # Just for trigger actors status update
    _trigger_actor = Actor.remote()

    response = requests.get(webui_url + "/actors?view=summary")
    response.raise_for_status()
    data = response.json()
    assert 2 == len(data["data"]["actors"])
    assert actors[0]._ray_actor_id.hex() not in data["data"]["actors"]
    assert actors[1]._ray_actor_id.hex() in data["data"]["actors"]
    assert _trigger_actor._ray_actor_id.hex() in data["data"]["actors"]


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "ignore_reinit_error": True
    }], indirect=True)
def test_not_pop_dead_actor_with_retry(
        disable_aiohttp_cache, fast_reporter_update, fast_pop_dead_actor,
        ray_start_with_dashboard):
    @ray.remote(max_restarts=1)
    class ActorWithRetry:
        def kill_self(self):
            os._exit(0)

    actors = [ActorWithRetry.remote(), ActorWithRetry.remote()]

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    def check_job_actors_before_kill():
        response = requests.get(webui_url + "/actors?view=summary")
        response.raise_for_status()
        data = response.json()
        if len(data["data"]["actors"]) == len(actors):
            return True
        else:
            return False

    wait_for_condition(check_job_actors_before_kill, timeout=10)

    with pytest.raises(ray.exceptions.RayActorError) as _:
        print(ray.get(actors[0].kill_self.remote()))
    response = requests.get(webui_url + "/actors?view=summary")
    response.raise_for_status()
    data = response.json()
    assert 2 == len(data["data"]["actors"])

    # Dashboard will pop actors[0] when numRestarts >= maxRestarts
    # and state is `Dead`
    with pytest.raises(ray.exceptions.RayActorError) as _:
        print(ray.get(actors[0].kill_self.remote()))
    response = requests.get(webui_url + "/actors?view=summary")
    response.raise_for_status()
    data = response.json()
    assert 1 == len(data["data"]["actors"])


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
def test_gcs_resource_leak_when_kill_actor(
        disable_aiohttp_cache, fast_reporter_update, fast_pop_dead_actor,
        ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    from ray.workers import default_worker
    default_worker_bak_file = default_worker.__file__ + ".bak"
    try:
        os.rename(default_worker.__file__, default_worker_bak_file)

        @ray.remote(num_cpus=1)
        class Actor:
            def ping(self):
                return "pong"

        actor = Actor.remote()

        def ensure_actor_in_state_of_pending_creation(actor):
            session_dir = ray.worker.global_worker.node.address_info[
                "session_dir"]
            session_path = Path(session_dir)
            log_dir_path = session_path / "logs"
            paths = list(log_dir_path.iterdir())
            for path in paths:
                path_str = str(path)
                if "python-worker-" in path_str and path_str.split(
                        ".")[1] == "err":
                    with open(path_str, "r") as file_to_read:
                        line = file_to_read.readline()
                        if default_worker.__file__ in line:
                            return True
            return False

        # can't open file '/xxx/.../default_worker.py'
        wait_for_condition(
            lambda: ensure_actor_in_state_of_pending_creation(actor))
        resp = requests.get(webui_url + "/scheduler/cluster_resources").json()
        node_resource = resp["data"]["nodeResources"][0]
        total_cpu = node_resource["totalResources"]["CPU"]
        avail_cpu = node_resource["availableResources"]["CPU"]
        assert total_cpu != avail_cpu

        kill_actor_and_wait_for_failure(actor)
        resp = requests.get(webui_url + "/scheduler/cluster_resources").json()
        node_resource = resp["data"]["nodeResources"][0]
        total_cpu = node_resource["totalResources"]["CPU"]
        avail_cpu = node_resource["availableResources"]["CPU"]
        assert total_cpu == avail_cpu
    finally:
        os.rename(default_worker_bak_file, default_worker.__file__)


def _submit_python_job(webui_url, job_code: str,
                       long_running: bool = False) -> str:
    RAY_DEFAULT_NODEGROUP = "NAMESPACE_LABEL_RAY_DEFAULT"
    TEST_PYTHON_JOB = {
        "name": "Test job",
        "owner": "abc.xyz",
        "namespaceId": RAY_DEFAULT_NODEGROUP,
        "language": "PYTHON",
        "longRunning": long_running,
        "driverArgs": []
    }
    JOB_ROOT_DIR = "/tmp/ray/job"
    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    def _gen_job_zip(job_code, driver_entry):
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
            with zipfile.ZipFile(f, mode="w") as zip_f:
                with zip_f.open(f"{driver_entry}.py", "w") as driver:
                    driver.write(job_code.encode())
            return f.name

    def _gen_url(weburl, path):
        return f"{weburl}/test/file?path={path}"

    def _check_running():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "RUNNING", job_info["failErrorMessage"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    job_id = None
    try:
        driver_entry = "simple_job"
        path = _gen_job_zip(job_code, driver_entry)
        url = _gen_url(webui_url, path)
        job = TEST_PYTHON_JOB
        job["url"] = url
        job["driverEntry"] = driver_entry
        resp = requests.post(webui_url + "/jobs", json=job)
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_id = result["data"]["jobId"]
        wait_for_condition(_check_running, timeout=120)
    except Exception:
        logger.exception("Error submitting job")
    finally:
        os.remove(path)
    assert job_id
    return job_id


def test_actors_death_cause_normal(disable_aiohttp_cache, enable_test_module,
                                   ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    foo_actor = Foo.remote(4)
    ray.get(foo_actor.do_task.remote())
    # Remove all references of actor to make it exit normally
    del foo_actor
    time.sleep(100)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    def _check_actor_died_normally():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["deathCause"]["keyword"] == "NORMAL"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")

    wait_for_condition(_check_actor_died_normally, 10)


def test_actors_death_cause_worker_died(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    foo_actor = Foo.remote(4)
    ray.get(foo_actor.do_task.remote())
    time.sleep(100)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)
    worker_pid = 0

    def _get_actor_worker_pid():
        try:
            resp = requests.get(f"{webui_url}/logical/actors")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["actors"]
            actor_id, actor = actors.popitem()
            nonlocal worker_pid
            worker_pid = actor["pid"]
            assert worker_pid != 0
            return True
        except Exception:
            logger.exception("Check failed:")
            return False

    wait_for_condition(_get_actor_worker_pid, 10)
    # Kill the worker process
    os.kill(worker_pid, 9)

    def _check_actor_died_because_of_worker_died():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["deathCause"]["keyword"] == "WORKER DIED"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")
            return False

    wait_for_condition(_check_actor_died_because_of_worker_died, 10)


def test_actors_death_cause_ray_kill(disable_aiohttp_cache, enable_test_module,
                                     ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    foo_actor = Foo.remote(4)
    ray.get(foo_actor.do_task.remote())
    # Use ray.kill to kill the actor
    ray.kill(foo_actor)
    time.sleep(100)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    def _check_actor_died_on_ray_kill():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["deathCause"]["keyword"] == "RAY_KILLED"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")

    wait_for_condition(_check_actor_died_on_ray_kill, 10)


def test_actors_death_cause_owner_died(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Bar:
    def __init__(self):
        self.num = 1

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num
        self.bar = Bar.remote()

    def do_task(self):
        return self.num

def main():
    foo_actor = Foo.remote(4)
    ray.get(foo_actor.do_task.remote())
    # Kil Bar actor's owner
    ray.kill(foo_actor)
    time.sleep(100)

if __name__ == "__main__":
    ray.init()
    main()
"""
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    def _check_actor_died_on_owner_died():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["deathCause"]["keyword"] == "OWNER DIED"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")

    wait_for_condition(_check_actor_died_on_owner_died, 10)


def test_actors_death_cause_node_died(disable_aiohttp_cache,
                                      enable_test_module,
                                      ray_start_cluster_head_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    actor1 = Foo.options(resources={"worker1": 1}, name="actor1").remote(4)
    actor2 = Foo.options(resources={"worker2": 1}, name="actor2").remote(4)
    ray.get([actor1.do_task.remote(), actor2.do_task.remote()])
    time.sleep(100)

if __name__ == "__main__":
    ray.init()
    main()
"""
    # Build a cluster of 3 nodes
    cluster = ray_start_cluster_head_with_dashboard
    node_1 = cluster.add_node(resources={"worker1": 1})
    node_2 = cluster.add_node(resources={"worker2": 1})
    webui_url = cluster.head_node.webui_url
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    # Get node name to raylet id map
    def _get_node_name_to_raylet_id_map():
        result = requests.get(webui_url + "/nodes?view=details").json()
        nodes = result["data"]["clients"]
        node_name_to_raylet_id = {}
        for node in nodes:
            if "worker1" in node["raylet"]["resourcesTotal"]:
                node_name_to_raylet_id["worker1"] = node["raylet"]["nodeId"]
            if "worker2" in node["raylet"]["resourcesTotal"]:
                node_name_to_raylet_id["worker2"] = node["raylet"]["nodeId"]
        return node_name_to_raylet_id

    node_name_to_raylet_id = _get_node_name_to_raylet_id_map()
    assert len(node_name_to_raylet_id) == 2

    # Submit job to the cluster
    job_id = _submit_python_job(webui_url, job_code)

    def _check_all_actors_alive():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            assert len(actors) == 2
            for actor in actors.values():
                assert actor["state"] == "ALIVE"
            return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_all_actors_alive, 60)

    # Kill the worker node where the driver is not on
    def _get_driver_raylet_id():
        resp = requests.get(f"{webui_url}/jobs/{job_id}")
        resp.raise_for_status()
        result = resp.json()
        return result["data"]["detail"]["jobInfo"]["rayletId"]

    driver_raylet_id = _get_driver_raylet_id()
    if node_name_to_raylet_id["worker1"] != driver_raylet_id:
        cluster.remove_node(node_1)
        actor_died_on_node = "actor1"
    else:
        cluster.remove_node(node_2)
        actor_died_on_node = "actor2"

    # Check actor's death cause is NODE DIED
    def _check_actor_died_on_node_died():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            for actor in actors.values():
                if actor["name"] == actor_died_on_node:
                    assert actor["state"] == "DEAD"
                    assert actor["deathCause"]["keyword"] == "NODE DIED"
                    assert actor["deathCause"]["message"]
                    return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_actor_died_on_node_died, 20)


def test_actors_death_cause_job_died(disable_aiohttp_cache, enable_test_module,
                                     ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    actor1 = Foo.options(name="actor1", lifetime="detached").remote(4)
    ray.get(actor1.do_task.remote())
    time.sleep(60)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    def _check_all_actors_alive():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            assert len(actors) == 1
            for actor in actors.values():
                assert actor["state"] == "ALIVE"
            return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_all_actors_alive, 60)

    # Kill job
    requests.delete(f"{webui_url}/jobs/{job_id}")

    # Check actor's death cause is JOB DIED
    def _check_actor_died_on_job_died():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["state"] == "DEAD"
            assert actor["deathCause"]["keyword"] == "JOB DIED"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_actor_died_on_job_died, 20)


def test_actors_death_cause_creation_exception(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        # This should throw an exception when creating the actor
        self.num = num
        num / 0

    def do_task(self):
        return self.num

def main():
    actor1 = Foo.options(name="actor1").remote(4)
    try:
        ray.get(actor1.do_task.remote())
    except Exception:
        pass
    time.sleep(120)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    # Check actor's death cause is creation exception
    def _check_actor_died_on_creation_exception():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["state"] == "DEAD"
            assert actor["deathCause"]["keyword"] == "CREATION EXCEPTION"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_actor_died_on_creation_exception, 60)


def test_actors_death_cause_runtime_env(
        fastern_worker_register_failures, disable_aiohttp_cache,
        enable_test_module, ray_start_with_dashboard):
    job_code = """
import ray
import time

@ray.remote
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    actor1 = Foo.options(
        name="actor1",
        runtime_env = {"pip": ["http://localhost:8000/not_exist.zip"]}
        ).remote(4)
    ray.get(actor1.do_task.remote())
    time.sleep(120)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    def _check_actor_died_on_runtime_env():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["state"] == "DEAD"
            assert actor["deathCause"]["keyword"] == "RUNTIME ENV"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_actor_died_on_runtime_env, 60)


def test_actors_death_cause_bundle_removed(
        fastern_worker_register_failures, disable_aiohttp_cache,
        enable_test_module, disable_gcs_task_scheduling,
        ray_start_with_dashboard):
    job_code = """
import ray
import time
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group
)

@ray.remote(num_cpus=2)
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    bundle1 = {"CPU": 2.0}
    pg = placement_group([bundle1], strategy = "SPREAD")
    ray.get(pg.ready())
    actor0 = Foo.options(placement_group=pg,
                            placement_group_bundle_index=0,
                            name = "actor0").remote(4)
    # Actor 1 should be pending creation
    actor1 = Foo.options(placement_group=pg,
                            placement_group_bundle_index=0,
                            name = "actor1").remote(4)
    # Wait for actor0 to be scheduled
    time.sleep(3)
    # Remove bundle so actor1 will be canceled
    pg.remove_bundles([0])
    time.sleep(120)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    # Check actor's death cause is bundle removed
    def _check_actor_died_on_bundle_removed():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            assert len(actors) == 2
            for actor in actors.values():
                if actor["name"] == "actor1":
                    assert actor["state"] == "DEAD"
                    assert actor["deathCause"]["keyword"] == "BUNDLE REMOVED"
                    assert actor["deathCause"]["message"]
                    return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_actor_died_on_bundle_removed, 60)


def test_actors_death_cause_scheduling_failed(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard):
    job_code = """
import ray
import time
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Set an infeasible cpu request
@ray.remote(num_cpus=20)
class Foo:
    def __init__(self, num):
        self.num = num

    def do_task(self):
        return self.num

def main():
    actor1 = Foo.options(
        scheduling_strategy = NodeAffinitySchedulingStrategy(
            nodes = [ray.get_runtime_context().node_id],
            soft = False
        )
    ).remote(4)
    time.sleep(120)

if __name__ == "__main__":
    ray.init()
    main()
"""

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = _submit_python_job(webui_url, job_code)

    def _check_actor_died_on_scheduling_failed():
        try:
            resp = requests.get(f"{webui_url}/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            actors = result["data"]["detail"]["jobActors"]
            actor_id, actor = actors.popitem()
            assert actor["state"] == "DEAD"
            assert actor["deathCause"]["keyword"] == "SCHEDULING FAILED"
            assert actor["deathCause"]["message"]
            return True
        except Exception:
            logger.exception("Check failed:")
        return False

    wait_for_condition(_check_actor_died_on_scheduling_failed, 60)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
