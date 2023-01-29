import os
import sys
import copy
import uuid
import logging
import requests
import tempfile
import zipfile
import hashlib
import shutil
import socket
import subprocess
import asyncio
import time
import psutil

import mock
import pytest
import ray
from ray.new_dashboard.modules.job import job_consts
from ray.new_dashboard.modules.job.job_agent import (
    JobInfo,
    JobAgent,
    PreparePythonEnviron,
)
from ray.new_dashboard.modules.job.virtualenv_cache import (
    VenvHash,
    VenvCache,
    VenvCacheSerializer,
    VenvCacheFileSerializer,
)
from ray.new_dashboard.modules.job.job_utils import JobData
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.new_dashboard.utils import LazySemaphore
from ray.ray_constants import (
    env_integer,
    from_memory_units,
)

logger = logging.getLogger(__name__)

DEFAULT_RAY_NAMESPCE = "default_ray_namespace"
MB = 1024 * 1024
RAY_DEFAULT_NODEGROUP = "NAMESPACE_LABEL_RAY_DEFAULT"
TEMP_DIR = "/tmp/ray"
JOB_ROOT_DIR = "/tmp/ray/job"
TEST_PYTHON_JOB = {
    "name": "Test job",
    "owner": "abc.xyz",
    "namespaceId": RAY_DEFAULT_NODEGROUP,
    "language": "PYTHON",
    "url": "http://xxx/yyy.zip",
    "driverEntry": "python_file_name_without_ext",
    "driverArgs": [],
    "customConfig": {
        "k1": "v1",
        "k2": "v2"
    },
    "jvmOptions": [],
    "dependencies": {
        "python": [
            "py-spy >= 0.2.0",
        ],
        "java": [{
            "name": "spark",
            "version": "2.1",
            "url": "<http>xxx/yyy.jar",
            "md5": "<md5 hex>"
        }]
    }
}

TEST_PYTHON_JOB_CODE = """
import os
import ray
import time


@ray.remote
class Actor:
    def __init__(self, index):
        self._index = index

    def foo(self, x):
        print("worker job dir {}".format(os.environ["RAY_JOB_DIR"]))
        print("worker cwd {}".format(os.getcwd()))
        return "Actor {}: {}".format(self._index, x)


def main():
    actors = []
    print("driver job dir {}".format(os.environ["RAY_JOB_DIR"]))
    print("driver cwd {}".format(os.getcwd()))
    for x in range(2):
        actors.append(Actor.remote(x))

    counter = 0
    while True:
        for a in actors:
            r = a.foo.remote(counter)
            print(ray.get(r))
            counter += 1
            time.sleep(1)


if __name__ == "__main__":
    ray.init()
    main()
"""

TEST_PYTHON_JOB_DUMMY_CODE = """
import os
import ray
import time

long_running = {long_running}

@ray.remote
class Actor:
    def __init__(self, index):
        self._index = index

    def foo(self, x):
        return "Actor " + str(self._index) + ": " + str(x)


def main():
    actors = []
    for x in range(2):
        actors.append(Actor.remote(x))

    counter = 0
    if long_running:
        ray.shutdown()
    while True:
        for a in actors:
            try:
                r = a.foo.remote(counter)
                print(ray.get(r))
            except Exception as ex:
                print("error:", ex)
            counter += 1
            time.sleep(1)


if __name__ == "__main__":
    ray.init()
    main()
"""


def rebuild_job_consts():
    """ Rebuild some variables of job_consts
    that cannot be used across Loop.

    """

    # This should be synchronized with the initialization of
    # job_consts.JOB_PREPARE_ENVIRONMENT_SEMAPHORE
    job_consts.JOB_PREPARE_ENVIRONMENT_SEMAPHORE = LazySemaphore(
        max(
            env_integer("JOB_ENVIRONMENT_PREPARATION_CONCURRENCY",
                        psutil.cpu_count() // 2), 1))


def _gen_job_zip(job_code, driver_entry):
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        with zipfile.ZipFile(f, mode="w") as zip_f:
            with zip_f.open(f"{driver_entry}.py", "w") as driver:
                driver.write(job_code.encode())
        return f.name


def _gen_md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def _gen_url(weburl, path):
    return f"{weburl}/test/file?path={path}"


def _get_python_job(web_url,
                    java_dependency_url=None,
                    java_dependency_md5=None,
                    downloader=None,
                    job_code=TEST_PYTHON_JOB_CODE,
                    *,
                    long_running=False,
                    archive_dependency_urls=None):
    driver_entry = "simple_job"
    path = _gen_job_zip(job_code, driver_entry)
    url = _gen_url(web_url, path)
    job = copy.deepcopy(TEST_PYTHON_JOB)
    job["url"] = url
    job["driverEntry"] = driver_entry
    if java_dependency_url:
        job["dependencies"]["java"][0]["url"] = java_dependency_url
    if java_dependency_md5:
        job["dependencies"]["java"][0]["md5"] = java_dependency_md5
    if downloader:
        job["downloader"] = downloader
    if long_running:
        job["longRunning"] = True
    if archive_dependency_urls:
        job["dependencies"]["archive"] = archive_dependency_urls
    return job


def _gen_job_code(memory_mb, actor_name=None):
    return """
import os
import ray
import time
@ray.remote
class SimpleActor:
    def echo(self, x):
        return x
@ray.remote
class Actor:
    def foo(self, x):
        return x
    def create_simple_actor(self, memory):
        return SimpleActor.options(memory=memory).remote()
def main():
    actor = Actor.options(memory={}, name={}).remote()
    counter = 0
    while True:
        r = actor.foo.remote(counter)
        print(ray.get(r))
        counter += 1
        time.sleep(1)
""".format(memory_mb * MB, repr(actor_name))


def _get_named_actor_with_retry(actor_name,
                                max_retries=100,
                                retry_interval_ms=500):
    actor = None
    while max_retries > 0:
        try:
            actor = ray.get_actor(
                name=actor_name, namespace=DEFAULT_RAY_NAMESPCE)
            break
        except Exception as ex:
            logger.info(ex)
            max_retries = max_retries - 1
            time.sleep(retry_interval_ms / 1000)
    return actor


def _query_job_memory_requirements(job_id_hex, webui_url, timeout_seconds=10):
    import time
    total_memory_mb = 0
    start_time = time.time()
    while True:
        time.sleep(0.5)
        try:
            response = requests.get(webui_url + "/test/dump")
            response.raise_for_status()
            try:
                dump_info = response.json()
                job_info = dump_info["data"]["jobs"][job_id_hex]
                total_memory_units = int(
                    job_info["config"]["totalMemoryUnits"])
                total_memory_mb = from_memory_units(total_memory_units)
                max_total_memory_units = int(
                    job_info["config"]["maxTotalMemoryUnits"])
                max_total_memory_mb = from_memory_units(max_total_memory_units)
                break
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")
    return total_memory_mb, max_total_memory_mb


def create_namespace(webui_url,
                     namespace_id,
                     node_shape_and_count_list=None,
                     enable_subnamespace_isolation=False,
                     job_resource_requirements_max_min_ratio_limit=10):
    try:
        limit = job_resource_requirements_max_min_ratio_limit
        if node_shape_and_count_list is None:
            node_shape_and_count_list = [{"nodeCount": 1, "group": "4c8g"}]
        resp = requests.post(
            webui_url + "/namespaces_v2",
            json={
                "namespaceId": namespace_id,
                "enableSubNamespaceIsolation": enable_subnamespace_isolation,
                "nodeShapeAndCountList": node_shape_and_count_list,
                "scheduleOptions": {
                    "overcommitRatio": 1.0,
                    "jobResourceRequirementsMaxMinRatioLimit": limit
                },
                "enableJobQuota": True
            })
        resp.raise_for_status()
        result = resp.json()
        print(result)
        assert result["result"] is True, resp.text

        resp = requests.get(webui_url + "/namespaces")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text

        hostname = socket.gethostname()
        for nid in [namespace_id, RAY_DEFAULT_NODEGROUP]:
            for namespaces in result["data"]["namespaces"]:
                if namespaces["namespaceId"] == nid:
                    assert hostname in namespaces["hostNameList"]
                    break
            else:
                assert False, f"Not found {namespace_id}"
        return True
    except Exception as ex:
        logger.info(ex)
        return False


def submit_job(webui_url,
               namespace_id,
               job_name=None,
               java_dependency_url=None,
               java_dependency_md5=None,
               downloader=None,
               job_code=TEST_PYTHON_JOB_CODE,
               total_memory_mb=None,
               max_total_memory_mb=None,
               set_code_search_path=True):
    try:
        json = _get_python_job(webui_url, java_dependency_url,
                               java_dependency_md5, downloader, job_code)
        json["namespaceId"] = namespace_id
        json["namespace"] = DEFAULT_RAY_NAMESPCE
        if job_name is not None:
            json["name"] = job_name
        if total_memory_mb is not None:
            json["totalMemoryMb"] = total_memory_mb
        if max_total_memory_mb is not None:
            json["maxTotalMemoryMb"] = max_total_memory_mb

        resp = requests.post(webui_url + "/jobs", json=json)
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_id_hex = result["data"]["jobId"]
        if set_code_search_path:
            package_path = "/tmp/ray/job/" + job_id_hex + "/package"
            if len(sys.path) > 0 and "/tmp/ray/job/" in sys.path[0]:
                sys.path[0] = package_path
            else:
                sys.path.insert(0, package_path)
            print("sys.path[0] = ", sys.path[0])
            ray.worker.global_worker._load_code_from_local = True
        return job_id_hex
    except Exception as ex:
        logger.info(ex)
        return None


def test_namespace(disable_aiohttp_cache, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    hostname = socket.gethostname()
    namespace_id = "default_namespace"

    def _create_namespace():
        try:
            resp = requests.post(
                webui_url + "/namespaces_v2",
                json={
                    "namespaceId": namespace_id,
                    "enableSubNamespaceIsolation": False,
                    "nodeShapeAndCountList": [{
                        "nodeCount": 1,
                        "group": "4c8g"
                    }]
                })
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            resp = requests.get(webui_url + "/namespaces")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            for nid in [namespace_id, RAY_DEFAULT_NODEGROUP]:
                for namespaces in result["data"]["namespaces"]:
                    if namespaces["namespaceId"] == nid:
                        assert hostname in namespaces["hostNameList"]
                        break
                else:
                    assert False, f"Not found {namespace_id}"
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_create_namespace, timeout=10)

    def _remove_namespace():
        try:
            resp = requests.delete(webui_url + f"/namespaces/{namespace_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            resp = requests.get(webui_url + "/namespaces")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            namespaces = result["data"]["namespaces"]
            assert len(namespaces) == 1
            assert namespace_id not in [n["namespaceId"] for n in namespaces]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_remove_namespace, timeout=10)


def test_submit_job_with_invalid_url(disable_aiohttp_cache, enable_test_module,
                                     fast_reporter_update,
                                     ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs", json=_get_python_job(webui_url))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            error_message = job_info["failErrorMessage"]
            assert job_info["state"] == "FAILED", error_message
            assert "Download failed" in error_message or \
                "wget" in error_message, error_message
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_error, timeout=120)

    resp = requests.get(webui_url + "/jobs?view=summary&state=INVALID_STATE")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is False, resp.text
    assert "INVALID_STATE" in result["msg"] and "RUNNING" in result["msg"]


def test_submit_job_with_incorrect_md5(
        disable_aiohttp_cache, enable_test_module, fast_reporter_update,
        ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url, java_dependency_url=fake_jar_url))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "FAILED", job_info["failErrorMessage"]
            assert "is corrupted" in job_info["failErrorMessage"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_error, timeout=30)

    resp = requests.get(webui_url + "/jobs?view=summary&state=FAILED")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + "/jobs?view=summary&state=RUNNING")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 0


@pytest.mark.parametrize("downloader", [
    job_consts.AIOHTTP_DOWNLOADER, job_consts.WGET_DOWNLOADER,
    job_consts.DRAGONFLY_DOWNLOADER
])
def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    fast_reporter_update, ray_start_with_dashboard,
                    downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)
    fake_jar_md5 = _gen_md5(__file__)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url,
                    java_dependency_url=fake_jar_url,
                    java_dependency_md5=fake_jar_md5,
                    downloader=downloader))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    resp = requests.get(webui_url + f"/jobs/{job_id}?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1
    assert summary["jobInfo"]["name"] == "Test job"

    def _check_running():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "RUNNING", job_info["failErrorMessage"]
            job_actors = result["data"]["detail"]["jobActors"]
            job_workers = result["data"]["detail"]["jobWorkers"]
            assert len(job_actors) > 0
            assert len(job_workers) > 0
            assert "slsUrl" in job_workers[0]
            assert "slsUrl" in job_info
            assert "eventUrl" in job_info
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_running, timeout=120)

    resp = requests.get(webui_url + "/jobs?view=summary&state=RUNNING")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    def _submit_job_with_conflict():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url,
                    java_dependency_url=fake_jar_url,
                    java_dependency_md5=fake_jar_md5,
                    downloader=downloader))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is False, resp.text
            assert result["data"]["conflictJobId"] == job_id
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job_with_conflict, 5)


@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_submit_job_with_numba_cache_dir(
        disable_aiohttp_cache, enable_test_module, fast_reporter_update,
        ray_start_with_dashboard, downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)
    fake_jar_md5 = _gen_md5(__file__)

    job_id = None

    code_string = """
import os
import ray

NUMBA_CACHE_DIR = 'NUMBA_CACHE_DIR'

@ray.remote
class Actor:
    def foo(self):
        # check actor has ncd dir
        actor_ncd = os.environ[NUMBA_CACHE_DIR]
        assert os.path.isdir(actor_ncd)
        return actor_ncd


def main():
    ncd = os.environ[NUMBA_CACHE_DIR]
    assert os.path.isdir(ncd)
    print(ncd)

    actor = Actor.remote()
    r = actor.foo.remote()
    actor_ncd = ray.get(r)
    assert actor_ncd is not None
    print(actor_ncd)
    test_string = "OK"
    ray.put_job_result(test_string)
    assert ray.get_job_result() == test_string


if __name__ == "__main__":
    ray.init()
    main()
"""

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url,
                    java_dependency_url=fake_jar_url,
                    java_dependency_md5=fake_jar_md5,
                    downloader=downloader,
                    job_code=code_string))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    test_string = "OK"

    def _check_string():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}?view=result")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_result = result["data"]["result"]
            assert job_result == test_string
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_string, timeout=5)


def test_job_data(ray_start_with_dashboard):
    ray.state.state.put_job_data(b"aaa", b"bbb")
    ray.state.state.put_job_data(b"ccc", b"ddd")
    assert ray.state.state.get_job_data(b"aaa") == b"bbb"
    assert ray.state.state.get_job_data(b"ccc") == b"ddd"


def test_job_result(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    # Invalid job id
    resp = requests.get(webui_url + "/jobs/00000000?view=result")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert result["data"]["result"] == ""

    job_id = "01000000"

    test_string = "hello world!"
    ray.put_job_result(test_string)
    assert ray.get_job_result() == test_string

    def _check_string():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}?view=result")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_result = result["data"]["result"]
            assert job_result == test_string
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_string, timeout=5)

    with pytest.raises(Exception):
        ray.state.put_job_result("x" * 32768)


@pytest.mark.asyncio
async def test_venv_hash(enable_test_module, fast_get_etag_timeout,
                         ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    job_id = "0180"

    VenvHash([], {})
    await VenvHash.create(job_id, [])

    d = {VenvHash(["grpcio"], {"url1": "x"}): True}
    assert VenvHash(["grpcio"], {"url1": "x"}) in d
    assert VenvHash(["grpcio > 1.32.0"], {"url1": "x"}) not in d
    assert VenvHash(["grpcio"], {"url1": "y"}) not in d

    # Mock key1 and key2 has same hash but different url content meta.
    key1 = VenvHash(["grpcio"], {"url1": "x"})
    key2 = VenvHash(["grpcio"], {"url1": "x"})
    assert hash(key1) == hash(key2)
    key2._url_content_hash = {}
    d = {key1: True}
    assert key2 not in d

    # VenvHash should have unique hash if vcs in requirement list.
    key1 = VenvHash(["git+http://git.example.com/MyProject#egg=MyProject"], {})
    key2 = VenvHash(["git+http://git.example.com/MyProject#egg=MyProject"], {})
    assert hash(key1) != hash(key2)

    # VenvHash should have unique hash if pip cmd in requirement list.
    key1 = VenvHash(["pip install -U six"], {})
    key2 = VenvHash(["pip install -U six"], {})
    assert hash(key1) != hash(key2)

    # VenvHash url with same etag.
    key1 = await VenvHash.create(job_id,
                                 [f"{webui_url}/test/url_with_same_etag"])
    key2 = await VenvHash.create(job_id,
                                 [f"{webui_url}/test/url_with_same_etag"])
    assert hash(key1) == hash(key2)
    d = {key1: True}
    assert key2 in d

    # VenvHash url with unique etag.
    key1 = await VenvHash.create(job_id,
                                 [f"{webui_url}/test/url_with_unique_etag"])
    key2 = await VenvHash.create(job_id,
                                 [f"{webui_url}/test/url_with_unique_etag"])
    assert hash(key1) != hash(key2)
    d = {key1: True}
    assert key2 not in d

    # VenvHash url without etag.
    key1 = await VenvHash.create(job_id,
                                 [f"{webui_url}/test/url_without_etag"])
    key2 = await VenvHash.create(job_id,
                                 [f"{webui_url}/test/url_without_etag"])
    assert hash(key1) != hash(key2)
    d = {key1: True}
    assert key2 not in d

    # VenvHash with invalid url.
    key1 = await VenvHash.create(job_id, [f"{webui_url}/test/url_not_exists"])
    key2 = await VenvHash.create(job_id, [f"{webui_url}/test/url_not_exists"])
    assert hash(key1) != hash(key2)
    d = {key1: True}
    assert key2 not in d

    out1 = hash(await VenvHash.create("0180", ["a", "b"]))
    out2 = subprocess.check_output([
        sys.executable, "-c",
        "from ray.new_dashboard.modules.job.virtualenv_cache import VenvHash;"
        "print(hash(VenvHash(['a', 'b'], {})))"
    ])
    out2, *_ = out2.split()
    assert str(out1) == out2.strip().decode("utf-8")


@pytest.mark.asyncio
async def test_venv_cache():
    VenvCache.clear()
    VenvCache._serializer = VenvCacheSerializer
    VenvCache._free_venv_count = 3

    async def _fail_new_virtualenv(path):
        raise Exception("Mock new virtualenv failure.")

    with pytest.raises(Exception):
        await VenvCache.get_virtualenv("", "0180", [], _fail_new_virtualenv)

    job_id1 = "0180"
    job_id2 = "0280"

    fake_virtual_env = set()

    async def _fake_new_virtualenv(path):
        logger.info("_fake_new_virtualenv %s", path)
        fake_virtual_env.add(path)

    async def _fake_delete_virtualenv(path):
        logger.info("_fake_delete_virtualenv %s", path)
        fake_virtual_env.discard(path)

    venv = await VenvCache.get_virtualenv("", job_id1, [],
                                          _fake_new_virtualenv)
    assert len(fake_virtual_env) == 1
    assert list(fake_virtual_env)[0] == venv

    venv = await VenvCache.get_virtualenv("", job_id2, [],
                                          _fake_new_virtualenv)
    assert len(fake_virtual_env) == 1
    assert list(fake_virtual_env)[0] == venv

    await VenvCache.deref_virtualenv(job_id1, _fake_delete_virtualenv)
    assert len(fake_virtual_env) == 1

    await VenvCache.deref_virtualenv(job_id2, _fake_delete_virtualenv)
    assert len(fake_virtual_env) == 1

    job_id_to_venv = {}

    for x in range(10):
        job_id = f"{x:04}"
        venv = await VenvCache.get_virtualenv("", job_id, [str(x)],
                                              _fake_new_virtualenv)
        job_id_to_venv[job_id] = venv

    assert len(fake_virtual_env) == 11
    assert len(job_id_to_venv) == 10

    # Current free list is [[]]

    for x in range(3):
        job_id = f"{x:04}"
        await VenvCache.deref_virtualenv(job_id, _fake_delete_virtualenv)

    # Delete [], then
    # Current free list is [["0"], ["1"], ["2"]]

    assert len(fake_virtual_env) == len(job_id_to_venv)
    assert fake_virtual_env == set(job_id_to_venv.values())

    job_id = "1000"
    venv = await VenvCache.get_virtualenv("", job_id, [str(1)],
                                          _fake_new_virtualenv)
    assert job_id_to_venv[f"{1:04}"] == venv
    job_id_to_venv[job_id] = venv

    job_id = "2000"
    venv = await VenvCache.get_virtualenv("", job_id, [str(3)],
                                          _fake_new_virtualenv)
    assert job_id_to_venv[f"{3:04}"] == venv
    job_id_to_venv[job_id] = venv

    # Current free list is [["0"], ["2"]]

    count = len(fake_virtual_env)
    assert count == 10
    job_id = "3000"
    venv = await VenvCache.get_virtualenv("", job_id, [str(3000)],
                                          _fake_new_virtualenv)
    assert venv not in job_id_to_venv.values()
    job_id_to_venv[job_id] = venv
    assert len(fake_virtual_env) == count + 1

    job_id = f"{4:04}"
    await VenvCache.deref_virtualenv(job_id, _fake_delete_virtualenv)
    assert len(fake_virtual_env) == 11

    # Current free list is [["0"], ["2"], ["4"]]

    job_id = f"{5:04}"
    await VenvCache.deref_virtualenv(job_id, _fake_delete_virtualenv)
    assert len(fake_virtual_env) == 10

    # Delete ["0"], then
    # Current free list is [["2"], ["4"], ["5"]]
    assert job_id_to_venv[f"{0:04}"] not in fake_virtual_env

    job_id = "3000"
    await VenvCache.deref_virtualenv(job_id, _fake_delete_virtualenv)
    assert len(fake_virtual_env) == 9

    # Delete ["2"], then
    # Current free list is [["4"], ["5"], ["3000"]]
    assert job_id_to_venv[f"{2:04}"] not in fake_virtual_env

    free_venv_list = VenvCache.free_list()
    expected = [
        job_id_to_venv[job_id] for job_id in [f"{4:04}", f"{5:04}", "3000"]
    ]
    assert expected == free_venv_list

    # noqa: E501 1  + 10        - 3        + 1        - 1     - 1     - 1        + 3               = 9
    # noqa: E501 [] + range(10) - range(3) + ["3000"] - ["4"] - ["5"] - ["3000"] + free list count
    assert VenvCache.size() == 9

    # dereference not exist job.
    await VenvCache.deref_virtualenv("not_exist_job", _fake_delete_virtualenv)
    assert VenvCache.size() == 9


@pytest.mark.asyncio
async def test_venv_cache_serializer():
    VenvCache.clear()
    VenvCache._serializer = VenvCacheFileSerializer
    VenvCache._free_venv_count = 5

    async def _fake_delete_virtualenv(p):
        shutil.rmtree(p, ignore_errors=True)

    await VenvCache.init("", _fake_delete_virtualenv)
    assert len(VenvCache.free_list()) == 0
    assert VenvCache.size() == 0
    assert VenvCache.job_count() == 0

    with tempfile.TemporaryDirectory() as cache_dir:
        test_venv_map = {
            VenvHash([str(i)], {}): os.path.join(cache_dir, str(uuid.uuid4()))
            for i in range(10)
        }
        for venv_hash, virtualenv_path in test_venv_map.items():
            os.makedirs(virtualenv_path)
            VenvCacheFileSerializer.init(virtualenv_path, venv_hash)

        VenvCacheFileSerializer.ref(test_venv_map[VenvHash([str(1)], {})],
                                    f"{1:04}")
        VenvCacheFileSerializer.ref(test_venv_map[VenvHash([str(1)], {})],
                                    f"{2:04}")
        VenvCacheFileSerializer.ref(test_venv_map[VenvHash([str(2)], {})],
                                    f"{3:04}")

        VenvCacheFileSerializer.deref(test_venv_map[VenvHash([str(1)], {})],
                                      f"{2:04}")
        VenvCacheFileSerializer.ref(test_venv_map[VenvHash([str(2)], {})],
                                    f"{3:04}")

        venv_hash, virtualenv_path = test_venv_map.popitem()
        shutil.rmtree(virtualenv_path)
        os.mkdir(virtualenv_path)

        deleted_virtualenv = []

        async def _delete_virtualenv(p):
            deleted_virtualenv.append(p)
            shutil.rmtree(p, ignore_errors=True)

        await VenvCache.init(cache_dir, _delete_virtualenv)
        assert len(os.listdir(cache_dir)) == 7
        assert len(VenvCache.free_list()) == 5
        assert VenvCache.size() == 7
        assert VenvCache.job_count() == 2
        assert virtualenv_path in deleted_virtualenv

        # Test the VenvCache is OK after failover.
        new_virtualenv = False

        async def _fake_new_virtualenv(path):
            logger.info("_fake_new_virtualenv %s", path)
            nonlocal new_virtualenv
            new_virtualenv = True

        r = await VenvCache.get_virtualenv(cache_dir, f"{1:04}", [str(1)],
                                           _fake_new_virtualenv)
        assert new_virtualenv is False
        assert r == test_venv_map[VenvHash([str(1)], {})]


@pytest.mark.parametrize("driver_pid_mode", [
    "not_driver_node", "not_exist", "exist_but_not_driver",
    "exist_and_is_driver"
])
@mock.patch.object(VenvCache, "deref_virtualenv")
@pytest.mark.asyncio
async def test_clean_job_virtualenv(fake_deref_virtualenv, driver_pid_mode):
    proc = None
    dpid = None
    if driver_pid_mode == "not_driver_node":
        dpid = 0
    elif driver_pid_mode == "not_exist":
        # Make sure dpid doesn't exist
        dpid = 9999999
    elif driver_pid_mode == "exist_but_not_driver":
        proc = await asyncio.create_subprocess_exec(*["sleep", "120"], )
        dpid = proc.pid
    elif driver_pid_mode == "exist_and_is_driver":
        proc = await asyncio.create_subprocess_exec(
            *["sleep", "120"],
            env={"RAY_JOB_ID_FOR_DRIVER": ray.JobID.from_int(1).hex()})
        dpid = proc.pid

    assert dpid is not None

    class _FakeDashboardAgent:
        temp_dir = "fake_temp_dir"
        http_session = None

    class _FakeRequest:
        job_id = ray.JobID.from_int(1).binary()
        driver_pid = dpid

    agent = JobAgent(_FakeDashboardAgent)
    await agent.CleanJobEnv(_FakeRequest, None)
    assert fake_deref_virtualenv.call_count > 0

    if driver_pid_mode == "exist_but_not_driver":
        time.sleep(2)
        assert psutil.pid_exists(dpid)
    elif driver_pid_mode == "exist_and_is_driver":
        time.sleep(2)
        assert not psutil.pid_exists(dpid)


@pytest.mark.asyncio
async def test_clean_job_data():
    job_root_dir = job_consts.JOB_ROOT_DIR.format(temp_dir=TEMP_DIR)
    job_dir = job_consts.JOB_DIR.format(temp_dir=TEMP_DIR, job_id="5555")
    task = JobData.monitor_job_data(job_root_dir, 3)
    job_data_dir_base = os.path.join(job_dir,
                                     job_consts.JOB_DATA_DIR_BASE_NAME)
    job_data_dir = os.path.join(job_data_dir_base, "6666")
    os.makedirs(job_data_dir, exist_ok=True)
    assert os.path.isdir(job_data_dir)
    await asyncio.sleep(4)
    task.cancel()
    assert not os.path.isdir(job_data_dir)


@mock.patch.object(VenvCacheFileSerializer, "ref")
@mock.patch.object(VenvCacheFileSerializer, "init")
@mock.patch.object(PreparePythonEnviron, "_check_ray_is_internal")
@mock.patch.object(
    PreparePythonEnviron,
    "_ray_mark_internal",
    return_value=("fake_ray_version", "fake_ray_path"))
@mock.patch.object(PreparePythonEnviron, "_check_output_cmd", return_value="")
@mock.patch.object(PreparePythonEnviron, "_download_package_with_cache")
@pytest.mark.asyncio
async def test_pip_version_no_python_dependency(
        fake_download_package, fake_cmd, fake_mark_ray, fake_check_ray,
        fake_init, fake_ref):
    job_info = JobInfo("fake_job_id", {
        "pipVersion": "<20.3",
    }, "fake_temp_dir", "fake_log_dir")
    await PreparePythonEnviron(job_info, None).run()
    # The pip version not executed if there is no Python dependency.
    assert fake_download_package.call_count == 0
    assert fake_mark_ray.call_count == 1
    assert fake_check_ray.call_count == 1
    assert fake_cmd.call_count == 2
    assert fake_init.call_count == (1 if job_info.enable_virtualenv_cache()
                                    else 0)
    assert fake_ref.call_count == (1 if job_info.enable_virtualenv_cache() else
                                   0)

    if fake_init.call_count > 0:
        venv_python_executable = os.path.join(fake_init.call_args.args[0],
                                              "bin/python")
    else:
        venv_python_executable = \
            "fake_temp_dir/job/fake_job_id/pyenv/bin/python"

    pip_version_call = mock.call([
        venv_python_executable, "-m", "pip", "install",
        "--disable-pip-version-check", "pip<20.3", "-i",
        "https://pypi.antfin-inc.com/simple/"
    ])
    assert sum(c == pip_version_call for c in fake_cmd.mock_calls) == 0


@mock.patch.object(VenvCacheFileSerializer, "ref")
@mock.patch.object(VenvCacheFileSerializer, "init")
@mock.patch.object(PreparePythonEnviron, "_check_ray_is_internal")
@mock.patch.object(
    PreparePythonEnviron,
    "_ray_mark_internal",
    return_value=("fake_ray_version", "fake_ray_path"))
@mock.patch.object(PreparePythonEnviron, "_check_output_cmd", return_value="")
@mock.patch.object(PreparePythonEnviron, "_download_package_with_cache")
@pytest.mark.asyncio
async def test_pip_install_with_no_cache(fake_download_package, fake_cmd,
                                         fake_mark_ray, fake_check_ray,
                                         fake_init, fake_ref):
    job_info = JobInfo("fake_job_id", {
        "pipVersion": "<20.3",
        "dependencies": {
            "python": ["no_cache_test"],
        },
    }, "fake_temp_dir", "fake_log_dir")
    job_info.set_pip_no_cache(True)
    prepare_python_env = PreparePythonEnviron(job_info, None)
    await prepare_python_env.run()
    # The pip version not executed if there is no Python dependency.
    assert fake_download_package.call_count == 0
    assert fake_mark_ray.call_count == 1
    assert fake_check_ray.call_count == 1
    assert fake_init.call_count == (1 if job_info.enable_virtualenv_cache()
                                    else 0)
    assert fake_ref.call_count == (1 if job_info.enable_virtualenv_cache() else
                                   0)
    # Add pip install and pip check
    assert fake_cmd.call_count == 4

    if fake_init.call_count > 0:
        venv_python_executable = os.path.join(fake_init.call_args.args[0],
                                              "bin/python")
    else:
        venv_python_executable = \
            "fake_temp_dir/job/fake_job_id/pyenv/bin/python"

    pip_install_call = mock.call(
        [
            venv_python_executable, "-m", "pip", "install",
            "--disable-pip-version-check", "-i",
            "https://pypi.antfin-inc.com/simple/", "--no-cache-dir",
            "no_cache_test"
        ],
        env=prepare_python_env._pip_env)
    assert sum(c == pip_install_call for c in fake_cmd.mock_calls) == 1


@mock.patch.object(VenvCacheFileSerializer, "ref")
@mock.patch.object(VenvCacheFileSerializer, "init")
@mock.patch.object(PreparePythonEnviron, "_check_ray_is_internal")
@mock.patch.object(
    PreparePythonEnviron,
    "_ray_mark_internal",
    return_value=("fake_ray_version", "fake_ray_path"))
@mock.patch.object(PreparePythonEnviron, "_check_output_cmd", return_value="")
@mock.patch.object(PreparePythonEnviron, "_download_package_with_cache")
@pytest.mark.parametrize(
    "job_info",
    [
        # All Python dependencies are pip cmd.
        JobInfo(
            "fake_job_id", {
                "dependencies": {
                    "python": ["pip install -U six"]
                },
                "pipVersion": "<20.3",
            }, "fake_temp_dir", "fake_log_dir"),
        # All Python dependencies are packages names.
        JobInfo("fake_job_id", {
            "dependencies": {
                "python": [
                    "six",
                ]
            },
            "pipVersion": "<20.3",
        }, "fake_temp_dir", "fake_log_dir"),
        # Python dependencies are mixed.
        JobInfo(
            "fake_job_id", {
                "dependencies": {
                    "python": [
                        "six",
                        "pip install -U six",
                    ]
                },
                "pipVersion": "<20.3",
            }, "fake_temp_dir", "fake_log_dir")
    ])
@pytest.mark.asyncio
async def test_pip_version_with_python_dependency(
        fake_download_package, fake_cmd, fake_mark_ray, fake_check_ray,
        fake_init, fake_ref, job_info):
    await PreparePythonEnviron(job_info, None).run()
    # The pip version should be executed exactly once before any pip cmd.
    assert fake_download_package.call_count == 0
    assert fake_mark_ray.call_count == 1
    assert fake_check_ray.call_count == 1
    assert fake_init.call_count == (1 if job_info.enable_virtualenv_cache()
                                    else 0)
    assert fake_ref.call_count == (1 if job_info.enable_virtualenv_cache() else
                                   0)

    pip_tmp_dir = job_consts.PYTHON_VIRTUALENV_PIP_TMP_DIR.format(
        temp_dir="fake_temp_dir", job_id="fake_job_id")
    pip_env = dict(os.environ, TMPDIR=pip_tmp_dir)

    if fake_init.call_count > 0:
        venv_python_executable = os.path.join(fake_init.call_args.args[0],
                                              "bin/python")
    else:
        venv_python_executable = \
            "fake_temp_dir/job/fake_job_id/pyenv/bin/python"

    pip_version_call = mock.call(
        [
            venv_python_executable, "-m", "pip", "install",
            "--disable-pip-version-check", "pip<20.3", "-i",
            "https://pypi.antfin-inc.com/simple/"
        ],
        env=pip_env)
    assert fake_cmd.call_count > 2
    assert fake_cmd.mock_calls[1] == pip_version_call
    assert sum(c == pip_version_call for c in fake_cmd.mock_calls) == 1


@mock.patch.object(VenvCacheFileSerializer, "ref")
@mock.patch.object(VenvCacheFileSerializer, "init")
@mock.patch.object(PreparePythonEnviron, "_is_in_virtualenv")
@mock.patch("builtins.open", create=True)
@mock.patch.object(PreparePythonEnviron, "_check_ray_is_internal")
@mock.patch.object(
    PreparePythonEnviron,
    "_ray_mark_internal",
    return_value=("fake_ray_version", "fake_ray_path"))
@mock.patch.object(PreparePythonEnviron, "_check_output_cmd", return_value="")
@mock.patch.object(
    PreparePythonEnviron,
    "_download_package_with_cache",
    return_value="cached_package")
@pytest.mark.parametrize("in_virtualenv", [True, False])
@pytest.mark.asyncio
async def test_prepare_python_environ(
        fake_download_package, fake_cmd, fake_mark_ray, fake_check_ray,
        fake_open, fake_in_virtualenv, fake_init, fake_ref, in_virtualenv):
    job_info = JobInfo(
        "fake_job_id",
        {
            "dependencies": {
                "python": [
                    "six",
                    "nose-cov",  # Package name with -
                    "docopt == 0.6.1",  # Package name with spaces and ==
                    "keyring >= 4.1.1",  # Package name with >=
                    "coverage != 3.5",  # Package name with !=
                    "Mopidy-Dirble ~= 1.1",  # Package name with ~=
                    "pip install -U \"six<=1.0\"",
                    "http://xxx/yyy.zip",
                ]
            },
            "pipVersion": "<20.3",
        },
        "fake_temp_dir",
        "fake_log_dir")
    fake_in_virtualenv.return_value = in_virtualenv
    await PreparePythonEnviron(job_info, None).run()
    assert fake_download_package.call_count == 1
    assert fake_mark_ray.call_count == 1
    assert fake_check_ray.call_count == 1
    assert fake_init.call_count == (1 if job_info.enable_virtualenv_cache()
                                    else 0)
    assert fake_ref.call_count == (1 if job_info.enable_virtualenv_cache() else
                                   0)

    pip_tmp_dir = job_consts.PYTHON_VIRTUALENV_PIP_TMP_DIR.format(
        temp_dir="fake_temp_dir", job_id="fake_job_id")
    pip_env = dict(os.environ, TMPDIR=pip_tmp_dir)

    if fake_init.call_count > 0:
        venv_path = fake_init.call_args.args[0]
    else:
        venv_path = "fake_temp_dir/job/fake_job_id/pyenv"
    venv_python_executable = os.path.join(venv_path, "bin/python")

    if in_virtualenv:
        expect_calls = [
            mock.call([
                sys.executable,
                os.path.join(
                    os.path.dirname(job_consts.__file__),
                    "clonevirtualenv.py"),
                os.path.abspath(
                    os.path.join(os.path.dirname(sys.executable), "..")),
                venv_path
            ])
        ]
    else:
        expect_calls = [
            mock.call([
                sys.executable, "-m", "virtualenv", "--app-data",
                "fake_temp_dir/job/fake_job_id/virtualenv_app_data",
                "--reset-app-data", "--no-periodic-update",
                "--system-site-packages", "--no-download", venv_path
            ])
        ]

    expect_calls.extend([
        mock.call(
            [
                venv_python_executable, "-m", "pip", "install",
                "--disable-pip-version-check", "pip<20.3", "-i",
                "https://pypi.antfin-inc.com/simple/"
            ],
            env=pip_env),
        mock.call(
            [
                venv_python_executable, "-m", "pip", "install",
                "--disable-pip-version-check", "-i",
                "https://pypi.antfin-inc.com/simple/", "six", "nose-cov",
                "docopt == 0.6.1", "keyring >= 4.1.1", "coverage != 3.5",
                "Mopidy-Dirble ~= 1.1", "cached_package"
            ],
            env=pip_env),
        mock.call(
            [venv_python_executable, "-m", "pip", "install", "-U", "six<=1.0"],
            env=pip_env),
        mock.call(
            [
                venv_python_executable, "-m", "pip", "check",
                "--disable-pip-version-check"
            ],
            env=pip_env)
    ])

    fake_cmd.assert_has_calls(expect_calls)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
