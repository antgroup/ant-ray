import os
from random import randint
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
import asyncio
import async_timeout
import time
import psutil

import mock
import pytest
import ray
from ray.new_dashboard.modules.job import job_consts
from ray.new_dashboard.modules.job.job_agent import (
    DownloadPackage,
    JobInfo,
    JobAgent,
    JobProcessor,
    PrepareJavaEnviron,
    PreparePythonEnviron,
    DownloadArchive,
)
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.new_dashboard.utils import create_task, LazySemaphore
from ray import state
from ray.ray_constants import (
    gcs_task_scheduling_enabled,
    env_integer,
    from_memory_units,
)
from ray._private.utils import get_ray_site_packages_path

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
    job["preInitializeJobRuntimeEnvEnabled"] = True
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


@mock.patch("os.rename", return_value=None)
@mock.patch("uuid.uuid4", return_value="test_uuid")
@mock.patch.object(DownloadArchive, "_check_output_cmd", return_value="")
@mock.patch.object(DownloadArchive, "_download_package")
@pytest.mark.asyncio
async def test_download_archive(fake_download, fake_cmd, fake_uuid,
                                fake_rename):
    job_info = JobInfo(
        "fake_job_id",
        {
            "dependencies": {
                "archive": [
                    "http://xxx/yyy.zip",
                    "http://xxx%2fyyy.zip",  # The same url.
                    # The different url with same filename.
                    "http://yyy/yyy.zip",
                    # The different url with same filename.
                    "http://xxx/yyy.zip?a=1",
                ]
            },
        },
        "fake_temp_dir",
        "fake_log_dir")
    await DownloadArchive(job_info, None).run()

    download_calls = [
        mock.call(
            None, "http://yyy/yyy.zip",
            "fake_temp_dir/job/fake_job_id/archives/yyy_30297df3.zip"
            ".test_uuid"),
        mock.call(
            None, "http://xxx%2fyyy.zip",
            "fake_temp_dir/job/fake_job_id/archives/yyy_aa0b332c.zip"
            ".test_uuid"),
        mock.call(
            None, "http://xxx/yyy.zip",
            "fake_temp_dir/job/fake_job_id/archives/yyy_aa0b332c.zip"
            ".test_uuid"),
        mock.call(
            None, "http://xxx/yyy.zip?a=1",
            "fake_temp_dir/job/fake_job_id/archives/yyy_519f5fc2.zip"
            ".test_uuid"),
    ]

    fake_download.assert_has_calls(download_calls, any_order=True)


def test_check_signature(disable_aiohttp_cache, enable_test_module,
                         enable_check_signature, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    body = "{\"name\": \"sign_test\", \"language\": \"JAVA\", " \
           "\"namespaceId\": \"NAMESPACE_LABEL_RAY_DEFAULT\", " \
           "\"url\": \"http://XXX.zip\", \"driverEntry\": \"HelloWorld\"}"
    signature = "OsFnstOdz1PsnXt7FXsjjN/Ri192TOLZhyuP7T/RVH02ZZOX674Ij" \
                "f5tbOgzgRN9L0DS03cGXeKRB3yPXQiZwtebv3f+/W5k7/1JS2DJRb" \
                "zfUWInBM9XQGyFaw3VdVQxhjbIUvIMXW2OB89ojRHHhBJHpiAePzr" \
                "KKY1cPyWwL7uv9wXMtCK+PUCz5eft9dmMVdzlnc+/+qi6WcbQ6U8P" \
                "3jYM5TyxtaFF/Qv2MZ70IWoo0lH1jVM94CFsrr+3sH1MmCbM30WKi" \
                "m0N5jG13P5UOt+gctIqHs+WbBgcMlldzNnmZQn0zpXACJ+W+Qi+HT" \
                "OfXv8V/4wNj5U4R4S7HJcAlw=="

    headers = {"Body-Signature": signature}

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs", data=body, headers=headers)
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    def _submit_job_with_error_signature():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                data=body,
                headers={"signature": "bad sign"})
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is False, resp.text
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job_with_error_signature, 5)


@mock.patch.object(JobProcessor, "_download_package")
@pytest.mark.asyncio
async def test_download_manager_concurrency(fake_download_package):
    num_concurrent_tasks = int(
        job_consts.DOWNLOAD_MANAGER_SEMAPHORE.value * 1.5)

    block_download_events = []

    async def _block_download(*args):
        e = asyncio.Event()
        block_download_events.append(e)
        await e.wait()

    fake_download_package.side_effect = _block_download

    job_info = JobInfo("fake_job_id", {}, "fake_temp_dir", "fake_log_dir")
    p = JobProcessor(job_info)
    download_tasks = []
    for i in range(num_concurrent_tasks):
        task = create_task(
            p._download_package_with_cache(None, "fake_download_dir",
                                           "fake_cache_dir", f"fake_url_{i}"))
        download_tasks.append(task)

    await asyncio.sleep(0.1)

    # The semaphore make sure only 10 download coroutines starts.
    assert len(
        block_download_events) == job_consts.DOWNLOAD_MANAGER_SEMAPHORE.value

    num_download_finished = int(
        job_consts.DOWNLOAD_MANAGER_SEMAPHORE.value * 0.6)
    # Trigger the first 10 download coroutines finished.
    for i in range(num_download_finished):
        block_download_events[i].set()

    await asyncio.sleep(0.1)

    # The first 10 download tasks will be finished, too.
    for i in range(len(block_download_events)):
        if i < num_download_finished:
            assert download_tasks[i].done()
        else:
            assert not download_tasks[i].done()

    for _ in range(20):
        if len(block_download_events) == num_concurrent_tasks:
            break
        await asyncio.sleep(0.1)

    # Trigger all download coroutines finished.
    for e in block_download_events:
        e.set()

    await asyncio.sleep(0.1)

    # All download tasks will be finished.
    for task in download_tasks:
        assert task.done()


@mock.patch(
    "ray.new_dashboard.modules.job.job_agent.PreparePythonEnviron",
    autospec=True)
@mock.patch(
    "ray.new_dashboard.modules.job.job_agent.PrepareJavaEnviron",
    autospec=True)
@mock.patch(
    "ray.new_dashboard.modules.job.job_agent.DownloadPackage", autospec=True)
@pytest.mark.asyncio
async def test_prepare_job_environ_concurrency(fake_prepare_python_environ,
                                               fake_prepare_java_environ,
                                               fake_download_package):
    class _FakeDashboardAgent:
        temp_dir = "fake_temp_dir"
        http_session = None

    class _FakeRequest:
        start_driver = True

    job_info = JobInfo("fake_job_id", {"language": "PYTHON"}, "fake_temp_dir",
                       "fake_log_dir")

    agent = JobAgent(_FakeDashboardAgent)
    num_concurrent_tasks = int(
        job_consts.JOB_PREPARE_ENVIRONMENT_SEMAPHORE.value * 2)

    prepare_tasks = []
    for i in range(num_concurrent_tasks):
        task = create_task(agent._prepare_job_environ(job_info, _FakeRequest))
        prepare_tasks.append(task)

    with async_timeout.timeout(10):
        condition = False
        while not condition:
            condition = True
            await asyncio.sleep(0.1)

            for task in prepare_tasks:
                if not task.done():
                    condition = False
                    break


@mock.patch.object(PreparePythonEnviron, "run", autospec=True)
@mock.patch.object(PrepareJavaEnviron, "run")
@mock.patch.object(DownloadPackage, "run")
@pytest.mark.asyncio
async def test_prepare_job_with_bad_wheel(fake_download_package_run,
                                          fake_prepare_java_envion_run,
                                          fake_prepare_python_environ_run):
    rebuild_job_consts()

    class _FakeDashboardAgent:
        temp_dir = "fake_temp_dir"
        http_session = None

    class _FakeRequest:
        start_driver = True

    async def fake_python_run(self, *args, **kwags):
        pip_install_with_no_cache = self._job_info.is_pip_no_cache()
        if pip_install_with_no_cache:
            """ Pip install with no cache in second retry.
            """
            return
        virtual_env_path = f"/tmp/virtual{uuid.uuid4().hex}"
        virtual_env_app_path = f"/tmp/virtual_app{uuid.uuid4().hex}"
        try:
            await self._create_virtualenv(virtual_env_path,
                                          virtual_env_app_path)
            python = self._get_virtualenv_python(virtual_env_path)
            pip_version = self._job_info.pip_version()
            await self._check_output_cmd([
                python, "-m", "pip", "install", "--disable-pip-version-check",
                f"pip{pip_version}"
            ])
            await self._check_output_cmd([
                python, "-m", "pip", "install",
                "/tmp/test-0.0.1-py3-none-any.whl"
            ])
        finally:
            shutil.rmtree(virtual_env_path, ignore_errors=True)
            shutil.rmtree(virtual_env_app_path, ignore_errors=True)

    async def foo_run(*args, **kwags):
        """ Do nothing!
        """

    fake_prepare_python_environ_run.side_effect = fake_python_run
    fake_prepare_java_envion_run.side_effect = foo_run
    fake_download_package_run.side_effect = foo_run
    job_info_1 = JobInfo(
        "fake_job_id",
        {
            "language": "PYTHON",
            "virtualenvCache": False,
            # When pip >= 22, It will raise InvalidWheel instead of
            # BadZipFile, and we rely on pip-cmd stderr to determine the
            # type of error, but str(InvalidWheel) not include strings
            # "InvalidWheel", so we choose to specify the pip version
            # "pip<22.0".
            "pipVersion": "<22.0",
        },
        "fake_temp_dir",
        "fake_log_dir")

    job_info_2 = JobInfo("fake_job_id", {
        "language": "PYTHON",
        "virtualenvCache": False,
        "pipVersion": ">=22.0",
    }, "fake_temp_dir", "fake_log_dir")

    # Bad wheel
    with open("/tmp/test-0.0.1-py3-none-any.whl", "wb") as f:
        f.write(b"bad wheel")

    agent = JobAgent(_FakeDashboardAgent)
    num_concurrent_tasks = int(
        job_consts.JOB_PREPARE_ENVIRONMENT_SEMAPHORE.value * 2)

    prepare_tasks = []
    for i in range(num_concurrent_tasks):
        task = create_task(
            agent._prepare_job_environ(job_info_1, _FakeRequest))
        prepare_tasks.append(task)

        task = create_task(
            agent._prepare_job_environ(job_info_2, _FakeRequest))
        prepare_tasks.append(task)

    await asyncio.gather(*prepare_tasks)
    os.remove("/tmp/test-0.0.1-py3-none-any.whl")


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "job_memory_check_ratio": 1.0,
            "overcommit_ratio": 1.0,
            "enable_job_quota": True
        },
        "_memory": 8 * 1024 * 1024 * 1024
    }],
    indirect=True)
@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_update_fixed_job_memory_requirements(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard,
        downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    namespace_id = "default_namespace"
    assert create_namespace(webui_url=webui_url, namespace_id=namespace_id)

    # Now the cluster has 8G memory.
    # Try to submit job_1 with 6000MB and actually only uses 4000MB.
    actor_name_1 = "actor1"
    job_id_1 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_1",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name_1),
        total_memory_mb=6000)
    assert job_id_1 is not None
    print("job_id_1 = {}".format(job_id_1))
    actor1 = _get_named_actor_with_retry(actor_name_1)
    assert actor1 is not None
    assert ray.get(actor1.foo.remote(1)) == 1

    job_id = ray.JobID(ray._private.utils.hex_to_binary(job_id_1))
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=7000)
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=4000)
    # Ensure job quota update failed as the using resources are 4000MB.
    with pytest.raises(Exception):
        state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=3000)

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=5000)
    # Now the job_1's quota is 5000MB, and 4000MB is in used.

    # Try to submit job_2 with 4000MB too.
    actor_name_2 = "actor2"
    job_id_2 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_2",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=3000, actor_name=actor_name_2),
        total_memory_mb=4000)
    assert job_id_2 is None

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=4000)
    # Now the job_1's quota is 4000MB, and 4000MB is in used.

    # Try to submit job_2 with 4000MB again.
    job_id_2 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_2",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=3000, actor_name=actor_name_2),
        total_memory_mb=4000)
    assert job_id_2 is not None
    actor2 = _get_named_actor_with_retry(actor_name_2)
    assert actor2 is not None
    assert ray.get(actor2.foo.remote(1)) == 1

    job_id = ray.JobID(ray._private.utils.hex_to_binary(job_id_2))
    with pytest.raises(Exception):
        state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=5000)
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=3000)
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=4000)


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "job_memory_check_ratio": 1.0,
            "enable_job_quota": True
        },
        "_memory": 8 * 1024 * 1024 * 1024
    }],
    indirect=True)
@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_job_config_pub_sub(disable_aiohttp_cache, enable_test_module,
                            ray_start_with_dashboard, downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    def query_job_min_memory_requirements(job_id_hex, timeout_seconds=10):
        total_memory_mb, _ = _query_job_memory_requirements(
            job_id_hex, webui_url, timeout_seconds)
        return total_memory_mb

    namespace_id = "default_namespace"
    assert create_namespace(webui_url=webui_url, namespace_id=namespace_id)

    # Now the cluster has 8G memory.
    # Try to submit job_1 with 6000MB and actually only uses 4000MB.
    actor_name_1 = "actor1"
    job_id_hex = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name_1),
        total_memory_mb=6000)
    assert job_id_hex is not None
    print("job_id_hex = {}".format(job_id_hex))
    actor1 = _get_named_actor_with_retry(actor_name_1)
    assert actor1 is not None
    assert ray.get(actor1.foo.remote(1)) == 1

    job_id = ray.JobID(ray._private.utils.hex_to_binary(job_id_hex))
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=7000)
    assert query_job_min_memory_requirements(job_id_hex) == 7000 * MB

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=4000)
    assert query_job_min_memory_requirements(job_id_hex) == 4000 * MB

    # Ensure job quota update failed as the using resources are 4000MB.
    with pytest.raises(Exception):
        state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=3000)
    assert query_job_min_memory_requirements(job_id_hex) == 4000 * MB

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=5000)
    assert query_job_min_memory_requirements(job_id_hex) == 5000 * MB


def test_eval_var_in_env_str():
    # Clear env for test.
    os.environ.pop("LD_LIBRARY_PATH", None)

    old_path = os.getenv("PATH", "")
    job_info = JobInfo(
        "fake_job_id", {
            "env": {
                "LD_LIBRARY_PATH": "test_library_path:${LD_LIBRARY_PATH}",
                "xxx": "${RAY_JOB_DIR}, ${RAY_JOB_DIR}",
                "PATH": "/tmp:${PATH}"
            }
        }, "fake_temp_dir", "fake_log_dir")

    env = job_info.env()

    assert env["LD_LIBRARY_PATH"].startswith("test_library_path:")
    assert env["xxx"] == "fake_temp_dir/job/fake_job_id/package, " \
        "fake_temp_dir/job/fake_job_id/package"
    assert env["PATH"] == "/tmp:" + old_path


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "job_memory_check_ratio": 1.0,
            "raylet_whole_report_resources_period_milliseconds": 1000
        },
        "_memory": 8 * 1024 * 1024 * 1024
    }],
    indirect=True)
@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_pg_resource_leak(disable_aiohttp_cache, enable_test_module,
                          ray_start_with_dashboard, downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)
    namespace_id = "default_namespace"
    assert create_namespace(webui_url=webui_url, namespace_id=namespace_id)

    def _gen_job_code(memory_mb, actor_name=None):
        return """
import os
import ray
import time

memory_in_bytes = {}

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
    placement_group = ray.util.placement_group(
        name="pg_name",
        strategy="SPREAD",
        bundles=[{{"CPU": 0.01, "memory": memory_in_bytes}}]
    )
    assert placement_group.wait(10000)

    actor = Actor.options(
        placement_group=placement_group,
        placement_group_bundle_index=0,
        memory=memory_in_bytes,
        name={}).remote()

    counter = 0
    while True:
        r = actor.foo.remote(counter)
        print(ray.get(r))
        counter += 1
        time.sleep(1)
""".format(memory_mb * 1024**2, repr(actor_name))

    def _contains_bundle_resources(all_resource_usage):
        for _, resource_usage in all_resource_usage.items():
            resources_total = resource_usage["resources_total"]
            for label in resources_total.keys():
                if "group" in label:
                    return True

            resources_available = resource_usage["resources_available"]
            for label in resources_available.keys():
                if "group" in label:
                    return True
        return False

    wait_for_condition(lambda: not _contains_bundle_resources(
        state.state.get_all_resource_usage()))
    original_all_resource_usage = state.state.get_all_resource_usage()

    # Now the cluster has 8G memory.
    actor_name_1 = "actor1"
    job_id_1 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_1",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name_1),
        total_memory_mb=4000)
    assert job_id_1 is not None
    print("job_id_1 = {}".format(job_id_1))
    actor1 = _get_named_actor_with_retry(actor_name_1)
    assert actor1 is not None
    assert ray.get(actor1.foo.remote(1)) == 1

    wait_for_condition(lambda: _contains_bundle_resources(
        state.state.get_all_resource_usage()))

    ray.kill(actor1, no_restart=True)
    wait_for_condition(lambda: not _contains_bundle_resources(
        state.state.get_all_resource_usage()))

    all_resource_usage = state.state.get_all_resource_usage()
    assert dict(original_all_resource_usage) == dict(all_resource_usage)


os.environ[
    "ray_config_file"] = "http://alipay-kepler-resource.cn-hangzhou.alipay.aliyun-inc.com/ray-cluster-config-dev/systemjob-test.yaml"  # noqa: E501

os.environ["DISABLE_AUTO_SUBMIT_SYSTEM_JOBS"] = "1"


def test_submit_system_jobs(disable_aiohttp_cache, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    def _send_submit_system_jobs_http_request():
        try:
            resp = requests.post(webui_url +
                                 "/system_jobs/submit_system_jobs_v2")
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            resp = requests.get(webui_url + "/jobs?view=summary")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            summary = result["data"]["summary"]
            assert len(summary) == 2
            execpted_system_jobs = {
                "SYSTEM_JOB_actor-observer", "SYSTEM_JOB_actor-optimizer"
            }
            for system_job in summary:
                job_id = system_job["jobId"]
                resp = requests.get(webui_url + "/jobs/{}".format(job_id))
                resp.raise_for_status()
                result = resp.json()
                assert result["result"] is True, resp.text
                system_job_detail = result["data"]["detail"]["jobInfo"]
                assert system_job_detail["name"] in execpted_system_jobs
                assert system_job_detail["namespaceId"] == "SYSTEM_NAMESPACE"
                assert system_job_detail["nodegroupId"] == "SYSTEM_NAMESPACE"
                assert system_job_detail["isDead"] is False
                assert system_job_detail["metricTemplate"] == "system-actors"
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    assert _send_submit_system_jobs_http_request()


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "job_memory_check_ratio": 1.0,
            "overcommit_ratio": 1.0,
            "enable_job_quota": True
        },
        "_memory": 8 * 1024 * 1024 * 1024
    }],
    indirect=True)
@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_update_flexible_job_memory_requirements(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard,
        downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)
    namespace_id = "default_namespace"
    assert create_namespace(webui_url=webui_url, namespace_id=namespace_id)

    # Now the cluster has 8G memory.
    # Try to submit job_1 with [2000MB, 6000MB] and actually uses 4000MB.
    actor_name_1 = "actor1"
    job_id_1 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_1",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name_1),
        total_memory_mb=2000,
        max_total_memory_mb=6000)
    assert job_id_1 is not None
    print("job_id_1 = {}".format(job_id_1))
    actor1 = _get_named_actor_with_retry(actor_name_1)
    assert actor1 is not None
    assert ray.get(actor1.foo.remote(1)) == 1
    assert _query_job_memory_requirements(job_id_1, webui_url) == (2000 * MB,
                                                                   6000 * MB)

    job_id = ray.JobID(ray._private.utils.hex_to_binary(job_id_1))
    # Update job_1 resource requirements to [3000MB, 3000MB]
    # Actually uses 4000MB.
    # It will be failed as the max < runtime.
    with pytest.raises(Exception):
        assert state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=3000, max_total_memory_mb=3000)
    assert _query_job_memory_requirements(job_id_1, webui_url) == (2000 * MB,
                                                                   6000 * MB)

    # Update job_1 resource requirements to [3000MB, 4000MB]
    # Actually uses 4000MB.
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=3000, max_total_memory_mb=4000)
    assert _query_job_memory_requirements(job_id_1, webui_url) == (3000 * MB,
                                                                   4000 * MB)

    # Try to submit job_2 with [6000MB, 6000MB].
    actor_name_2 = "actor2"
    job_id_2 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_2",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=3000, actor_name=actor_name_2),
        total_memory_mb=6000,
        max_total_memory_mb=6000)
    # It will be failed as the request resources of job_1 is 3000MB now.
    # max(j1.request[3000], j1.runtime[4000]) + j2.request(6000) > 8192
    assert job_id_2 is None

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=2000, max_total_memory_mb=4000)
    # Now the job_1's resource requirements are [2000MB, 4000MB].

    # Try to submit job_2 with [3000MB, 4000MB] again.
    job_id_2 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_2",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=3000, actor_name=actor_name_2),
        total_memory_mb=4000,
        max_total_memory_mb=6000)
    # It's ok to submit job2.
    # max(j1.request[3000], j1.runtime[4000]) + j2.request(4000) < 8192
    assert job_id_2 is not None
    actor2 = _get_named_actor_with_retry(actor_name_2)
    assert actor2 is not None
    assert ray.get(actor2.foo.remote(1)) == 1
    assert _query_job_memory_requirements(job_id_2, webui_url) == (4000 * MB,
                                                                   6000 * MB)

    job_id = ray.JobID(ray._private.utils.hex_to_binary(job_id_2))
    # Update job_2 resource requirements to [5000, 6000]
    # Actually uses 3000.
    # It will be failed.
    # max(j1.request[3000], j1.runtime[4000]) + j2.request(5000) > 8192
    with pytest.raises(Exception):
        state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=5000, max_total_memory_mb=6000)
    assert _query_job_memory_requirements(job_id_2, webui_url) == (4000 * MB,
                                                                   6000 * MB)

    # Update job_2 resource requirements to [1000MB, 2000MB]
    # Actually uses 3000MB.
    # It will be failed as the max < runtime.
    with pytest.raises(Exception):
        state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=1000, max_total_memory_mb=2000)
    assert _query_job_memory_requirements(job_id_2, webui_url) == (4000 * MB,
                                                                   6000 * MB)

    # Update job_2 resource requirements to [2000MB, 6000MB]
    # Actually uses 3000MB.
    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=2000, max_total_memory_mb=6000)
    assert _query_job_memory_requirements(job_id_2, webui_url) == (2000 * MB,
                                                                   6000 * MB)

    # Submit job_3 with [1000MB, 3000MB], actually uses 1000MB.
    actor_name_3 = "actor3"
    job_id_3 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_3",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=1000, actor_name=actor_name_3),
        total_memory_mb=1000,
        max_total_memory_mb=3000)
    assert job_id_3 is not None
    actor3 = _get_named_actor_with_retry(actor_name_3)
    assert actor3 is not None
    assert ray.get(actor3.foo.remote(1)) == 1
    assert _query_job_memory_requirements(job_id_3, webui_url) == (1000 * MB,
                                                                   3000 * MB)

    pending_actor = ray.get(actor3.create_simple_actor.remote(1000 * MB))
    _, remaining = ray.wait([pending_actor.echo.remote(1)], timeout=3)
    assert len(remaining) == 1


@pytest.mark.skipif(
    not gcs_task_scheduling_enabled(),
    reason="This case only works with gcs scheduling enabled.")
@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "job_memory_check_ratio": 1.0,
            "overcommit_ratio": 1.0,
            "job_resource_requirements_max_min_ratio_limit": 5.0,
            "enable_job_quota": True
        },
        "_memory": 8 * 1024 * 1024 * 1024
    }],
    indirect=True)
@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_update_flexible_job_memory_requirements_with_limit(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard,
        downloader):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)
    namespace_id = "default_namespace"
    assert create_namespace(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_resource_requirements_max_min_ratio_limit=5)

    # Now the cluster has 8G memory.
    # Try to submit job_1 with [2000MB, 6000MB] and actually uses 4000MB.
    actor_name_1 = "actor1"
    job_id_1 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_1",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name_1),
        total_memory_mb=1000,
        max_total_memory_mb=6000)
    assert job_id_1 is None

    job_id_1 = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job_1",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name_1),
        total_memory_mb=1000,
        max_total_memory_mb=5000)
    assert job_id_1 is not None
    print("job_id_1 = {}".format(job_id_1))
    actor1 = _get_named_actor_with_retry(actor_name_1)
    assert actor1 is not None
    assert ray.get(actor1.foo.remote(1)) == 1
    assert _query_job_memory_requirements(job_id_1, webui_url) == (1000 * MB,
                                                                   5000 * MB)

    job_id = ray.JobID(ray._private.utils.hex_to_binary(job_id_1))
    # Update job_1 resource requirements to [1000MB, 6000MB].
    # It will be failed as the 6000 / 1000 > 5.0
    with pytest.raises(Exception):
        assert state.update_job_resource_requirements(
            job_id=job_id, total_memory_mb=1000, max_total_memory_mb=6000)
    assert _query_job_memory_requirements(job_id_1, webui_url) == (1000 * MB,
                                                                   5000 * MB)

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=1000, max_total_memory_mb=4000)
    assert _query_job_memory_requirements(job_id_1, webui_url) == (1000 * MB,
                                                                   4000 * MB)

    assert state.update_job_resource_requirements(
        job_id=job_id, total_memory_mb=1000, max_total_memory_mb=5000)
    assert _query_job_memory_requirements(job_id_1, webui_url) == (1000 * MB,
                                                                   5000 * MB)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "include_dashboard": True,
        "_system_config": {
            "job_memory_check_ratio": 1.0,
            "overcommit_ratio": 1.0
        },
        "num_cpus": 4,
        "memory": 8 * 1024**3,
        "resources": {
            "MEM": 8
        }
    }],
    indirect=True)
@pytest.mark.parametrize("downloader", [job_consts.WGET_DOWNLOADER])
def test_runtime_env_leak(disable_aiohttp_cache, enable_test_module,
                          ray_start_cluster_head, downloader):
    cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    print("JOB_ROOT_DIR:", JOB_ROOT_DIR)
    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    namespace_id = "default_namespace"
    assert create_namespace(webui_url=webui_url, namespace_id=namespace_id)
    # Now the cluster has 8G memory.
    # Try to submit job with 5000MB and actually uses 4000MB.
    actor_name = "actor"
    job_id = submit_job(
        webui_url=webui_url,
        namespace_id=namespace_id,
        job_name="job",
        java_dependency_url=_gen_url(webui_url, __file__),
        java_dependency_md5=_gen_md5(__file__),
        downloader=downloader,
        job_code=_gen_job_code(memory_mb=4000, actor_name=actor_name),
        total_memory_mb=5000)
    assert job_id is not None
    print("job_id = {}".format(job_id))
    actor = _get_named_actor_with_retry(actor_name)
    assert actor is not None
    assert ray.get(actor.foo.remote(1)) == 1

    # kill node
    print("kill raylet")
    cluster.list_all_nodes()[0].kill_raylet()

    job_dir = JOB_ROOT_DIR + "/" + job_id
    assert os.path.exists(job_dir)

    # drop job
    print("dropping job ", job_id)
    requests.delete(webui_url + f"/jobs/{job_id}")

    print("ensure job ", job_id, " is dead.")

    def is_job_dead(job_id):
        jobs = ray.state.jobs()
        for job_info in jobs:
            if job_info["JobID"] == job_id:
                return job_info["IsDead"]
        return False

    wait_for_condition(lambda: is_job_dead(job_id), 10)

    print("add a new node")
    cluster.add_node(num_cpus=4, memory=8 * 1024**3, resources={"MEM": 8})
    cluster.wait_for_nodes()

    print("ensure the job dir will be removed.")
    wait_for_condition(lambda: not os.path.exists(job_dir), 10)


def test_add_so_path_to_ld_library_path_env():
    job_info = JobInfo("fake_job_id", {}, "fake_temp_dir", "fake_log_dir")
    p = JobProcessor(job_info)
    ray_api_so_path = os.path.join(get_ray_site_packages_path(), "ray/cpp/lib")
    unpack_dir = "/fake_temp_dir/000/"
    env_dict = job_info.env()
    p._add_so_path_to_ld_library_path_env(env_dict, unpack_dir)
    assert env_dict[
        "LD_LIBRARY_PATH"] == ":" + unpack_dir + ":" + ray_api_so_path


@pytest.mark.parametrize("long_running", [False, True])
def test_cancel_job_after_raylet_fo(
        disable_aiohttp_cache, enable_test_module, fast_reporter_update,
        ray_start_cluster_head_with_dashboard, long_running):
    cluster = ray_start_cluster_head_with_dashboard

    webui_url = cluster.head_node.webui_url
    assert wait_until_server_available(webui_url) is True
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
                    downloader=job_consts.AIOHTTP_DOWNLOADER,
                    long_running=long_running,
                    job_code=TEST_PYTHON_JOB_DUMMY_CODE.format(
                        long_running=long_running)))
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
            # if long_running is True, driver will shutdown
            if long_running:
                assert len(job_workers) == 0
            else:
                assert len(job_workers) > 0
                assert "slsUrl" in job_workers[0]
            assert "slsUrl" in job_info
            assert "eventUrl" in job_info
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_running, timeout=120)
    # Simulate raylet restart
    cluster.head_node.kill_raylet()
    cluster.add_node()

    def get_submitted_job():
        jobs = ray.state.jobs()
        for job in jobs:
            if job["JobID"] == job_id:
                return job
        assert False

    submitted_job = get_submitted_job()

    if long_running:
        time.sleep(5)
        jobs = ray.state.jobs()
        assert len(jobs) == 2
        assert not submitted_job["IsDead"]

    def check_driver():
        try:
            jobs = ray.state.jobs()
            assert len(jobs) == 2, f"jobs number: {len(jobs)}"
            submitted_job = get_submitted_job()
            # drop job
            job_id = submitted_job["JobID"]
            requests.delete(webui_url + f"/jobs/{job_id}")
            assert submitted_job[
                "IsDead"], f"job not died, job id: {submitted_job['JobID']}"
            assert not psutil.pid_exists(
                submitted_job["DriverPid"]), "driver still alive"
            return True
        except Exception as ex:
            print("error:", ex)
            return False

    wait_for_condition(check_driver, timeout=120)


def test_submit_job_with_archive(disable_aiohttp_cache, enable_test_module,
                                 fast_reporter_update,
                                 ray_start_with_dashboard):
    downloader = job_consts.WGET_DOWNLOADER
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)
    fake_jar_md5 = _gen_md5(__file__)

    job_id = None
    a_zip = ("http://raylet.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/"
             "ray_internal_test/dashboard/a.zip")
    b_zip = ("http://raylet.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/"
             "ray_internal_test/dashboard/b.zip")

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url,
                    java_dependency_url=fake_jar_url,
                    java_dependency_md5=fake_jar_md5,
                    downloader=downloader,
                    archive_dependency_urls=[a_zip, b_zip]))
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
    current_job_package_dir = os.path.join(JOB_ROOT_DIR, job_id, "package")

    assert os.path.exists(os.path.join(current_job_package_dir, "a.txt"))
    assert os.stat(os.path.join(current_job_package_dir,
                                "a.txt")).st_nlink == 2

    assert os.path.exists(os.path.join(current_job_package_dir, "a_dir"))

    assert os.path.exists(os.path.join(current_job_package_dir, "b.txt"))
    assert os.stat(os.path.join(current_job_package_dir,
                                "b.txt")).st_nlink == 2

    assert os.path.exists(
        os.path.join(current_job_package_dir, "a_dir", "a_internal.txt"))
    assert os.stat(
        os.path.join(current_job_package_dir, "a_dir",
                     "a_internal.txt")).st_nlink == 2


@pytest.mark.parametrize("downloader", [
    job_consts.AIOHTTP_DOWNLOADER, job_consts.WGET_DOWNLOADER,
    job_consts.DRAGONFLY_DOWNLOADER
])
def test_get_all_jobs(set_dashboard_inactive_jobs_capacity,
                      disable_aiohttp_cache, enable_test_module,
                      fast_reporter_update, ray_start_with_dashboard,
                      downloader):

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    inactive_jobs_capacity = int(
        os.environ["DASHBOARD_INACTIVE_JOBS_CAPACITY"])
    active_job_ids = set()
    # Generate active jobs
    generate_active_jobs_number = randint(1, 4)
    for i in range(generate_active_jobs_number):
        job_id = submit_job(
            webui_url=webui_url,
            namespace_id=RAY_DEFAULT_NODEGROUP,
            job_name="job_{}".format(i),
            java_dependency_url=_gen_url(webui_url, __file__),
            java_dependency_md5=_gen_md5(__file__))
        active_job_ids.add(job_id)

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "FAILED"
            return True
        except Exception as ex:
            logger.info(ex)
        return False

    generate_inactive_jobs_number = inactive_jobs_capacity + 1
    # Generate inactive jobs
    inactive_job_ids = []
    for i in range(generate_inactive_jobs_number):
        job_id = submit_job(
            webui_url=webui_url,
            namespace_id=RAY_DEFAULT_NODEGROUP,
            job_name="inactive_job_{}".format(i))
        wait_for_condition(_check_error, 120)
        inactive_job_ids.append(job_id)

    def _get_job_ids(exclude_inactive: bool = False):
        request_url = webui_url + "/jobs?view=summary"
        if exclude_inactive:
            request_url += "&exclude-inactive=true"
        resp = requests.get(request_url)
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        summary = result["data"]["summary"]
        return {job["jobId"] for job in summary}

    # Retrive all active jobs, should equals to recorded active jobs
    retrived_active_job_ids = _get_job_ids(True)
    assert active_job_ids == retrived_active_job_ids
    # Retrive all jobs, should only keeps limited inactive jobs
    retrived_all_job_ids = _get_job_ids(False)
    assert (retrived_all_job_ids - active_job_ids) == set(
        inactive_job_ids[-inactive_jobs_capacity:])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
