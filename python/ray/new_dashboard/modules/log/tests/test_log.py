import os
import sys
import logging
import requests
import time
import traceback
import html.parser
import urllib.parse

from ray.new_dashboard.tests.conftest import *  # noqa
import pytest
import ray
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
)

logger = logging.getLogger(__name__)


class LogUrlParser(html.parser.HTMLParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._urls = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._urls.append(dict(attrs)["href"])

    def error(self, message):
        logger.error(message)

    def get_urls(self):
        return self._urls


def test_log(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    def write_log(s):
        print(s)

    test_log_text = "test_log_text"
    ray.get(write_log.remote(test_log_text))
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    timeout_seconds = 10
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/log_index")
            response.raise_for_status()
            parser = LogUrlParser()
            parser.feed(response.text)
            all_nodes_log_urls = parser.get_urls()
            assert len(all_nodes_log_urls) == 1

            response = requests.get(all_nodes_log_urls[0])
            response.raise_for_status()
            parser = LogUrlParser()
            parser.feed(response.text)

            # Search test_log_text from all worker logs.
            parsed_url = urllib.parse.urlparse(all_nodes_log_urls[0])
            paths = parser.get_urls()
            urls = []
            for p in paths:
                if "worker" in p:
                    urls.append(parsed_url._replace(path=p).geturl())

            for u in urls:
                response = requests.get(u)
                response.raise_for_status()
                if test_log_text in response.text:
                    break
            else:
                raise Exception(f"Can't find {test_log_text} from {urls}")

            # Test range request.
            response = requests.get(
                webui_url + "/logs/dashboard.log",
                headers={"Range": "bytes=49-63"})
            response.raise_for_status()
            assert response.text == "GRPC_FORCE_IPV4"

            # Test logUrl in node info.
            response = requests.get(webui_url + f"/nodes/{node_id}")
            response.raise_for_status()
            node_info = response.json()
            assert node_info["result"] is True
            node_info = node_info["data"]["detail"]
            assert "logUrl" in node_info
            assert node_info["logUrl"] in all_nodes_log_urls
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


def test_log_proxy(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    log_dir = ray_start_with_dashboard["logs_dir"]
    binary_file = os.path.join(log_dir, "test_binary_file")
    test_binary_data = b"0xffccdd" * 10

    with open(binary_file, "wb") as f:
        f.write(test_binary_data)

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            # Test range request.
            response = requests.get(
                f"{webui_url}/log_proxy?url={webui_url}/logs/dashboard.log",
                headers={"Range": "bytes=49-63"})
            response.raise_for_status()
            assert response.text == "GRPC_FORCE_IPV4"
            # Test proxy binary file.
            response = requests.get(
                f"{webui_url}/log_proxy?url={webui_url}/logs/test_binary_file")
            response.raise_for_status()
            assert "octet-stream" in response.headers["content-type"]
            assert response.text == test_binary_data.decode("ascii")
            # Test 404.
            response = requests.get(f"{webui_url}/log_proxy?"
                                    f"url={webui_url}/logs/not_exist_file.log")
            assert response.status_code == 404
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


def test_clean_log(set_dashboard_agent_log_clean_configs,
                   ray_start_with_dashboard,
                   set_running_job_for_agent_log_clean):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    # Wait for all system files to be created
    time.sleep(3)
    from ray.new_dashboard.modules.log.log_consts import \
        RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS, RAY_LOGS_TTL_SECONDS, \
        RAY_MAXIMUM_NUMBER_OF_LOGS_TO_KEEP

    log_dir = ray_start_with_dashboard["logs_dir"]
    event_dir = os.path.join(log_dir, "events")
    log_files_before_stub = \
        {os.path.join(log_dir, file) for file in os.listdir(log_dir)}.union(
            {os.path.join(event_dir, file) for file in os.listdir(event_dir)}
        )

    test_binary_data = b"0xffccdd" * 10
    logs_should_be_removed = []
    for job_id in ["00100080", "00c00080"]:
        driver_log = os.path.join(log_dir, "driver-{}.log".format(job_id))
        driver_out = os.path.join(log_dir, "driver-{}.out".format(job_id))
        for file in [driver_log, driver_out]:
            with open(file, "wb") as f:
                f.write(test_binary_data)
            logs_should_be_removed.append(file)
    # To guarantee file modify order
    # whitelist worker logs should be removed even without
    # corresponding core worker log
    jvm_log = os.path.join(log_dir, "jvm_gc_pid{}.log".format(205))
    core_worker_event = os.path.join(event_dir,
                                     "event_CORE_WORKER_{}.log".format(206))
    for file in [jvm_log, core_worker_event]:
        with open(file, "wb") as f:
            f.write(test_binary_data)
        logs_should_be_removed.append(file)
    time.sleep(0.1)

    def stub_log_files_for_job(job_id, pids):
        for pid in pids:
            worker_log = os.path.join(log_dir, "anyprefix-{}.log".format(pid))
            worker_out = os.path.join(log_dir, "worker-a-{}.2.out".format(pid))
            worker_err = os.path.join(log_dir, "someprefix_{}.err".format(pid))
            core_worker_log = os.path.join(
                log_dir, "python-core-worker-{}_{}.log".format(job_id, pid))
            worker_event = os.path.join(event_dir, "WORKER-{}.log".format(pid))
            test_binary_data = b"0xffccdd" * 10
            for file in [
                    worker_log, worker_out, worker_err, core_worker_log,
                    worker_event
            ]:
                with open(file, "wb") as f:
                    f.write(test_binary_data)
                    logs_should_be_removed.append(file)
            time.sleep(0.1)

    # This job's worker log won't exceed single job log number limit
    stub_log_files_for_job("0d00", [199, 200])
    # The first two group should be removed due to per job worker log limit
    stub_log_files_for_job("0c00", [201, 202, 203, 204])

    logs_should_be_excluded_from_cleaning = set()
    # Logs with similar pattern but not related to a core worker is excluded
    system_generated_log = os.path.join(log_dir, "redis-{}.log".format(99))
    logs_should_be_excluded_from_cleaning.add(system_generated_log)
    # Worker log whose process is still active should not be removed
    current_pid = os.getpid()
    running_worker_log = \
        os.path.join(log_dir, "worker-f2a-{}.out".format(current_pid))
    logs_should_be_excluded_from_cleaning.add(running_worker_log)
    # Driver log whose job hasn't finished should not be removed
    mocked_job_id = set_running_job_for_agent_log_clean["mocked_job_id"]
    running_job_driver_log = \
        os.path.join(log_dir, "driver-{}.log".format(mocked_job_id))
    logs_should_be_excluded_from_cleaning.add(running_job_driver_log)
    for log_file in logs_should_be_excluded_from_cleaning:
        with open(log_file, "wb") as f:
            f.write(test_binary_data)

    log_files_after_stub = \
        {os.path.join(log_dir, file) for file in os.listdir(log_dir)}.union(
            {os.path.join(event_dir, file) for file in os.listdir(event_dir)}
        )
    # Logs after stub should include initial logs plus stubbed logs
    assert log_files_after_stub == log_files_before_stub.union(
        logs_should_be_excluded_from_cleaning, set(logs_should_be_removed))

    # To test cleaning by number, this test needs the following two conditions
    assert RAY_MAXIMUM_NUMBER_OF_LOGS_TO_KEEP < len(logs_should_be_removed)
    assert RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS < RAY_LOGS_TTL_SECONDS
    # Wait for first cleaning
    time.sleep(RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS)
    log_files_after_first_clean = \
        {os.path.join(log_dir, file) for file in os.listdir(log_dir)}.union(
            {os.path.join(event_dir, file) for file in os.listdir(event_dir)}
        )
    # Logs excluded should remain in the file
    assert logs_should_be_excluded_from_cleaning.issubset(
        log_files_after_first_clean)
    # Logs exceeded number threshold should be removed
    logs_exceeds_per_job_number_limit = set(logs_should_be_removed[-20:-10])
    assert logs_exceeds_per_job_number_limit.isdisjoint(
        log_files_after_first_clean)
    logs_exceeds_overall_number_limit = set(logs_should_be_removed[:11])
    assert logs_exceeds_overall_number_limit.isdisjoint(
        log_files_after_first_clean)
    # Logs below number threshold should be kept
    logs_should_remain_after_first_clean = (
        set(logs_should_be_removed) -
        logs_exceeds_per_job_number_limit) - logs_exceeds_overall_number_limit
    assert logs_should_remain_after_first_clean.issubset(
        log_files_after_first_clean)

    # Wait for time based cleaning to take effect
    time.sleep(RAY_LOGS_TTL_SECONDS)
    log_files_after_second_clean = \
        {os.path.join(log_dir, file) for file in os.listdir(log_dir)}.union(
            {os.path.join(event_dir, file) for file in os.listdir(event_dir)}
        )
    assert logs_should_be_excluded_from_cleaning.issubset(
        log_files_after_second_clean)
    assert set(logs_should_be_removed).isdisjoint(log_files_after_second_clean)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
