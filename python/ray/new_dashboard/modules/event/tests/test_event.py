import os
import sys
import time
import json
import copy
import logging
import requests
import asyncio
import random
import tempfile
import subprocess

import pytest
import psutil
import numpy as np
import grpc

import ray
from ray import ray_constants
from ray._private.utils import binary_to_hex
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.new_dashboard.modules.event import event_consts
from ray.new_dashboard.modules.event.event_utils import get_external_logger
import ray.new_dashboard.consts as dashboard_consts
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.new_dashboard.modules.event.event_utils import (
    monitor_events, )

logger = logging.getLogger(__name__)


def get_test_event_logger(name, log_file, max_bytes=100 * 1000,
                          backup_count=5):
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger


def get_event(msg="empty message", job_id=None):
    return {
        "event_id": binary_to_hex(np.random.bytes(18)),
        "source_type": random.choice(event_pb2.Event.SourceType.keys()),
        "host_name": "po-dev.inc.alipay.net",
        "pid": random.randint(1, 65536),
        "label": "",
        "message": msg,
        "time_stamp": time.time(),
        "severity": "INFO",
        "custom_fields": {
            "job_id": ray.JobID.from_int(random.randint(1, 100)).hex()
            if job_id is None else job_id,
            "node_id": "",
            "task_id": "",
        }
    }


def test_external_event_logger():
    with tempfile.TemporaryDirectory() as temp_dir:
        logger = get_external_logger(temp_dir)
        logger.info("test_event_logger1")
        external_logfile = os.path.join(
            temp_dir, f"event_{event_consts.EVENT_SOURCE_EXTERNAL}.log")
        with open(external_logfile, "r") as f:
            assert "test_event_logger1" in f.read()
        os.remove(external_logfile)
        logger.info("test_event_logger2")
        with open(external_logfile, "r") as f:
            data = f.read()
            assert "test_event_logger1" not in data
            assert "test_event_logger2" in data


def test_report_external_events(
        disable_aiohttp_cache, enable_test_module, fast_update_agents,
        fast_monitor_events, minimal_event_persistence_logger_count,
        minmize_head_update_internal_seconds, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    sample_events = [get_event(f"external message {i}") for i in range(10)]
    sample_events_job_ids = {
        e["custom_fields"]["job_id"]
        for e in sample_events
    }
    resp = requests.post(f"{webui_url}/events", json=sample_events)
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert "10 external events" in result["msg"]
    resp = requests.post(
        f"{webui_url}/events", json=[get_event("external message")])
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True
    assert "1 external events" in result["msg"]

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            print(sample_events_job_ids, all_events.keys())
            return sample_events_job_ids & all_events.keys(
            ) == sample_events_job_ids
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_events, timeout=5)

    agent_http_port = None

    def _get_agent_port():
        try:
            resp = requests.get(f"{webui_url}/test/dump?key=agents")
            resp.raise_for_status()
            result = resp.json()
            assert len(result["data"]["agents"]) > 0
            nonlocal agent_http_port
            agent_http_port = list(
                result["data"]["agents"].values())[0]["httpPort"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    ip = ray._private.services.get_node_ip_address()
    wait_for_condition(_get_agent_port, timeout=5)
    agent_url = f"http://{ip}:{agent_http_port}"
    sample_events = [get_event(f"external message {i}") for i in range(8, 20)]
    sample_events_job_ids = {
        e["custom_fields"]["job_id"]
        for e in sample_events
    }
    resp = requests.post(f"{agent_url}/agent_events", json=sample_events)
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert "12 external events" in result["msg"]
    resp = requests.post(
        f"{agent_url}/agent_events", json=[get_event("external message")])
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True
    assert "1 external events" in result["msg"]

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            print(all_events.keys(), sample_events_job_ids)
            return sample_events_job_ids & all_events.keys(
            ) == sample_events_job_ids
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_events, timeout=5)

    # Test for failover.
    all_processes = ray.worker._global_node.all_processes
    dashboard_proc_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][
        0]
    dashboard_proc = psutil.Process(dashboard_proc_info.process.pid)

    assert dashboard_proc is not None
    dashboard_proc.kill()
    dashboard_proc.wait()

    # Restart dashboard.
    subprocess.Popen(dashboard_proc_info.command)

    wait_for_condition(_check_events, timeout=10)


def test_check_hs_err_log(
        disable_aiohttp_cache, enable_test_module, fast_update_agents,
        fast_monitor_events, minimal_event_persistence_logger_count,
        minmize_head_update_internal_seconds, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])

    agent_grpc_port = None

    def _get_agent_port():
        try:
            resp = requests.get(f"{webui_url}/test/dump?key=agents")
            resp.raise_for_status()
            result = resp.json()
            assert len(result["data"]["agents"]) > 0
            nonlocal agent_grpc_port
            agent_grpc_port = list(result["data"]["agents"].values())[0][
                dashboard_consts.DASHBOARD_AGENT_GRPC_PORT]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    ip = ray._private.services.get_node_ip_address()
    wait_for_condition(_get_agent_port, timeout=5)

    hs_err_log_sample = """
#
# A fatal error has been detected by the Java Runtime Environment:
#
#  SIGSEGV (0xb) at pc=0x00007fc6a46881fb, pid=100070, tid=0x00007fc686a2b700
#
# JRE version: OpenJDK Runtime Environment (8.0_242-b18) (build 1.8.0_242-b18)
# Java VM: OpenJDK 64-Bit Server VM (25.242-b18 mixed ... oops)
# Problematic frame:
# C  [libjemalloc.so.2+0x51fb]  T.659+0x14b
#
# Failed to write core dump. Core dumps have been disabled. ...
#
# If you would like to submit a bug report, please visit:
#   mailto:jvm@list.alibaba-inc.com
#

---------------  T H R E A D  ---------------

Current thread (0x00007fc6a15c9800):  VMThread [stack: ...] [id=100091]

siginfo: si_signo: 11 (SIGSEGV), si_code: 1 (SEGV_MAPERR), si_addr: ...

Registers:
RAX=0x0000000000000053, RBX=0x00007fc56a2d5000, ...
RSP=0x00007fc686a2aa80, RBP=0x00007fc6a28ef000, ...
R8 =0xfffffffb8e0989f8, R9 =0x00007fc6801bb5f8, ...
R12=0x00007fc6a28f0fe0, R13=0x00007fc6a2a00080, ...
RIP=0x00007fc6a46881fb, EFLAGS=0x0000000000010206, ...
  TRAPNO=0x000000000000000e

Top of Stack: (sp=0x00007fc686a2aa80)
0x00007fc686a2aa80:   00007fc686a2aaf0 00007fc686659000
0x00007fc686a2aa90:   00007fc686a2aaf0 0000000000000000
0x00007fc686a2aaa0:   00007fc686a2ab10 0000000000000000
0x00007fc686a2aab0:   00007fc67cf4bfc0 00007fc67e831890
0x00007fc686a2aac0:   00007fc686a2aadf 00007fc67d7e88c8
0x00007fc686a2aad0:   00007fc6a362a5b8 0000000000000001

Instructions: (pc=0x00007fc6a46881fb)
0x00007fc6a46881db:   7c 24 30 49 8b 4c 24 40 8b 47 08 83 c0 01 41 89
0x00007fc6a46881eb:   c0 89 47 08 8b 45 18 49 c1 e0 03 49 f7 d8 85 c0
0x00007fc6a46881fb:   4a 89 1c 01 0f 8e 31 01 00 00 83 e8 01 89 45 18
0x00007fc6a468820b:   48 8b 5c 24 18 48 8b 6c 24 20 4c 8b 64 24 28 4c

[error occurred during error reporting ...]

Register to memory mapping:

RAX=
[error occurred during error reporting (printing register info), id 0xb]

Stack: [...],  sp=0x00007fc686a2aa80,  free space=1022k
Native frames: (J=compiled Java code, j=interpreted, Vv=VM code, C=native code)
C  [libjemalloc.so.2+0x51fb]  T.659+0x14b

VM_Operation (0x00007fc6a4adb040): Exit, mode: safepoint, ...
"""
    hs_err_log_path = "/tmp/hs_err_test.log"
    with open(hs_err_log_path, "w+") as f:
        f.truncate(0)
        f.write(hs_err_log_sample)

    print(f"grpc address : {ip}:{agent_grpc_port}")
    sample_job_id = "0100"
    sample_job_name = "test name"
    sample_worker_id = "123456789"
    sample_actor_id = "987654321"
    sample_pid = 12345
    sample_ip = "127.0.0.1"
    sample_is_driver = False
    with grpc.insecure_channel(f"{ip}:{agent_grpc_port}") as channel:
        stub = event_pb2_grpc.EventServiceStub(channel)
        request = event_pb2.CheckJavaHsErrLogRequest(
            log_path=hs_err_log_path,
            job_id=sample_job_id,
            job_name=sample_job_name,
            worker_id=sample_worker_id,
            actor_id=sample_actor_id,
            pid=sample_pid,
            ip=sample_ip,
            is_driver=sample_is_driver)
        stub.CheckJavaHsErrLog(request)

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            print(all_events.keys())
            if sample_job_id not in all_events.keys():
                return False
        except Exception as ex:
            logger.info(ex)
            return False

        job_events = all_events[sample_job_id]
        assert len(job_events) == 1
        event = job_events[0]
        print(event)
        assert event["customFields"]["jobId"] == sample_job_id
        assert event["customFields"]["jobName"] == sample_job_name
        assert event["customFields"]["workerId"] == sample_worker_id
        assert event["customFields"]["actorId"] == sample_actor_id
        assert event["customFields"]["pid"] == str(sample_pid)
        assert event["customFields"]["ip"] == sample_ip
        assert event["customFields"]["isDriver"] == str(sample_is_driver)
        assert event["jobId"] == sample_job_id
        assert event["jobName"] == sample_job_name
        assert hs_err_log_sample[0:100] in event["message"]
        return True

    wait_for_condition(_check_events, timeout=10)


def test_per_job_event_limit(
        disable_aiohttp_cache, enable_test_module, fast_update_agents,
        fast_monitor_events, minimal_per_job_event_count,
        minmize_head_update_internal_seconds, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    sample_events = [get_event(f"external message {i}") for i in range(10)]
    job_id = ray.JobID.from_int(100).hex()
    for e in sample_events:
        e["custom_fields"]["job_id"] = job_id
    resp = requests.post(f"{webui_url}/events", json=sample_events)
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert "10 external events" in result["msg"]

    check_count = [0]
    expect_messages = [f"external message {i}" for i in range(5, 10)]

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            # Check the count is is minimal_per_job_event_count
            assert len(all_events[job_id]) == 5
            all_messages = [e["message"] for e in all_events[job_id]]
            # Check the order.
            assert all_messages == expect_messages
            check_count[0] += 1
            if check_count[0] == 2:
                return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_events, timeout=5)


def test_event_message_limit(test_event_report, disable_aiohttp_cache,
                             enable_test_module, fast_update_agents,
                             fast_monitor_events, maximize_per_job_event_count,
                             ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    event_dir = ray_start_with_dashboard["event_dir"]
    job_id = ray.JobID.from_int(100).hex()
    events = []
    # Sample event equals with limit.
    sample_event = get_event("", job_id=job_id)
    message_len = event_consts.READ_LINE_LENGTH_LIMIT - len(
        json.dumps(sample_event))
    for i in range(10):
        sample_event = copy.deepcopy(sample_event)
        sample_event["event_id"] = binary_to_hex(np.random.bytes(18))
        sample_event["message"] = str(i) * message_len
        assert len(
            json.dumps(sample_event)) == event_consts.READ_LINE_LENGTH_LIMIT
        events.append(sample_event)
    # Sample event longer than limit.
    sample_event = copy.deepcopy(sample_event)
    sample_event["event_id"] = binary_to_hex(np.random.bytes(18))
    sample_event["message"] = "2" * (message_len + 1)
    assert len(json.dumps(sample_event)) > event_consts.READ_LINE_LENGTH_LIMIT
    events.append(sample_event)

    for i in range(event_consts.READ_LINE_COUNT_LIMIT):
        events.append(get_event(str(i), job_id=job_id))

    with open(os.path.join(event_dir, "tmp.log"), "w") as f:
        f.writelines([(json.dumps(e) + "\n") for e in events])

    try:
        os.remove(os.path.join(event_dir, "event_GCS.log"))
    except Exception:
        pass
    os.rename(
        os.path.join(event_dir, "tmp.log"),
        os.path.join(event_dir, "event_GCS.log"))

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            assert len(
                all_events[job_id]) >= event_consts.READ_LINE_COUNT_LIMIT + 10
            messages = [e["message"] for e in all_events[job_id]]
            for i in range(10):
                assert str(i) * message_len in messages
            assert "2" * (message_len + 1) not in messages
            assert str(event_consts.READ_LINE_COUNT_LIMIT - 1) in messages
            return True
        except Exception as ex:
            logger.exception(ex)
            return False

    wait_for_condition(_check_events, timeout=15)


@pytest.mark.asyncio
async def test_monitor_events():
    with tempfile.TemporaryDirectory() as temp_dir:
        external = event_pb2.Event.SourceType.Name(event_pb2.Event.EXTERNAL)
        external_log = os.path.join(temp_dir, f"event_{external}.log")
        test_logger = get_test_event_logger(
            __name__ + str(random.random()),
            external_log,
            max_bytes=10,
            backup_count=10)
        read_events = []
        monitor_task = monitor_events(
            temp_dir,
            lambda x: read_events.extend(x),
            scan_interval_seconds=0.01)
        assert not monitor_task.done()
        count = 10

        async def _writer(*args, spin=True):
            for x in range(*args):
                test_logger.info("%s", x)
                if spin:
                    while str(x) not in read_events:
                        await asyncio.sleep(0.01)

        async def _check_events(expect_events, timeout=5):
            start_time = time.time()
            while True:
                sorted_events = sorted(int(i) for i in read_events)
                sorted_events = [str(i) for i in sorted_events]
                if time.time() - start_time > timeout:
                    raise TimeoutError(
                        f"Timeout, read events: {sorted_events}, "
                        f"expect events: {expect_events}")
                if len(sorted_events) == len(expect_events):
                    if sorted_events == expect_events:
                        break
                await asyncio.sleep(1)

        await asyncio.gather(
            _writer(count), _check_events([str(i) for i in range(count)]))

        read_events = []
        monitor_task.cancel()
        monitor_task = monitor_events(
            temp_dir,
            lambda x: read_events.extend(x),
            scan_interval_seconds=0.1)

        await _check_events([str(i) for i in range(count)])

        await _writer(count, count * 2)
        await _check_events([str(i) for i in range(count * 2)])

        log_file_count = len(os.listdir(temp_dir))

        test_logger = get_test_event_logger(
            __name__ + str(random.random()),
            external_log,
            max_bytes=1000,
            backup_count=10)
        assert len(os.listdir(temp_dir)) == log_file_count

        await _writer(count * 2, count * 3, spin=False)
        await _check_events([str(i) for i in range(count * 3)])
        await _writer(count * 3, count * 4, spin=False)
        await _check_events([str(i) for i in range(count * 4)])

        # Test read error
        with open(external_log, "ab") as f:
            f.write(b"\x01\xff\n")

        start_time = time.time()
        while True:
            if "byte 0xff in position 1" in read_events[-1]:
                break
            if time.time() - start_time > 10:
                raise TimeoutError(f"Timeout, read events {read_events}, "
                                   "expect the last one is a traceback")
            await asyncio.sleep(1)

        # Test cancel monitor task.
        monitor_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await monitor_task
        assert monitor_task.done()

        assert len(
            os.listdir(temp_dir)) > 1, "Event log should have rollovers."


def test_event_head_cached_queue_limit(
        disable_aiohttp_cache, enable_test_module, fast_update_agents,
        fast_monitor_events, minimal_head_cache_size,
        ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    event_dir = ray_start_with_dashboard["event_dir"]
    job_id = ray.JobID.from_int(100).hex()
    sample_event = get_event("", job_id=job_id)
    message_len = 10

    check_count = [0]

    def _check_events(sample_event=sample_event):
        try:
            sample_event = copy.deepcopy(sample_event)
            sample_event["event_id"] = binary_to_hex(np.random.bytes(18))
            sample_event["message"] = "#" * (message_len + 2)
            with open(os.path.join(event_dir, "event_GCS.log"), "a") as f:
                f.write(json.dumps(sample_event) + "\n")

            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            event_num = len(all_events.get(job_id, []))
            resp = requests.get(f"{webui_url}/events/cache_size")
            resp.raise_for_status()
            result = resp.json()
            cache_size = result["data"]["cacheSize"]
            assert event_num > 1
            # Have set EVENT_HEAD_CACHE_SIZE=1 in minimal_head_cache_size
            assert cache_size == 1
            check_count[0] += 1
            if check_count[0] == 3:
                return True
        except Exception as ex:
            logger.exception(ex)
            return False

    wait_for_condition(_check_events, timeout=20)


@pytest.mark.asyncio
async def test_monitor_events_limit(minimal_head_cache_size,
                                    minmize_head_update_internal_seconds):
    with tempfile.TemporaryDirectory() as temp_dir:
        external = event_pb2.Event.SourceType.Name(event_pb2.Event.EXTERNAL)
        external_log = os.path.join(temp_dir, f"event_{external}.log")
        test_logger = get_test_event_logger(
            __name__ + str(random.random()),
            external_log,
            max_bytes=10,
            backup_count=10)
        read_events = []
        monitor_events(
            temp_dir,
            lambda x: read_events.extend(x),
            condition_func=lambda: len(read_events) < 1,
            scan_interval_seconds=0.01)

        async def _writer(content, timeout=3):
            start_time = time.time()
            while True:
                test_logger.info("%s", content)
                await asyncio.sleep(0.1)
                if time.time() - start_time > timeout:
                    break

        await asyncio.gather(_writer(1), asyncio.sleep(5))

        assert len(read_events) == 1


def test_display_level(disable_aiohttp_cache, enable_test_module,
                       fast_update_agents, fast_monitor_events,
                       minimal_event_persistence_logger_count,
                       minmize_head_update_internal_seconds,
                       set_event_display_level_info, ray_start_with_dashboard):
    def _check_events(expect_severity):
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            display_severity = result["data"]["displaySeverity"]
            print(display_severity)
            return display_severity == expect_severity
        except Exception as ex:
            logger.info(ex)
            return False

    os.environ[event_consts.EVENT_DISPLAY_LEVEL_ENV_NAME] = "error"
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])

    assert _check_events("INFO,WARNING,ERROR,FATAL")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
