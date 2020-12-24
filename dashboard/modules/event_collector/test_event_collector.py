import sys
import time
import logging
import requests
import asyncio
import random
import tempfile

import psutil
import numpy as np

from ray import ray_constants
from ray.utils import binary_to_hex
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.core.generated import event_pb2
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.new_dashboard.modules.event_collector.event_collector_utils import (
    setup_persistence_event_logger,
    monitor_events,
    monitor_events2,
    TailFile,
    get_last_event_strings,
)

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def get_event(msg="empty message"):
    return {
        "eventId": binary_to_hex(np.random.bytes(18)),
        "sourceType": random.choice(event_pb2.Event.SourceType.keys()),
        "sourceHostname": "po-dev.inc.alipay.net",
        "sourcePid": random.randint(1, 65536),
        "label": "",
        "message": msg,
        "timestamp": time.time(),
        "jobId": ray.JobID.from_int(random.randint(1, 100)).hex(),
        "nodeId": "",
        "taskId": "",
        "severity": "INFO"
    }


def test_report_external_events(disable_aiohttp_cache, enable_test_module,
                                fast_update_agents, minimal_event_persistence_logger_count,
                                ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    sample_events = [get_event(f"external message {i}") for i in range(10)]
    sample_events_job_ids = set([e["jobId"] for e in sample_events])
    resp = requests.post(f"{webui_url}/events", json=sample_events)
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert "10 external events" in result["msg"]
    resp = requests.post(
        f"{webui_url}/events", json=[get_event(f"external message")])
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
            return sample_events_job_ids & all_events.keys(
            ) == sample_events_job_ids
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_events, timeout=2)

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

    wait_for_condition(_get_agent_port, timeout=5)
    agent_url = ":".join(webui_url.split(":")[:-1] + [str(agent_http_port)])
    sample_events = [get_event(f"external message {i}") for i in range(8, 20)]
    sample_events_job_ids = set([e["jobId"] for e in sample_events])
    resp = requests.post(f"{agent_url}/agent_events", json=sample_events)
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert "12 external events" in result["msg"]
    resp = requests.post(
        f"{agent_url}/agent_events", json=[get_event(f"external message")])
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


def test_tail_file():
    loop = asyncio.get_event_loop()

    read_data = []
    # Invalid file should raise FileNotFoundError.
    with pytest.raises(FileNotFoundError):
        TailFile(
            "not exists file",
            lambda x: read_data.append(x),
            whence=os.SEEK_SET)

    assert len(read_data) == 0

    # The last line should be end with \n,
    # else the line will not be read until EOF.
    sample_data = b"init\n"

    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(sample_data)
        filename = f.name

    # Invalid pos should raise OSError
    with pytest.raises(OSError):
        TailFile(
            filename,
            lambda x: read_data.extend(x),
            pos=-1,
            whence=os.SEEK_SET)

    # Read starts at 0, SEEK_SET
    tail_file = TailFile(
        filename, lambda x: read_data.extend(x), whence=os.SEEK_SET)
    assert tail_file.begin_position == 0

    start_time = time.time()

    async def _check_events():
        while True:
            if time.time() - start_time > 5:
                raise TimeoutError(f"Timeout, read {read_data}.")
            if len(read_data) == 1 and \
                read_data[0] == sample_data.decode().strip():
                break
            await asyncio.sleep(1)

    loop.run_until_complete(_check_events())
    tail_file.cancel()
    assert len(read_data) == 1 and read_data[0] == sample_data.decode().strip()

    # Read starts at 2, SEEK_SET
    read_data = []
    tail_file = TailFile(
        filename, lambda x: read_data.extend(x), pos=2, whence=os.SEEK_SET)
    assert tail_file.begin_position == 0

    start_time = time.time()

    async def _check_events():
        while True:
            if time.time() - start_time > 5:
                raise TimeoutError(f"Timeout, read {read_data}.")
            if len(read_data) == 1 and \
                read_data[0] == sample_data.decode().strip():
                break
            await asyncio.sleep(1)

    loop.run_until_complete(_check_events())
    assert len(read_data) == 1 and read_data[0] == sample_data.decode().strip()

    # Read starts at 5, SEEK_SET
    sample_data = b"init\nall\n"

    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(sample_data)
        filename = f.name

    read_data = []
    tail_file = TailFile(
        filename, lambda x: read_data.extend(x), pos=5, whence=os.SEEK_SET)
    assert tail_file.begin_position == 5

    start_time = time.time()

    async def _check_events():
        while True:
            if time.time() - start_time > 5:
                raise TimeoutError(f"Timeout, read {read_data}.")
            if len(read_data) == 1 and read_data[0] == "all":
                break
            await asyncio.sleep(1)

    loop.run_until_complete(_check_events())
    assert len(read_data) == 1 and read_data[0] == "all"

    # Read starts at -1, SEEK_END
    read_data = []
    tail_file = TailFile(
        filename, lambda x: read_data.extend(x), pos=-1, whence=os.SEEK_END)
    assert tail_file.begin_position == 5

    async def _check_events():
        while True:
            if time.time() - start_time > 5:
                raise TimeoutError(f"Timeout, read {read_data}.")
            if len(read_data) == 1 and read_data[0] == "all":
                break
            await asyncio.sleep(1)

    loop.run_until_complete(_check_events())
    assert len(read_data) == 1 and read_data[0] == "all"


def test_monitor_events():
    with tempfile.TemporaryDirectory() as temp_dir:
        external = event_pb2.Event.SourceType.Name(event_pb2.Event.PERSISTENCE)
        external_log = os.path.join(temp_dir, f"event_{external}.log")
        test_logger = setup_persistence_event_logger(
            __name__ + str(random.random()),
            external_log,
            max_bytes=10,
            backup_count=10)
        all_events = []
        co = monitor_events(
            temp_dir,
            lambda x: all_events.extend(x),
            scan_interval_seconds=0.1,
            incremental_only=False)
        loop = asyncio.get_event_loop()
        monitor_info = loop.run_until_complete(co)
        # The log file is empty.
        r = loop.run_until_complete(
            get_last_event_strings(temp_dir, 10,
                                   monitor_info.initialized_files))
        assert r[external] == ""
        assert not monitor_info.task.done()
        count = 10

        async def _write_events():
            for x in range(count):
                test_logger.info("%s", x)
                while str(x) not in all_events:
                    await asyncio.sleep(0.01)

        loop.create_task(_write_events())
        start_time = time.time()

        async def _check_events():
            while True:
                if time.time() - start_time > 5:
                    raise TimeoutError(
                        f"Timeout, read event count: {len(all_events)}")
                if len(all_events) == count:
                    break
                await asyncio.sleep(1)

        loop.run_until_complete(_check_events())

        # The files has been rotated, the initialized pos has been rotated out.
        r = loop.run_until_complete(
            get_last_event_strings(temp_dir, 10,
                                   monitor_info.initialized_files))
        assert r[external] == ""

        # Read tail 10 bytes, it's
        # 5\n6\n7\n8\n9\n
        # but, we have to align to line, so cut from the first line sep
        # 6\n7\n8\n9\n
        r = loop.run_until_complete(get_last_event_strings(temp_dir, 10))
        assert r[external] == "6\n7\n8\n9\n"

        all_events = []
        monitor_info.task.cancel()
        co = monitor_events(
            temp_dir,
            lambda x: all_events.extend(x),
            scan_interval_seconds=0.1,
            incremental_only=False)
        monitor_info = loop.run_until_complete(co)
        assert all(fid.begin_position == 0
                   for fid in monitor_info.initialized_files.values())
        monitor_info.task.cancel()
        assert sorted(all_events) == [str(i) for i in range(count)]

        all_events = []
        monitor_info.task.cancel()
        co = monitor_events(
            temp_dir,
            lambda x: all_events.extend(x),
            scan_interval_seconds=0.1,
            incremental_only=True)
        monitor_info = loop.run_until_complete(co)
        assert all(fid.begin_position != 0
                   for fid in monitor_info.initialized_files.values())
        monitor_info.task.cancel()
        assert len(all_events) == 0

        r = loop.run_until_complete(
            get_last_event_strings(temp_dir, 10,
                                   monitor_info.initialized_files))
        assert r[external] == "6\n7\n8\n9\n"

        assert len(
            os.listdir(temp_dir)) > 1, "Event log should have rollovers."


def test_monitor_events2():
    loop = asyncio.get_event_loop()
    with tempfile.TemporaryDirectory() as temp_dir:
        external = event_pb2.Event.SourceType.Name(event_pb2.Event.PERSISTENCE)
        external_log = os.path.join(temp_dir, f"event_{external}.log")
        test_logger = setup_persistence_event_logger(
            __name__ + str(random.random()),
            external_log,
            max_bytes=10,
            backup_count=10)
        all_events = []
        monitor_task = monitor_events2(
            temp_dir,
            lambda x: all_events.extend(x),
            scan_interval_seconds=0.1,
            incremental_only=False)
        # The log file is empty.
        r = loop.run_until_complete(get_last_event_strings(temp_dir, 10))
        assert r[external] == ""
        assert not monitor_task.done()
        count = 10

        async def _write_events():
            for x in range(count):
                test_logger.info("%s", x)
                while str(x) not in all_events:
                    await asyncio.sleep(0.01)

        loop.create_task(_write_events())
        start_time = time.time()

        async def _check_events():
            while True:
                if time.time() - start_time > 5000:
                    raise TimeoutError(
                        f"Timeout, read event count: {len(all_events)}")
                if len(all_events) == count:
                    break
                await asyncio.sleep(1)

        loop.run_until_complete(_check_events())

        # Read tail 10 bytes, it's
        # 5\n6\n7\n8\n9\n
        # but, we have to align to line, so cut from the first line sep
        # 6\n7\n8\n9\n
        r = loop.run_until_complete(get_last_event_strings(temp_dir, 10))
        assert r[external] == "6\n7\n8\n9\n"

        all_events = []
        monitor_task.cancel()
        monitor_task = monitor_events2(
            temp_dir,
            lambda x: all_events.extend(x),
            scan_interval_seconds=0.1,
            incremental_only=False)

        loop.run_until_complete(_check_events())
        assert sorted(all_events) == [str(i) for i in range(count)]

        all_events = []
        monitor_task.cancel()
        monitor_task = monitor_events2(
            temp_dir,
            lambda x: all_events.extend(x),
            scan_interval_seconds=0.1,
            incremental_only=True)
        monitor_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            loop.run_until_complete(monitor_task)
        assert monitor_task.done()
        assert len(all_events) == 0

        assert len(
            os.listdir(temp_dir)) > 1, "Event log should have rollovers."


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
