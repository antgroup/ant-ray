import logging
import os
import random
import sys
import threading
import time
import traceback
import types
from datetime import datetime, timedelta

import pytest
import requests

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.consts import RAY_DASHBOARD_STATS_UPDATING_INTERVAL
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


def test_nodes_update(enable_test_module, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 10
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/test/dump")
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["nodes"]) == 1
            break

        except (AssertionError, requests.exceptions.ConnectionError):
            logger.exception("Retry")
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


def test_node_info(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            print(f"actor pid={os.getpid()}")
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = set(ray.get(actor_pids))

    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    # NOTE: Leaving sum buffer time for data to get refreshed
    timeout_seconds = RAY_DASHBOARD_STATS_UPDATING_INTERVAL * 1.5

    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/nodes?view=hostnamelist")
            response.raise_for_status()
            hostname_list = response.json()
            assert hostname_list["result"] is True, hostname_list["msg"]
            hostname_list = hostname_list["data"]["hostNameList"]
            assert len(hostname_list) == 1

            hostname = hostname_list[0]
            response = requests.get(webui_url + f"/nodes/{node_id}")
            response.raise_for_status()
            detail = response.json()
            assert detail["result"] is True, detail["msg"]
            detail = detail["data"]["detail"]
            assert detail["hostname"] == hostname
            assert detail["raylet"]["state"] == "ALIVE"
            assert detail["raylet"]["isHeadNode"] is True
            assert "raylet" in detail["cmdline"][0]
            assert len(detail["workers"]) >= 2
            assert len(detail["actors"]) == 2, detail["actors"]

            actor_worker_pids = set()
            for worker in detail["workers"]:
                if "ray::Actor" in worker["cmdline"][0]:
                    actor_worker_pids.add(worker["pid"])
            assert actor_worker_pids == actor_pids

            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status()
            summary = response.json()
            assert summary["result"] is True, summary["msg"]
            assert len(summary["data"]["summary"]) == 1
            summary = summary["data"]["summary"][0]
            assert summary["hostname"] == hostname
            assert summary["raylet"]["state"] == "ALIVE"
            assert "raylet" in summary["cmdline"][0]
            assert "workers" not in summary
            assert "actors" not in summary
            assert "objectStoreAvailableMemory" in summary["raylet"]
            assert "objectStoreUsedMemory" in summary["raylet"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = (
                    traceback.format_exception(
                        type(last_ex), last_ex, last_ex.__traceback__
                    )
                    if last_ex
                    else []
                )
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
            "_system_config": {
                "health_check_initial_delay_ms": 0,
                "health_check_timeout_ms": 100,
                "health_check_failure_threshold": 3,
                "health_check_period_ms": 100,
            },
        }
    ],
    indirect=True,
)
def test_multi_nodes_info(
    enable_test_module, disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    cluster.add_node()
    cluster.add_node()
    dead_node = cluster.add_node()
    cluster.remove_node(dead_node, allow_graceful=False)

    def _check_nodes():
        try:
            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status()
            summary = response.json()
            assert summary["result"] is True, summary["msg"]
            summary = summary["data"]["summary"]
            assert len(summary) == 4
            for node_info in summary:
                node_id = node_info["raylet"]["nodeId"]
                response = requests.get(webui_url + f"/nodes/{node_id}")
                response.raise_for_status()
                detail = response.json()
                assert detail["result"] is True, detail["msg"]
                detail = detail["data"]["detail"]
                if node_id != dead_node.node_id:
                    assert detail["raylet"]["state"] == "ALIVE"
                else:
                    assert detail["raylet"]["state"] == "DEAD"
                    assert detail["raylet"].get("objectStoreAvailableMemory", 0) == 0
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_nodes, timeout=15)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{"include_dashboard": True}], indirect=True
)
def test_multi_node_churn(
    enable_test_module, disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = format_web_url(cluster.webui_url)

    success = True

    def verify():
        nonlocal success
        while True:
            try:
                resp = requests.get(webui_url)
                resp.raise_for_status()
                resp = requests.get(webui_url + "/nodes?view=summary")
                resp.raise_for_status()
                summary = resp.json()
                assert summary["result"] is True, summary["msg"]
                assert summary["data"]["summary"]
                time.sleep(1)
            except Exception:
                success = False
                break

    t = threading.Thread(target=verify, daemon=True)
    t.start()

    t_st = datetime.now()
    duration = timedelta(seconds=60)
    worker_nodes = []
    while datetime.now() < t_st + duration:
        time.sleep(5)
        if len(worker_nodes) < 2:
            worker_nodes.append(cluster.add_node())
            continue
        should_add_node = random.randint(0, 1)
        if should_add_node:
            worker_nodes.append(cluster.add_node())
        else:
            node_index = random.randrange(0, len(worker_nodes))
            node_to_remove = worker_nodes.pop(node_index)
            cluster.remove_node(node_to_remove)

    assert success


@pytest.mark.skipif(
    sys.platform == "win32", reason="setproctitle does not change psutil.cmdline"
)
def test_node_physical_stats(enable_test_module, shutdown_only):
    """
    Tests NodeHead._update_node_physical_stats.
    """
    addresses = ray.init(include_dashboard=True, num_cpus=6)

    @ray.remote(num_cpus=1)
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote() for _ in range(6)]
    actor_pids = ray.get([actor.getpid.remote() for actor in actors])
    actor_pids = set(actor_pids)

    webui_url = addresses["webui_url"]
    assert wait_until_server_available(webui_url) is True
    webui_url = format_web_url(webui_url)

    def _check_workers():
        try:
            resp = requests.get(webui_url + "/test/dump?key=node_physical_stats")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            node_physical_stats = result["data"]["nodePhysicalStats"]
            assert len(node_physical_stats) == 1
            current_stats = node_physical_stats[addresses["node_id"]]
            # Check Actor workers
            current_actor_pids = set()
            for worker in current_stats["workers"]:
                if "ray::Actor" in worker["cmdline"][0]:
                    current_actor_pids.add(worker["pid"])
            assert current_actor_pids == actor_pids
            # Check raylet cmdline
            assert "raylet" in current_stats["cmdline"][0]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_workers, timeout=10)


def test_worker_pids_reported(enable_test_module, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.32.5"]}})
    class UvActor:
        def get_pid(self):
            return os.getpid()

    uv_actor = UvActor.remote()
    uv_actor_pid = ray.get(uv_actor.get_pid.remote())
    driver_pid = os.getpid()

    def _check_worker_pids():
        try:
            response = requests.get(webui_url + f"/nodes/{node_id}")
            response.raise_for_status()
            dump_info = response.json()
            assert dump_info["result"] is True
            detail = dump_info["data"]["detail"]
            pids = [worker["pid"] for worker in detail["workers"]]
            assert len(pids) >= 2  # might include idle worker
            assert uv_actor_pid in pids
            assert driver_pid in pids
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_worker_pids, timeout=20)


@pytest.mark.asyncio
async def test_node_head_get_nodes_logical_resources_autoscaler_v2_smoke(monkeypatch):
    """Smoke test autoscaler v2 branch for NameError regressions (e.g. parse_usage)."""
    from ray.dashboard.modules.node.node_head import NodeHead
    from ray.dashboard.subprocesses.module import SubprocessModuleConfig

    # Force autoscaler v2 branch.
    import ray.autoscaler.v2.utils as autoscaler_v2_utils

    monkeypatch.setattr(autoscaler_v2_utils, "is_autoscaler_v2", lambda: True)

    # Stub parser to avoid depending on real reply schema.
    import ray.autoscaler.v2.sdk as autoscaler_v2_sdk

    class _Usage:
        def __init__(self, resource_name, used, total):
            self.resource_name = resource_name
            self.used = used
            self.total = total

    class _Node:
        def __init__(self, node_id):
            self.node_id = node_id
            self.resource_usage = types.SimpleNamespace(
                usage=[_Usage("CPU", 1.0, 4.0)]
            )

    dummy_status = types.SimpleNamespace(active_nodes=[_Node("n1")], idle_nodes=[])
    monkeypatch.setattr(
        autoscaler_v2_sdk.ClusterStatusParser,
        "from_get_cluster_status_reply",
        staticmethod(lambda reply, stats=None: dummy_status),
    )

    # Ensure parse_usage exists and returns an iterable of strings.
    import ray.autoscaler._private.util as autoscaler_util

    monkeypatch.setattr(autoscaler_util, "parse_usage", lambda d, verbose=True: ["ok"])

    class _StubGcsClient:
        async def async_get_cluster_status(self):
            return object()

    config = SubprocessModuleConfig(
        cluster_id_hex="00" * 28,
        gcs_address="127.0.0.1:6379",
        session_name="session",
        temp_dir="/tmp",
        session_dir="/tmp/session",
        logging_level="INFO",
        logging_format="%(message)s",
        log_dir="/tmp",
        logging_filename="dashboard.log",
        logging_rotate_bytes=1024 * 1024,
        logging_rotate_backup_count=1,
        socket_dir="/tmp",
    )
    head = NodeHead(config)
    head._gcs_client = _StubGcsClient()

    result = await head.get_nodes_logical_resources()
    assert result == {"n1": "ok"}

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
