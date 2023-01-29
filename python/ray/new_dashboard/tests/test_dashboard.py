import os
import sys
import copy
import json
import jsonschema
import time
import logging
import asyncio
import ipaddress
import subprocess
import collections

import async_timeout
import numpy as np
import aiohttp.web
import ray
import pprint
import psutil
import pytest
import redis
import requests
import grpc.aio as aiogrpc

from ray import ray_constants
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
    wait_until_succeeded_without_exception,
    run_string_as_driver,
)
from ray.new_dashboard import dashboard
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()


def cleanup_test_files():
    module_path = ray.new_dashboard.modules.__path__[0]
    filename = os.path.join(module_path, "test_for_bad_import.py")
    logger.info("Remove test file: %s", filename)
    try:
        os.remove(filename)
    except Exception:
        pass


def prepare_test_files():
    module_path = ray.new_dashboard.modules.__path__[0]
    filename = os.path.join(module_path, "test_for_bad_import.py")
    logger.info("Prepare test file: %s", filename)
    with open(filename, "w") as f:
        f.write(">>>")


cleanup_test_files()


def _search_agent(processes):
    for p in processes:
        try:
            for c in p.cmdline():
                if "new_dashboard/agent.py" in c:
                    return p
        except Exception:
            pass


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "agent_register_timeout_ms": 5000
        }
    }],
    indirect=True)
def test_basic(ray_start_with_dashboard):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    address_info = ray_start_with_dashboard
    node_id = address_info["node_id"]
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    client = redis.StrictRedis(
        host=address[0],
        port=int(address[1]),
        password=ray_constants.REDIS_DEFAULT_PASSWORD)

    all_processes = ray.worker._global_node.all_processes
    assert ray_constants.PROCESS_TYPE_DASHBOARD in all_processes
    assert ray_constants.PROCESS_TYPE_REPORTER not in all_processes
    dashboard_proc_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][
        0]
    dashboard_proc = psutil.Process(dashboard_proc_info.process.pid)
    assert dashboard_proc.status() in [
        psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING
    ]
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    # Test for bad imports, the agent should be restarted.
    logger.info("Test for bad imports.")
    agent_proc = _search_agent(raylet_proc.children())
    prepare_test_files()
    agent_pids = set()
    try:
        assert agent_proc is not None
        agent_proc.kill()
        agent_proc.wait()
        # The agent will be restarted for imports failure.
        for x in range(100):
            agent_proc = _search_agent(raylet_proc.children())
            if agent_proc:
                agent_pids.add(agent_proc.pid)
            # The agent should be restarted,
            # so we can break if the len(agent_pid) > 1
            if len(agent_pids) > 1:
                break
            time.sleep(0.1)
    finally:
        cleanup_test_files()
    assert len(agent_pids) > 1, agent_pids

    agent_proc = _search_agent(raylet_proc.children())
    if agent_proc:
        agent_proc.kill()
        agent_proc.wait()

    logger.info("Test agent register is OK.")
    wait_for_condition(lambda: _search_agent(raylet_proc.children()))
    assert dashboard_proc.status() in [
        psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING
    ]
    agent_proc = _search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    # Check if agent register is OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = _search_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)

    # Check redis keys are set.
    logger.info("Check redis keys are set.")
    dashboard_address = client.get(ray_constants.REDIS_KEY_DASHBOARD)
    assert dashboard_address is not None
    dashboard_rpc_address = client.get(
        dashboard_consts.REDIS_KEY_DASHBOARD_RPC)
    assert dashboard_rpc_address is not None
    key = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{node_id}"
    agent_ports = client.get(key)
    assert agent_ports is not None


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "dashboard_host": "127.0.0.1"
    }, {
        "dashboard_host": "0.0.0.0"
    }],
    indirect=True)
def test_dashboard_address(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_ip = webui_url.split(":")[0]
    assert not ipaddress.ip_address(webui_ip).is_unspecified
    assert webui_ip in [
        "127.0.0.1", ray_start_with_dashboard["node_ip_address"]
    ]


def test_nodes_update(enable_test_module, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
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
            assert len(dump_data["agents"]) == 1
            assert len(dump_data["nodeIdToIp"]) == 1
            assert len(dump_data["nodeIdToHostname"]) == 1
            assert dump_data["nodes"].keys() == dump_data[
                "nodeIdToHostname"].keys()

            response = requests.get(webui_url + "/test/notified_agents")
            response.raise_for_status()
            try:
                notified_agents = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert notified_agents["result"] is True
            notified_agents = notified_agents["data"]
            assert len(notified_agents) == 1
            assert notified_agents == dump_data["agents"]
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


def test_http_get(enable_test_module, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    target_url = webui_url + "/test/dump"

    timeout_seconds = 30
    start_time = time.time()
    while True:
        time.sleep(3)
        try:
            response = requests.get(webui_url + "/test/http_get?url=" +
                                    target_url)
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["agents"]) == 1
            node_id, ports = next(iter(dump_data["agents"].items()))
            ip = ray_start_with_dashboard["node_ip_address"]
            http_port = ports[dashboard_consts.DASHBOARD_AGENT_HTTP_PORT]

            response = requests.get(
                f"http://{ip}:{http_port}"
                f"/test/http_get_from_agent?url={target_url}")
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_system_config": {
            "agent_heartbeat_period_milliseconds": 1000,
            "agent_num_heartbeats_timeout": 10,
        }
    }],
    indirect=True)
def test_detect_agent_hangs(enable_test_module, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    all_processes = ray.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    target_url = webui_url + "/test/dump"
    agent_proc = _search_agent(raylet_proc.children())

    timeout_seconds = 30
    start_time = time.time()
    while True:
        time.sleep(3)
        try:
            response = requests.get(webui_url + "/test/http_get?url=" +
                                    target_url)
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["agents"]) == 1
            node_id, ports = next(iter(dump_data["agents"].items()))
            ip = ray_start_with_dashboard["node_ip_address"]
            http_port = ports[dashboard_consts.DASHBOARD_AGENT_HTTP_PORT]

            with pytest.raises(requests.ReadTimeout):
                requests.get(
                    f"http://{ip}:{http_port}"
                    f"/test/agent_hang", timeout=1)

            # 20s > 1000ms * 10
            agent_proc.wait(20)

            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


def test_class_method_route_table(enable_test_module):
    head_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardHeadModule)
    agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule)
    test_head_cls = None
    for cls in head_cls_list:
        if cls.__name__ == "TestHead":
            test_head_cls = cls
            break
    assert test_head_cls is not None
    test_agent_cls = None
    for cls in agent_cls_list:
        if cls.__name__ == "TestAgent":
            test_agent_cls = cls
            break
    assert test_agent_cls is not None

    def _has_route(route, method, path):
        if isinstance(route, aiohttp.web.RouteDef):
            if route.method == method and route.path == path:
                return True
        return False

    def _has_static(route, path, prefix):
        if isinstance(route, aiohttp.web.StaticDef):
            if route.path == path and route.prefix == prefix:
                return True
        return False

    all_routes = dashboard_utils.ClassMethodRouteTable.routes()
    assert any(_has_route(r, "HEAD", "/test/route_head") for r in all_routes)
    assert any(_has_route(r, "GET", "/test/route_get") for r in all_routes)
    assert any(_has_route(r, "POST", "/test/route_post") for r in all_routes)
    assert any(_has_route(r, "PUT", "/test/route_put") for r in all_routes)
    assert any(_has_route(r, "PATCH", "/test/route_patch") for r in all_routes)
    assert any(
        _has_route(r, "DELETE", "/test/route_delete") for r in all_routes)
    assert any(_has_route(r, "*", "/test/route_view") for r in all_routes)

    # Test bind()
    bound_routes = dashboard_utils.ClassMethodRouteTable.bound_routes()
    assert len(bound_routes) == 0
    dashboard_utils.ClassMethodRouteTable.bind(
        test_agent_cls.__new__(test_agent_cls))
    bound_routes = dashboard_utils.ClassMethodRouteTable.bound_routes()
    assert any(_has_route(r, "POST", "/test/route_post") for r in bound_routes)
    assert all(
        not _has_route(r, "PUT", "/test/route_put") for r in bound_routes)

    # Static def should be in bound routes.
    routes.static("/test/route_static", "/path")
    bound_routes = dashboard_utils.ClassMethodRouteTable.bound_routes()
    assert any(
        _has_static(r, "/path", "/test/route_static") for r in bound_routes)

    # Test duplicated routes should raise exception.
    try:

        @routes.get("/test/route_get")
        def _duplicated_route(req):
            pass

        raise Exception("Duplicated routes should raise exception.")
    except Exception as ex:
        message = str(ex)
        assert "/test/route_get" in message
        assert "test_head.py" in message

    # Test exception in handler
    post_handler = None
    for r in bound_routes:
        if _has_route(r, "POST", "/test/route_post"):
            post_handler = r.handler
            break
    assert post_handler is not None

    loop = asyncio.get_event_loop()
    r = loop.run_until_complete(post_handler())
    assert r.status == 200
    resp = json.loads(r.body)
    assert resp["result"] is False
    assert "Traceback" in resp["msg"]


def test_async_loop_forever():
    counter = [0]

    @dashboard_utils.async_loop_forever(interval_seconds=0.1)
    async def foo():
        counter[0] += 1
        raise Exception("Test exception")

    loop = asyncio.get_event_loop()
    loop.create_task(foo())
    loop.call_later(1, loop.stop)
    loop.run_forever()
    assert counter[0] > 2


def test_dashboard_module_decorator(enable_test_module):
    head_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardHeadModule)
    agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule)

    assert any(cls.__name__ == "TestHead" for cls in head_cls_list)
    assert any(cls.__name__ == "TestAgent" for cls in agent_cls_list)

    test_code = """
import os
import ray.new_dashboard.utils as dashboard_utils

os.environ.pop("RAY_DASHBOARD_MODULE_TEST")
head_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardHeadModule)
agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule)
print(head_cls_list)
print(agent_cls_list)
assert all(cls.__name__ != "TestHead" for cls in head_cls_list)
assert all(cls.__name__ != "TestAgent" for cls in agent_cls_list)
print("success")
"""
    run_string_as_driver(test_code)


def test_aiohttp_cache(enable_test_module, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 5
    start_time = time.time()
    value1_timestamps = set()
    while True:
        time.sleep(1)
        try:
            for x in range(10):
                response = requests.get(webui_url +
                                        "/test/aiohttp_cache/t1?value=1")
                response.raise_for_status()
                timestamp = response.json()["data"]["timestamp"]
                value1_timestamps.add(timestamp)
            assert len(value1_timestamps) > 1
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")

    sub_path_timestamps = set()
    for x in range(10):
        response = requests.get(webui_url +
                                f"/test/aiohttp_cache/tt{x}?value=1")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        sub_path_timestamps.add(timestamp)
    assert len(sub_path_timestamps) == 10

    volatile_value_timestamps = set()
    for x in range(10):
        response = requests.get(webui_url +
                                f"/test/aiohttp_cache/tt?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 10

    volatile_value_timestamps = set()
    for x in range(5):
        response = requests.get(webui_url + "/test/aiohttp_cache/tt?value=10")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 5

    response = requests.get(webui_url + "/test/aiohttp_cache/raise_exception")
    response.raise_for_status()
    result = response.json()
    assert result["result"] is False
    assert "KeyError" in result["msg"]

    volatile_value_timestamps = set()
    for x in range(10):
        response = requests.get(webui_url +
                                f"/test/aiohttp_cache_lru/tt{x % 4}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 4

    volatile_value_timestamps = set()
    data = collections.defaultdict(set)
    for x in [0, 1, 2, 3, 4, 5, 2, 1, 0, 3]:
        response = requests.get(webui_url +
                                f"/test/aiohttp_cache_lru/t1?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        data[x].add(timestamp)
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 8
    assert len(data[3]) == 2
    assert len(data[0]) == 2

    volatile_value_timestamps = set()
    for x in range(10):
        response = requests.get(webui_url + "/test/aiohttp_cache_exception")
        response.raise_for_status()
        assert response.json()["result"] is False
        timestamp = response.json()["timestamp"]
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 2, volatile_value_timestamps

    volatile_value_timestamps = set()
    for x in range(10):
        response = requests.get(webui_url +
                                "/test/aiohttp_not_cache_exception")
        response.raise_for_status()
        assert response.json()["result"] is False
        timestamp = response.json()["timestamp"]
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 10, volatile_value_timestamps

    # test expiration
    volatile_value_timestamps = set()
    data = collections.defaultdict(set)
    for x in [0, 1, 2, 0]:
        response = requests.get(webui_url +
                                f"/test/aiohttp_cache_lru/expire?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        data[x].add(timestamp)
        volatile_value_timestamps.add(timestamp)
    time.sleep(60)
    for x in [1, 2]:
        response = requests.get(webui_url +
                                f"/test/aiohttp_cache_lru/expire?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        data[x].add(timestamp)
        volatile_value_timestamps.add(timestamp)
    assert len(volatile_value_timestamps) == 5
    assert len(data[1]) == 2
    assert len(data[2]) == 2


def test_get_cluster_status(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    address_info = ray_start_with_dashboard
    webui_url = address_info["webui_url"]
    webui_url = format_web_url(webui_url)

    # Check that the cluster_status endpoint works without the underlying data
    # from the GCS, but returns nothing.
    def get_cluster_status():
        response = requests.get(f"{webui_url}/api/cluster_status")
        response.raise_for_status()
        print(response.json())
        assert response.json()["result"]
        assert "autoscalingStatus" in response.json()["data"]
        assert response.json()["data"]["autoscalingStatus"] is None
        assert "autoscalingError" in response.json()["data"]
        assert response.json()["data"]["autoscalingError"] is None
        assert "clusterStatus" in response.json()["data"]
        assert "loadMetricsReport" in response.json()["data"]["clusterStatus"]

    wait_until_succeeded_without_exception(get_cluster_status,
                                           (requests.RequestException, ))

    # Populate the GCS field, check that the data is returned from the
    # endpoint.
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    client = redis.StrictRedis(
        host=address[0],
        port=int(address[1]),
        password=ray_constants.REDIS_DEFAULT_PASSWORD)

    client.hset(ray_constants.DEBUG_AUTOSCALING_STATUS_LEGACY, "value",
                "hello")
    client.hset(ray_constants.DEBUG_AUTOSCALING_ERROR, "value", "world")

    response = requests.get(f"{webui_url}/api/cluster_status")
    response.raise_for_status()
    assert response.json()["result"]
    assert "autoscalingStatus" in response.json()["data"]
    assert response.json()["data"]["autoscalingStatus"] == "hello"
    assert "autoscalingError" in response.json()["data"]
    assert response.json()["data"]["autoscalingError"] == "world"
    assert "clusterStatus" in response.json()["data"]
    assert "loadMetricsReport" in response.json()["data"]["clusterStatus"]


def test_snapshot(ray_start_with_dashboard):
    driver_template = """
import ray

ray.init(address="{address}", namespace="my_namespace")

@ray.remote
class Pinger:
    def ping(self):
        return "pong"

a = Pinger.options(lifetime={lifetime}, name={name}).remote()
ray.get(a.ping.remote())
    """

    detached_driver = driver_template.format(
        address=ray_start_with_dashboard["redis_address"],
        lifetime="'detached'",
        name="'abc'")
    named_driver = driver_template.format(
        address=ray_start_with_dashboard["redis_address"],
        lifetime="None",
        name="'xyz'")
    unnamed_driver = driver_template.format(
        address=ray_start_with_dashboard["redis_address"],
        lifetime="None",
        name="None")

    run_string_as_driver(detached_driver)
    run_string_as_driver(named_driver)
    run_string_as_driver(unnamed_driver)

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    response = requests.get(f"{webui_url}/api/snapshot")
    response.raise_for_status()
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__),
        "modules/snapshot/snapshot_schema.json")
    pprint.pprint(data)
    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

    assert len(data["data"]["snapshot"]["actors"]) == 3
    assert len(data["data"]["snapshot"]["jobs"]) == 4

    for actor_id, entry in data["data"]["snapshot"]["actors"].items():
        assert entry["jobId"] in data["data"]["snapshot"]["jobs"]
        assert entry["actorClass"] == "Pinger"
        assert entry["startTime"] >= 0
        if entry["isDetached"]:
            # Note: Detached actor also will be destroyed in ant internal codes
            # so it's state is DEAD and endTime is great than 0.
            assert entry["endTime"] > 0, entry
        else:
            assert entry["endTime"] > 0, entry
        assert "runtimeEnv" in entry


def test_immutable_types():
    d = {str(i): i for i in range(1000)}
    d["list"] = list(range(1000))
    d["list"][0] = {str(i): i for i in range(1000)}
    d["dict"] = {str(i): i for i in range(1000)}
    immutable_dict = dashboard_utils.make_immutable(d)
    assert type(immutable_dict) == dashboard_utils.ImmutableDict
    assert immutable_dict == dashboard_utils.ImmutableDict(d)
    assert immutable_dict == d
    assert dashboard_utils.ImmutableDict(immutable_dict) == immutable_dict
    assert dashboard_utils.ImmutableList(
        immutable_dict["list"]) == immutable_dict["list"]
    assert "512" in d
    assert "512" in d["list"][0]
    assert "512" in d["dict"]

    # Test type conversion
    assert type(dict(immutable_dict)["list"]) == dashboard_utils.ImmutableList
    assert type(list(
        immutable_dict["list"])[0]) == dashboard_utils.ImmutableDict

    # Test json dumps / loads
    json_str = json.dumps(immutable_dict, cls=dashboard_utils.CustomEncoder)
    deserialized_immutable_dict = json.loads(json_str)
    assert type(deserialized_immutable_dict) == dict
    assert type(deserialized_immutable_dict["list"]) == list
    assert immutable_dict.mutable() == deserialized_immutable_dict
    dashboard_utils.rest_response(True, "OK", data=immutable_dict)
    dashboard_utils.rest_response(True, "OK", **immutable_dict)

    # Test copy
    copy_of_immutable = copy.copy(immutable_dict)
    assert copy_of_immutable == immutable_dict
    deepcopy_of_immutable = copy.deepcopy(immutable_dict)
    assert deepcopy_of_immutable == immutable_dict

    # Test get default immutable
    immutable_default_value = immutable_dict.get("not exist list", [1, 2])
    assert type(immutable_default_value) == dashboard_utils.ImmutableList

    # Test recursive immutable
    assert type(immutable_dict["list"]) == dashboard_utils.ImmutableList
    assert type(immutable_dict["dict"]) == dashboard_utils.ImmutableDict
    assert type(immutable_dict["list"][0]) == dashboard_utils.ImmutableDict

    # Test exception
    with pytest.raises(TypeError):
        dashboard_utils.ImmutableList((1, 2))

    with pytest.raises(TypeError):
        dashboard_utils.ImmutableDict([1, 2])

    with pytest.raises(TypeError):
        immutable_dict["list"] = []

    with pytest.raises(AttributeError):
        immutable_dict.update({1: 3})

    with pytest.raises(TypeError):
        immutable_dict["list"][0] = 0

    with pytest.raises(AttributeError):
        immutable_dict["list"].extend([1, 2])

    with pytest.raises(AttributeError):
        immutable_dict["list"].insert(1, 2)

    d2 = dashboard_utils.ImmutableDict({1: np.zeros([3, 5])})
    with pytest.raises(TypeError):
        print(d2[1])

    d3 = dashboard_utils.ImmutableList([1, np.zeros([3, 5])])
    with pytest.raises(TypeError):
        print(d3[1])


def test_http_proxy(enable_test_module, set_http_proxy, shutdown_only):
    address_info = ray.init(num_cpus=1, include_dashboard=True)
    assert (wait_until_server_available(address_info["webui_url"]) is True)

    webui_url = address_info["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 10
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            response = requests.get(
                webui_url + "/test/dump",
                proxies={
                    "http": None,
                    "https": None
                })
            response.raise_for_status()
            try:
                response.json()
                assert response.ok
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


def test_dashboard_port_conflict(ray_start_with_dashboard):
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

    host, port = address_info["webui_url"].split(":")
    temp_dir = "/tmp/ray"
    log_dir = "/tmp/ray/session_latest/logs"
    dashboard_cmd = [
        sys.executable, dashboard.__file__, f"--host={host}", f"--port={port}",
        f"--temp-dir={temp_dir}", f"--log-dir={log_dir}",
        f"--redis-address={address[0]}:{address[1]}",
        f"--redis-password={ray_constants.REDIS_DEFAULT_PASSWORD}"
    ]
    logger.info("The dashboard should be exit: %s", dashboard_cmd)
    p = subprocess.Popen(dashboard_cmd)
    p.wait(5)

    dashboard_cmd.append("--port-retries=10")
    subprocess.Popen(dashboard_cmd)

    timeout_seconds = 30
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            dashboard_url = client.get(ray_constants.REDIS_KEY_DASHBOARD)
            if dashboard_url:
                new_port = int(dashboard_url.split(b":")[-1])
                assert new_port > int(port)
                break
        except AssertionError as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "include_dashboard": True
    }], indirect=True)
def test_multi_nodes(enable_test_module, ray_start_cluster_head):
    cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    node = cluster.add_node()
    node2 = cluster.add_node()

    def _check_nodes():
        try:
            resp = requests.get(webui_url + "/test/dump?key=nodes")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert len(result["data"]["nodes"]) == 3, "Nodes count incorrect"
            for node_id, node_info in result["data"]["nodes"].items():
                assert node_info["state"] == "ALIVE", f"{node_id} is not ALIVE"
            resp = requests.get(webui_url + "/test/dump?key=agents")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert len(result["data"]["agents"]) == 3, "Agent count incorrect"
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_nodes, timeout=30)

    assert node.remaining_processes_alive()
    assert node2.remaining_processes_alive()
    cluster.remove_node(node2)
    cluster.remove_node(node)
    assert not any(n.any_processes_alive() for n in [node, node2])

    def _check_nodes():
        try:
            resp = requests.get(webui_url + "/test/dump?key=nodes")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert len(result["data"]["nodes"]) == 1, "Nodes count incorrect"
            resp = requests.get(webui_url + "/test/dump?key=dead_nodes")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert len(
                result["data"]["deadNodes"]) == 2, "Dead nodes count incorrect"
            resp = requests.get(webui_url + "/test/dump?key=agents")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert len(result["data"]["agents"]) == 1, "Agent count incorrect"
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_nodes, timeout=30)


def test_signal():
    s = dashboard_utils.Signal("abc")
    s.append(lambda: None)
    with pytest.raises(AssertionError):
        s.freeze()

    def foo():
        pass

    s.clear()
    s.append(foo)
    with pytest.raises(AssertionError):
        s.freeze()

    def foo():
        yield

    s.clear()
    s.append(foo)
    with pytest.raises(AssertionError):
        s.freeze()

    s.clear()
    s.append(foo())
    with pytest.raises(AssertionError):
        s.freeze()

    async def foo():
        yield

    s.clear()
    s.append(foo)
    with pytest.raises(AssertionError):
        s.freeze()

    s.clear()
    s.append(foo())
    with pytest.raises(AssertionError):
        s.freeze()

    async def foo():
        pass

    s.clear()
    s.append(foo())
    with pytest.raises(AssertionError):
        s.freeze()

    s.clear()
    s.append(foo)
    s.freeze()


def test_aio_grpc_reconnect_channel(enable_test_module,
                                    ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)

    all_processes = ray.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    reconnect_seq_pos = [0]

    def _check_rpc_is_OK():
        try:
            resp = requests.get(webui_url + "/test/reconnect_result")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            reconnect_sequence = result["data"]["reconnectSequence"]
            assert "Connect" in reconnect_sequence
            assert "OK" in reconnect_sequence
            reconnect_seq_pos[0] = len(reconnect_sequence)
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_rpc_is_OK, timeout=10)

    for _ in range(2):
        logger.info("Kill agent.")
        agent = _search_agent(raylet_proc.children())
        assert agent is not None
        agent.kill()
        agent.wait()

        def _check_rpc_is_OK():
            try:
                resp = requests.get(webui_url + "/test/reconnect_result")
                resp.raise_for_status()
                result = resp.json()
                assert result["result"] is True, resp.text
                reconnect_sequence = result["data"]["reconnectSequence"]
                assert "StatusCode.UNAVAILABLE" in reconnect_sequence[
                    reconnect_seq_pos[0]:]
                assert "Reconnect" in reconnect_sequence[reconnect_seq_pos[0]:]
                assert "OK" in reconnect_sequence[reconnect_seq_pos[0]:]
                reconnect_seq_pos[0] = len(reconnect_sequence)
                return True
            except Exception as ex:
                logger.info(ex)
                return False

        logger.info("Check reconnect agent.")
        wait_for_condition(_check_rpc_is_OK, timeout=100)


def test_gcs_reconnect(fast_gcs_failure_detection, enable_test_module,
                       ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)

    all_processes = ray.worker._global_node.all_processes
    gcs_server_info = all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0]
    gcs_server_proc = psutil.Process(gcs_server_info.process.pid)

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    node_ids = set()
    actor_ids = set()
    dead_actor_ids = set()

    @ray.remote
    class A:
        def foo(self):
            return "foo"

    actors = [A.remote(), A.remote(), A.remote()]
    r = [a.foo.remote() for a in actors]
    ray.get(r)
    actors = actors[:1]  # Destroy two actors.

    def _expect(key, result_key, count):
        resp = requests.get(f"{webui_url}/test/dump?key={key}")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        objects = result["data"][result_key]
        assert len(objects) == count
        return list(objects.keys())

    def _get_connected_node():
        try:
            ids = _expect("nodes", "nodes", 1)
            node_ids.update(ids)
            ids = _expect("actors", "actors", 1)
            actor_ids.update(ids)
            ids = _expect("dead_actors", "deadActors", 2)
            dead_actor_ids.update(ids)
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_get_connected_node, timeout=10)

    def _check_pubsub_thread_number():
        resp = requests.get(webui_url + "/test/pubsub_thread_number")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        count = result["data"]["value"]
        # 4 = 1 * REDIS_HEALTH_CHECK + 3 * Updater subscribe
        assert count == 4

    _check_pubsub_thread_number()

    for _ in range(2):
        gcs_server_cmd = gcs_server_proc.cmdline()
        gcs_server_proc.kill()
        gcs_server_proc.wait()

        resp = requests.get(webui_url + "/test/check_gcs_alive")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is False, resp.text
        assert "StatusCode.UNAVAILABLE" in result["msg"], resp.text

        p = subprocess.Popen(gcs_server_cmd)
        gcs_server_proc = psutil.Process(p.pid)

        # Clear DataSource.
        resp = requests.post(webui_url + "/test/clear_datasource")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text

        # Make sure DataSource is cleared.
        assert _expect("nodes", "nodes", 0) == []
        assert _expect("actors", "actors", 0) == []
        assert _expect("dead_actors", "deadActors", 0) == []

        def _get_connected_node():
            try:
                ids = _expect("nodes", "nodes", 1)
                assert node_ids == set(ids)
                ids = _expect("actors", "actors", 1)
                assert actor_ids == set(ids)
                ids = _expect("dead_actors", "deadActors", 2)
                assert dead_actor_ids == set(ids)

                resp = requests.get(webui_url + "/test/check_gcs_alive")
                resp.raise_for_status()
                result = resp.json()
                assert result["result"] is True, resp.text
                assert result["msg"] == "Success"
                return True
            except Exception as ex:
                logger.info(ex)
                return False

        wait_for_condition(_get_connected_node, timeout=10)
        # Once gcs reconnected, pubsub thread number should not increase
        _check_pubsub_thread_number()


def test_disconnect_redis(fast_redis_failure_detection, enable_test_module,
                          ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    all_processes = ray.worker._global_node.all_processes
    redis_proc_info = all_processes[ray_constants.PROCESS_TYPE_REDIS_SERVER][0]
    redis_proc = psutil.Process(redis_proc_info.process.pid)
    dashboard_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
    dashboard_proc = psutil.Process(dashboard_info.process.pid)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    resp = requests.get(webui_url + "/test/dump")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text

    redis_proc.kill()
    redis_proc.wait()

    # Redis is dead, the core_worker.shutdown() hangs.
    # We have to restart redis to make the test finish.
    subprocess.Popen(redis_proc_info.command)

    def _check_dashboard_is_dead():
        return dashboard_proc.status() not in [
            psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING
        ]

    wait_for_condition(_check_dashboard_is_dead, timeout=10)


def test_disconnect_redis2(fast_redis_failure_detection, enable_test_module,
                           ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    all_processes = ray.worker._global_node.all_processes
    redis_proc_info = all_processes[ray_constants.PROCESS_TYPE_REDIS_SERVER][0]
    redis_proc = psutil.Process(redis_proc_info.process.pid)
    dashboard_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
    dashboard_proc = psutil.Process(dashboard_info.process.pid)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    resp = requests.get(webui_url + "/test/dump")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text

    redis_proc.kill()
    redis_proc.wait()

    def _check_dashboard_is_dead():
        return dashboard_proc.status() not in [
            psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING
        ]

    wait_for_condition(_check_dashboard_is_dead, timeout=10)

    # Redis is dead, the core_worker.shutdown() hangs.
    # We have to restart redis to make the test finish.
    subprocess.Popen(redis_proc_info.command)


def test_disconnect_dashboard(enable_test_module, enable_system_jobs,
                              ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    all_processes = ray.worker._global_node.all_processes
    dashboard_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
    dashboard_proc = psutil.Process(dashboard_info.process.pid)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    def assert_result():
        resp = requests.get(webui_url + "/test/dump")
        resp.raise_for_status()
        result = resp.json()

        assert result["result"] is True, resp.text
        jobs = result["data"]["jobs"]
        assert len(jobs) == 2

        actor_optimizer_job = jobs["01000080"]
        assert actor_optimizer_job["name"] == "SYSTEM_JOB_actor-optimizer"
        assert actor_optimizer_job["namespaceId"] == "SYSTEM_NAMESPACE"
        assert actor_optimizer_job[
            "driverEntry"] == "io.ray.actoroptimizes.ActorOptimizerSystemJob"

        actor_observer_job = jobs["02000080"]
        assert actor_observer_job["name"] == "SYSTEM_JOB_actor-observer"
        assert actor_observer_job["namespaceId"] == "SYSTEM_NAMESPACE"
        assert actor_observer_job[
            "driverEntry"] == "io.ray.actorobserver.ActorObserverSystemJob"

    import time
    time.sleep(5)
    assert_result()
    dashboard_proc.kill()
    dashboard_proc.wait()

    # Kill and restart dashboard.
    p = subprocess.Popen(dashboard_info.command)
    dashboard_proc = psutil.Process(p.pid)

    def _check_dashboard_is_running():
        return dashboard_proc.status() in [
            psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING
        ]

    wait_for_condition(_check_dashboard_is_running, timeout=30)
    time.sleep(10)
    assert_result()

    dashboard_proc.terminate()
    dashboard_proc.wait()


@pytest.mark.parametrize("subscribing_error", [False, True])
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "include_dashboard": False
    }], indirect=True)
@pytest.mark.asyncio
async def test_psubscribe_updater(ray_start_regular, subscribing_error):
    test_lock = asyncio.Lock()
    test_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    class TestUpdater(dashboard_utils.PSubscribeUpdater):
        update_all = 0

        @classmethod
        async def _update_all(cls, gcs_channel):
            cls.update_all += 1

        @classmethod
        async def _update_from_psubscribe_channel(cls, receiver):
            async with test_lock:
                await test_event.wait()

    remaining_failure_times = 3

    class MockRedisClientPubsubThread(dashboard_utils.RedisClientPubsubThread):
        def __getattr__(self, item):
            nonlocal remaining_failure_times
            if remaining_failure_times:
                remaining_failure_times -= 1
                raise redis.exceptions.ConnectionError(
                    f"remaining_failure_times = {remaining_failure_times}")
            else:
                return super().__getattr__(item)

    class MockRedisClientThread(dashboard_utils.RedisClientThread):
        def pubsub(self, **kwargs):
            t = MockRedisClientPubsubThread(self, **kwargs)
            t.start()
            return t

    redis_address = ray_start_regular["redis_address"].split(":")

    if subscribing_error:
        wait_time = remaining_failure_times * \
            dashboard_consts.RESTARTABLE_TASK_AUTO_RESTART_INTERVAL_SECONDS + 1
        aioredis_client = MockRedisClientThread(
            redis_address[0], int(redis_address[1]),
            ray_constants.REDIS_DEFAULT_PASSWORD or None)
    else:
        wait_time = 5
        aioredis_client = dashboard_utils.RedisClientThread(
            redis_address[0], int(redis_address[1]),
            ray_constants.REDIS_DEFAULT_PASSWORD or None)

    aioredis_client.start()

    task = TestUpdater.create("MockChannel", aioredis_client, None)

    async def _check_lock():
        async with async_timeout.timeout(wait_time):
            while not test_lock.locked():
                await asyncio.sleep(0.01)

    # The test_lock should be locked in `_update_from_psubscribe_channel`
    await _check_lock()

    expected_update_all_run_times = 4 if subscribing_error else 1
    assert TestUpdater.update_all == expected_update_all_run_times

    assert test_lock.locked()
    assert not test_event.is_set()

    async def _acquire_lock():
        async with async_timeout.timeout(wait_time):
            await test_lock.acquire()
            test_event.set()

    acquire_lock_task = loop.create_task(_acquire_lock())
    # Call async_rerun
    await asyncio.gather(acquire_lock_task, task.restart())

    # The test_lock is acquired by `_acquire_lock`
    assert test_event.is_set()

    async def _check_restart_ok():
        async with async_timeout.timeout(wait_time):
            while TestUpdater.update_all != expected_update_all_run_times + 1:
                await asyncio.sleep(0.01)

    # The updater has been restarted.
    await _check_restart_ok()


@pytest.mark.asyncio
async def test_restartable_task():
    counter = 0

    async def _func():
        nonlocal counter
        counter += 1
        return counter

    task = dashboard_utils.RestartableTask(_func)
    for x in range(1, 10):
        assert await task == x
        assert task.done() is True
        await task.restart()

    # Test RestartableTask.__iter__, compatible with yield from
    @asyncio.coroutine
    def t1():
        return (yield from t2())

    @asyncio.coroutine
    def t2():
        return (yield from dashboard_utils.RestartableTask(t3))

    @asyncio.coroutine
    def t3():
        return 1, 2, 3

    val = await t1()
    assert val == (1, 2, 3)


def test_subprocess_error():
    for ex in [
            dashboard_utils.SubprocessCalledProcessError,
            dashboard_utils.SubprocessTimeoutExpired
    ]:
        with pytest.raises(subprocess.SubprocessError) as e:
            raise ex(123, "abc")
        assert "test_out" not in str(e.value)
        assert "test_err" not in str(e.value)
        with pytest.raises(subprocess.SubprocessError) as e:
            raise ex(123, "abc", stderr=b"test_err")
        assert "test_out" not in str(e.value)
        assert "test_err" in str(e.value)
        with pytest.raises(subprocess.SubprocessError) as e:
            raise ex(123, "abc", output=b"test_out")
        assert "test_out" in str(e.value)
        assert "test_err" not in str(e.value)
        with pytest.raises(subprocess.SubprocessError) as e:
            raise ex(123, "abc", output=b"test_out", stderr=b"test_err")
        assert "test_out" in str(e.value)
        assert "test_err" in str(e.value)


def test_subprocess_error_with_last_n_lines():
    stdout = "1\n2\n3\n4\n5\n"
    stderr = "5\n4\n3\n2\n1\n"
    exception = dashboard_utils.SubprocessCalledProcessError(
        123, "abc", output=stdout, stderr=stderr)
    exception.last_n_lines_of_stdout = 3
    exception.last_n_lines_of_stderr = 6
    assert "stdout='4\\n5\\n'" in str(exception)
    assert "stderr='5\\n4\\n3\\n2\\n1\\n'" in str(exception)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
