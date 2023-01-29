import os
import sys
import yaml
import logging
import requests
import socket

import pytest
import ray
from ray.cluster_utils import Cluster
from ray.new_dashboard.modules.internal import internal_consts
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (format_web_url, wait_until_server_available,
                            wait_for_condition)

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def test_update_frontend(enable_test_module, shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=6)

    webui_url = addresses["webui_url"]
    assert (wait_until_server_available(webui_url) is True)
    webui_url = format_web_url(webui_url)

    new_dashboard_dir = ray.new_dashboard.__path__[0]
    target_file = os.path.join(new_dashboard_dir,
                               "client/build/static/test_head.py")

    def _check_update():
        try:
            resp = requests.put(
                webui_url + "/frontend",
                json={"url": f"{webui_url}/test/frontend_url"})
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert os.path.exists(target_file)
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_update, timeout=10)


def test_cluster_token(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    resp = requests.get(webui_url + "/clusterToken")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    token = result["data"]["token"]

    resp = requests.get(webui_url + "/nodes?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text

    resp = requests.get(webui_url +
                        f"/nodes?view=summary&clusterToken={token}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text

    resp = requests.get(webui_url + "/nodes?view=summary&clusterToken=xxx")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is False, resp.text
    assert "token mismatch" in result["msg"]


def test_ray_features(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    resp = requests.get(f"{webui_url}/ray_features")
    assert resp.ok
    features = resp.json()
    with open(internal_consts.RAY_FEATURES_FILE, "rb") as f:
        assert set(features["data"]["features"]) == set(
            yaml.load(f, Loader=yaml.FullLoader))


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "node_name": socket.gethostbyname(socket.gethostname())
    }],
    indirect=True)
def test_freeze_node(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    local_host_name = socket.gethostname()
    local_ip = socket.gethostbyname(local_host_name)

    resp = requests.post(
        f"{webui_url}/scheduler/freeze_nodes",
        json={
            "node_name_set": [local_ip],
        })
    print(resp.json())
    assert resp.ok

    resp = requests.get(f"{webui_url}/scheduler/frozen_nodes")
    assert resp.ok
    print(resp.json())
    assert resp.json()["data"]["frozenNodeSet"] == [local_ip]

    resp = requests.post(
        f"{webui_url}/scheduler/unfreeze_nodes",
        json={
            "node_name_set": [local_ip],
        })
    assert resp.ok

    resp = requests.get(f"{webui_url}/scheduler/frozen_nodes")
    assert resp.ok
    print(resp.json())
    assert resp.json()["data"]["frozenNodeSet"] == []


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "include_dashboard": True
    }], indirect=True)
def test_node_info_for_signal(enable_test_module, disable_aiohttp_cache,
                              ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    cluster.add_node()
    cluster.add_node()

    response = requests.get(webui_url + "/nodes?view=summary")
    response.raise_for_status()
    summary = response.json()
    assert summary["result"] is True, summary["msg"]
    summary = summary["data"]["summary"]
    assert len(summary) == 3
    for node_summary in summary:
        node_id = node_summary["raylet"]["nodeId"]
        # For sls logs
        assert "nodeManagerHostname" in node_summary["raylet"]
        # For logUrl
        assert "nodeManagerAddress" in node_summary["raylet"]
        assert "startTime" in node_summary["raylet"]
        response = requests.get(webui_url + f"/nodes/{node_id}")
        response.raise_for_status()
        detail = response.json()
        assert detail["result"] is True, detail["msg"]
        detail = detail["data"]["detail"]
        assert "nodeManagerHostname" in detail["raylet"]
        assert "nodeManagerAddress" in detail["raylet"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
