import logging
import socket

import pytest
import requests

from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


def create_virtual_cluster(
    webui_url, virtual_cluster_id, allocation_mode, replica_sets, revision
):
    try:
        resp = requests.post(
            webui_url + "/virtual_clusters",
            json={
                "virtualClusterId": virtual_cluster_id,
                "allocationMode": allocation_mode,
                "replicaSets": replica_sets,
                "revision": revision,
            },
        )
        resp.raise_for_status()
        result = resp.json()
        print(result)
        assert result["result"] is True, resp.text
        return True
    except Exception as ex:
        logger.info(ex)
        return False


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_create_and_update_virtual_cluster(
    disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})
    hostname = socket.gethostname()
    virtual_cluster_id = ""
    allocation_mode = ""
    replica_sets = {}
    revision = 0

    def _create_or_update_virtual_cluster():
        try:
            nonlocal revision
            resp = requests.post(
                webui_url + "/virtual_clusters",
                json={
                    "virtualClusterId": virtual_cluster_id,
                    "allocationMode": allocation_mode,
                    "replicaSets": replica_sets,
                    "revision": revision,
                },
            )
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            assert result["data"]["virtualClusterId"] == virtual_cluster_id
            current_revision = result["data"]["revision"]
            assert current_revision > revision
            revision = current_revision
            virtual_cluster_replica_sets = {}
            for _, node_instance in result["data"]["nodeInstances"].items():
                assert node_instance["hostname"] == hostname
                virtual_cluster_replica_sets[node_instance["templateId"]] = (
                    virtual_cluster_replica_sets.get(node_instance["templateId"], 0) + 1
                )
            assert replica_sets == virtual_cluster_replica_sets
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    virtual_cluster_id = "virtual_cluster_1"
    allocation_mode = "exclusive"
    replica_sets = {"4c8g": 1, "8c16g": 1}
    wait_for_condition(_create_or_update_virtual_cluster, timeout=10)

    replica_sets = {"4c8g": 1}
    wait_for_condition(_create_or_update_virtual_cluster, timeout=10)

    replica_sets = {"4c8g": 1, "8c16g": 1}
    wait_for_condition(_create_or_update_virtual_cluster, timeout=10)

    replica_sets = {}
    wait_for_condition(_create_or_update_virtual_cluster, timeout=10)

    virtual_cluster_id = "virtual_cluster_2"
    allocation_mode = "mixed"
    replica_sets = {"4c8g": 1, "8c16g": 1}
    wait_for_condition(_create_or_update_virtual_cluster, timeout=10)


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_remove_virtual_cluster(disable_aiohttp_cache, ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})

    wait_for_condition(
        create_virtual_cluster,
        timeout=10,
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="exclusive",
        replica_sets={"4c8g": 1, "8c16g": 1},
        revision=0,
    )

    def _remove_virtual_cluster():
        try:
            resp = requests.delete(webui_url + "/virtual_clusters/virtual_cluster_1")
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_remove_virtual_cluster, timeout=10)


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_get_virtual_clusters(disable_aiohttp_cache, ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    hostname = socket.gethostname()

    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})

    wait_for_condition(
        create_virtual_cluster,
        timeout=10,
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="mixed",
        replica_sets={"4c8g": 2},
        revision=0,
    )

    wait_for_condition(
        create_virtual_cluster,
        timeout=10,
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode="exclusive",
        replica_sets={"8c16g": 2},
        revision=0,
    )

    def _get_virtual_clusters():
        try:
            resp = requests.get(webui_url + "/virtual_clusters")
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            for virtual_cluster in result["data"]["virtualClusters"]:
                if virtual_cluster["id"] == "virtual_cluster_1":
                    assert virtual_cluster["allocationMode"] == "mixed"
                    assert virtual_cluster["replicaSets"] == {"4c8g": 2}
                    assert len(virtual_cluster["nodeInstances"]) == 2
                    for _, node_instance in virtual_cluster["nodeInstances"].items():
                        assert node_instance["hostname"] == hostname
                        assert node_instance["templateId"] == "4c8g"
                    revision_1 = virtual_cluster["revision"]
                    assert revision_1 > 0
                elif virtual_cluster["id"] == "virtual_cluster_2":
                    assert virtual_cluster["allocationMode"] == "exclusive"
                    assert virtual_cluster["replicaSets"] == {"8c16g": 2}
                    assert len(virtual_cluster["nodeInstances"]) == 2
                    for _, node_instance in virtual_cluster["nodeInstances"].items():
                        assert node_instance["hostname"] == hostname
                        assert node_instance["templateId"] == "8c16g"
                    revision_2 = virtual_cluster["revision"]
                    assert revision_2 > 0
                else:
                    return False
            assert revision_2 > revision_1
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_get_virtual_clusters, timeout=10)
