import logging
import os
import random
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta

import pytest
import requests

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.consts import RAY_DASHBOARD_STATS_UPDATING_INTERVAL
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)
from ray.dashboard.modules.virtual_cluster.virtual_cluster_head import VirtualClusterHead

def test_virtual_clusters(disable_aiohttp_cache, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    hostname = socket.gethostname()
    virtual_cluster_id = "virtual_cluster_1"
    revision = 0

    def _create_virtual_cluster():
        try:
            resp = requests.post(
                webui_url + "/virtual_clusters",
                json={
                    "virtualClusterId": virtual_cluster_id,
                    "allocationMode": "mixed",
                    "replicaSets": {
                        "4c8g": 1,
                        "8c16g": 1
                    },
                    "revision": 1734141542684231600
                },
            )
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            resp = requests.get(webui_url + "/virtual_clusters")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            assert result["data"]["virtualClusterId"] == virtual_cluster_id
            revision = result["data"]["revision"]
            assert revision > 1734141542684231600
            for _, node_instance in result["data"]["nodeInstances"].items():
                assert node_instance["hostname"] == hostname
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_create_virtual_cluster, timeout=10)

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
