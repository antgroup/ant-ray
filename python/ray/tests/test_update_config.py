import sys
import logging
import pytest
import requests
import json
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)

logger = logging.getLogger(__name__)


def test_update_raylet(ray_start_with_dashboard):
    assert wait_until_server_available(
        ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    resp = requests.post(
        f"{webui_url}/update_internal_config",
        json={"debug_dump_period_milliseconds": 1230000},
    )
    print(resp.json())
    assert resp.ok

    def check():
        resp = requests.get(f"{webui_url}/internal_config")
        print(resp.json())
        assert resp.ok
        config_dict = json.loads(resp.json()["data"]["config"])
        return config_dict["debug_dump_period_milliseconds"] == 1230000

    wait_for_condition(check)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
