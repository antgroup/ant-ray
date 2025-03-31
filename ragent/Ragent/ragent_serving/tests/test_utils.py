import json
import os.path

from ragent_serving.utils import load_flow_metadata, get_env_info

current_dir = os.path.dirname(os.path.abspath(__file__))


def load_test_data(file_name: str):
    with open(os.path.join(current_dir, "data", file_name), "r") as file:
        data = file.read()
    return data


def test_load_metadata():
    url = "http://alipay-cognition.cn-hangzhou.alipay.aliyun-inc.com/antflow/flowconfig/demo/rag_agent_data.json"
    data = load_flow_metadata(url)
    assert data == json.loads(load_test_data("rag_agent_data.json"))


def test_env_info():
    info = get_env_info()
    assert "python version" in info
    assert "ragent" in info
    assert "antflow" in info
