import json
import os.path
import sys

import pytest

from ragent_serving.agent_serving import AgentServing


def test_agent_serving_base():
    metadata_url = "http://alipay-cognition.cn-hangzhou.alipay.aliyun-inc.com/antflow/flow_metadata/demo/rag_agent_data.json"
    config = json.dumps({"flow_id": "flow-id-xxx", "flow_metadata_url": metadata_url})
    serving = AgentServing(config)
    # query = "中餐贴并入工资发放政策"
    # resp = serving(query)
    # print(resp)
    query = json.dumps({"flow_id": "1", "inputs": {"input": "1+1=?"}})
    resp = serving(query)
    print(resp)


def test_yueque_loader_flow():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_url = "file://" + os.path.join(
        current_dir, "data", "yuque_loader_flow.json"
    )
    config = json.dumps({"flow_id": "flow-id-xxx", "flow_metadata_url": metadata_url})
    serving = AgentServing(config)
    query = json.dumps({"flow_id": "1", "inputs": {"input": "1+1=?"}})
    resp = serving(query)
    print(resp)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
