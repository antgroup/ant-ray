import asyncio
import json
import logging

from langflow.interface.run import build_langchain_object_with_caching
from langflow.processing.process import generate_result

from ragent.core.constants import RuntimeEngineType
from ragent.core.graph.graph import RagentGraph
from ragent.core.runnables import LambdaRunnable
from ragent_serving.utils import load_flow_metadata, get_env_info

logger = logging.getLogger(__name__)


class AgentServing:
    def __init__(self, config: str):
        logger.info(f"Ragent serving env:{get_env_info()}")
        self.config = json.loads(config)
        self.flow_id = self.config.get("flow_id", "")
        self.flow_metadata_url = self.config.get("flow_metadata_url", None)
        logger.info(f"Start to init agent, config:{self.config}")
        self.flow_metadata = load_flow_metadata(self.flow_metadata_url)
        logger.info(f"Flow metadata:{self.flow_metadata}")
        chain = asyncio.run(build_langchain_object_with_caching(self.flow_metadata))

        def call_chain(input, config):
            result, output = generate_result(chain, input)
            return {"result": str(result), "info": str(output)}

        self.graph = RagentGraph(LambdaRunnable(call_chain))
        self.graph.init(RuntimeEngineType.LOCAL)

    def __call__(self, input):
        logger.info(f"Handle request:{input}")
        if isinstance(input, str):
            data = json.loads(input)
        else:
            data = input
        messages = data.get("inputs", "")
        output = self.graph.invoke(input=messages)
        resp = json.dumps(output, ensure_ascii=False)
        logger.info(f"Success to handle request, resp:{resp}")
        return resp
