import logging
from ragent.core.langchain_agent import LangChainAgent, LangChainRemoteAgent
from ragent.core.config import set_log
import ray

logger = logging.getLogger(__name__)

set_log(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

llm = "openai"
prompt_name = "hwchase17/react"
tools = []

# Local langchain agent
lang_chain_agent = LangChainAgent(llm, tools, prompt_name)
result = lang_chain_agent.invoke(
    "what is LangChain? Answer the question directly instead of using tools."
)
logger.info(f"The result of local langchain agent: {result}.")

# Remote langchain agent
lang_chain_agent = LangChainAgent.remote(llm, tools, prompt_name)
result = ray.get(
    lang_chain_agent.invoke.remote(
        "what is LangChain? Answer the question directly instead of using tools."
    )
)
logger.info(f"The result of remote langchain agent: {result}.")
