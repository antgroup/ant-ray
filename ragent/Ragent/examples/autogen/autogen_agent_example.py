import autogen
import os
import sys
from autogen import UserProxyAgent, config_list_from_json
from autogen.oai.openai_utils import filter_config
from autogen.cache import Cache
from autogen.agentchat.contrib.web_surfer import WebSurferAgent

from ragent.agents.autogen_agent import AutogenConversableAgent
from ragent.core.graph.graph import RagentGraph


sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

BLOG_POST_URL = "https://microsoft.github.io/autogen/blog/2023/04/21/LLM-tuning-math"
BLOG_POST_TITLE = "Does Model and Inference Parameter Matter in LLM Applications? - A Case Study for MATH | AutoGen"
BING_QUERY = "Microsoft"
config_list = autogen.config_list_from_json(
    "/Users/paer/Documents/work/codes/X/ray_common_libs/Ragent/examples/OAI_CONFIG_LIST"
)
llm_config = {
    "timeout": 180,
    "temperature": 0,
    "config_list": config_list,
}


def create_web_surfer_agent() -> WebSurferAgent:
    model = ["moonshot-v1-8k"]
    summarizer_llm_config = {
        "config_list": filter_config(config_list, dict(model=model)),  # type: ignore[no-untyped-call]
        "timeout": 180,
    }

    assert len(llm_config["config_list"]) > 0  # type: ignore[arg-type]
    assert len(summarizer_llm_config["config_list"]) > 0

    page_size = 4096
    web_surfer = WebSurferAgent(
        "web_surfer",
        llm_config=llm_config,
        summarizer_llm_config=summarizer_llm_config,
        browser_config={"viewport_size": page_size},
    )

    return web_surfer


def pure_autogen_web_surfer() -> None:
    web_surfer = create_web_surfer_agent()

    user_proxy = UserProxyAgent(
        "user_proxy",
        human_input_mode="NEVER",
        code_execution_config=False,
        default_auto_reply="",
        is_termination_msg=lambda x: True,
    )

    # Make some requests that should test function calling
    user_proxy.initiate_chat(
        web_surfer,
        message="Please visit the page 'https://en.wikipedia.org/wiki/Microsoft'",
    )

    user_proxy.initiate_chat(web_surfer, message="Please scroll down.")

    user_proxy.initiate_chat(web_surfer, message="Please scroll up.")

    user_proxy.initiate_chat(web_surfer, message="When was it founded?")

    user_proxy.initiate_chat(web_surfer, message="What's this page about?")


def ragent_autogen_web_surfer():
    os.environ["RAGENT_RUNTIME"] = "RAY"

    web_surfer = create_web_surfer_agent()
    ragent_web_surfer = AutogenConversableAgent(web_surfer, name="TestWebSurfer")
    # result = ragent_web_surfer.ask("Please visit the page 'https://en.wikipedia.org/wiki/Microsoft'")
    # print(f"Websurfer result: {result}")
    remote_web_surfer = RagentGraph(ragent_web_surfer)
    result = remote_web_surfer.ask(
        "Please visit the page 'https://en.wikipedia.org/wiki/Microsoft'"
    )
    print(f"Websurfer result: {result}")


ragent_autogen_web_surfer()
