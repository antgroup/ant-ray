import requests
import json
from typing import (
    Any,
    List,
    Optional,
)
from langchain.callbacks.manager import (
    CallbackManagerForLLMRun,
)
from langchain.llms.base import LLM
from langchain_core.messages import BaseMessage, HumanMessage

from ragent.llms.llm import convert_message_to_dict, LanguageModelInput, BaseLLM


GPT_3_TURBO_16K = "gpt-3.5-turbo-16k"
GPT_4 = "gpt-4"


def prompt_engine_chat_completion(
    model: str = "gpt-3.5-turbo-16k",
    system_content: str = "",
    assistant_content: str = "",
    user_content: str = "",
) -> str:
    url = "https://promptsengine.alipay.com/api/prompt/predict"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    # 四尔那边提供的scene_id, 仅供demo调用，切勿频繁调用。
    scene_id = "RAY_DEMO_ON_AIGC"
    model_config = {"open_ai_model_config": {"model": model, "max_tokens": 3000}}
    prompts = []
    if system_content:
        prompts.append(
            {"prompt_template": {"template": system_content}, "role": "system"}
        )
    if assistant_content:
        prompts.append(
            {"prompt_template": {"template": assistant_content}, "role": "assistant"}
        )
    if user_content:
        prompts.append({"prompt_template": {"template": user_content}, "role": "user"})
    data = {
        "context": {"scene_id": scene_id},
        "prompt_config": prompts,
        "model_config": model_config,
        "extra_config": {"relation_id": "1000"},
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    # data = {
    #     "scene_id": scene_id,
    #     "model_config": model_config,
    #     "prompts": prompts
    # }
    # response = requests.post(url, headers=headers, json=data)
    reply = response.json()
    try:
        return reply["data"]["open_ai_result"]["model_answer"]["model_result"][0][
            "content"
        ]
    except Exception as e:
        print(f"Parsing OPENAI result failed, raw reply: {reply}")
        raise e


class AntLLM(BaseLLM):
    def __init__(self, model_name=GPT_3_TURBO_16K):
        super().__init__()

    @property
    def _llm_type(self) -> str:
        """Return type of llm."""
        return "ant-llm"

    def generate(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """Run the LLM on the given prompt and input."""
        # prompt 可能用 '\n' 分割了 system content 和 user content，可尝试切分后分别传入 LLM
        result = prompt_engine_chat_completion(
            model="gpt-3.5-turbo-16k",
            system_content="Answer the query and strictly follow the instructions given in query.",
            user_content=f"query: {prompt}",
        )
        return HumanMessage(content=result)
