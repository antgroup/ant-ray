import json
import logging
import os
import requests
from langchain_core.messages import BaseMessage, HumanMessage

from langchain_core.outputs import ChatGeneration, Generation

from ragent.core.runnables.runnable import RagentRunnable
from ragent.llms.llm import convert_message_to_dict, LanguageModelInput, BaseLLM

logger = logging.getLogger(__name__)

MODEL_QWEN_72B = "Qwen_72B_Chat_vLLM"
MODEL_QWEN_14B = "qwen_14b_chat_hf"


class AmaxLLM(BaseLLM):
    def __init__(self, model_name=MODEL_QWEN_72B):
        self.url = self.get_url()
        self.mode_name = model_name
        self.token = os.environ.get("OPENAI_API_KEY", None)
        self.stream = False
        self.headers = {"Content-Type": "application/json"}
        self.validate()

    def validate(self):
        if not self.token:
            raise ValueError("Please set OPENAI_API_KEY, eg: export OPENAI_API_KEY=xxx")

    def get_url(self):
        env_type = os.environ.get("ALIPAY_APP_ENV", None)
        if env_type is None:
            return "http://cv-cross.gateway.alipay.com/ua/invoke"
        if env_type.lower() == "prepub":
            return "http://gateway-cv-pre.alipay.com/ua/invoke"
        else:
            return "http://cv.gateway.alipay.com/ua/invoke"

    def format_body(self, message_dicts):
        data = {
            "api_version": "v2",
            "out_seq_length": 512,
            "prompts": [
                {
                    "prompt": message_dicts,
                    "repetition_penalty": 1.1,
                    "temperature": 0.2,
                    "top_k": 40,
                    "top_p": 0.9,
                }
            ],
            "stream": self.stream,
        }

        body = {
            "serviceCode": self.mode_name,
            "uri": "v1",
            "attributes": {
                "_TIMEOUT_": "50000",
                "_ROUTE_": "MAYA",
                "_APP_TOKEN_": self.token,
            },
            "params": {"features": {"data": json.dumps(data)}},
        }
        return body

    def generate(self, input: LanguageModelInput):
        messages = self._convert_input(input).to_messages()

        message_dicts = [convert_message_to_dict(m) for m in messages]
        body = self.format_body(message_dicts)
        logger.info(f"request prompt:{message_dicts}")
        response = requests.request(
            "POST", self.url, headers=self.headers, data=json.dumps(body)
        )
        reply = response.json()
        logger.info(f"reply:{reply}")
        if not reply["success"]:
            logger.error(
                f"Failed to request prompt, error:{reply['cognitionResponse']['message']}"
            )
            return None

        generated_code = json.loads(reply["resultMap"]["attributes"]["res"])[
            "generated_code"
        ]

        generated_reply = ""
        for m in generated_code:
            for s in m:
                logger.info(f"Reply message:{s}")
                generated_reply = generated_reply + s
        return HumanMessage(content=generated_reply)
