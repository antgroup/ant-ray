import logging
from typing import Dict

from langchain_core.messages import HumanMessage


from ragent.llms.llm import convert_message_to_dict, LanguageModelInput, BaseLLM

logger = logging.getLogger(__name__)


class MockLLM(BaseLLM):
    def __init__(self, mock_map: Dict):
        self.mock_map = mock_map

    def generate(self, input: LanguageModelInput):
        messages = self._convert_input(input).to_messages()

        message_dicts = [convert_message_to_dict(m) for m in messages]
        prompt = ""
        for m in message_dicts:
            prompt += m.get("content", "")

        for key, value in self.mock_map.items():
            if key in prompt:
                return HumanMessage(content=value)

        return HumanMessage(content="")
