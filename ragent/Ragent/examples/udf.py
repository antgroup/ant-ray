from typing import (
    Any,
    List,
    Optional,
)
from langchain.llms.base import LLM
from langchain.callbacks.manager import (
    CallbackManagerForLLMRun,
)
from langchain.embeddings.fake import FakeEmbeddings


class MockLLM(LLM):
    _udf_name: str = "udf"

    @property
    def _llm_type(self) -> str:
        """Return type of llm."""
        return "test-llm"

    @property
    def input_keys(self) -> List[str]:
        return ["input"]

    @property
    def output_keys(self) -> List[str]:
        return ["result"]

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """Run the LLM on the given prompt and input."""
        # prompt 可能用 '\n' 分割了 system content 和 user content，可尝试切分后分别传入 LLM
        return "test llm output"


def get_embedding_model(embedding_model_name, model_kwargs, encode_kwargs):
    embedding_model = FakeEmbeddings(size=768)
    return embedding_model
