import os
import pytest
import sys

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from ragent.core.runnables.langchain_runnable import LangchainRunnable
from ragent.llms.amax_llm import AmaxLLM


def test_amax_llm():
    llm = AmaxLLM()
    print(llm.generate("What's llm?"))


def test_amax_llm_in_chain():
    template = "Answer the question: {question}"
    prompt = ChatPromptTemplate.from_template(template)
    flow = (
        LangchainRunnable(prompt)
        | AmaxLLM(model_name="qwen_14b_chat_hf")
        | LangchainRunnable(StrOutputParser())
    )
    res = flow.invoke({"question": "What's llm?"})
    print(f"Result: {res}")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
