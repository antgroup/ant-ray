import os
import pytest
import sys

from langchain_core.prompts import ChatPromptTemplate

from ragent.core.graph.graph import RagentGraph
from ragent.core.runnables.langchain_runnable import LangchainRunnable


def test_ragent_graph():
    template = "Answer the question: {question}"
    prompt = ChatPromptTemplate.from_template(template)
    flow = RagentGraph(LangchainRunnable(prompt))

    res = flow.invoke({"question": "What's llm?"})
    print(f"Result: {res}")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
