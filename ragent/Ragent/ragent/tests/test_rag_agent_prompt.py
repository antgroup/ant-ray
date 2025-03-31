import os

import pytest
import sys

from langchain_community.vectorstores.chroma import Chroma
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough

from ragent.core.runnables.langchain_runnable import LangchainRunnable
from ragent.embeddings.gte_base_embedding import GteBaseEmbedding
from ragent.llms.amax_llm import AmaxLLM


RAG_PROMPT = """

"""


def test_amax_llm():
    agent_system_message = """
    You are a retrieval augmented generation agent,
    Answer user questions based on your own knowledge base and contextual knowledge points provided by the user. If you don't know, say you don't know.
    In addition to the above abilities, Use tools (only if necessary) to best answer the users questions.
    Example Input:
    Please learn the documents ./data/docs.ray.io/en/master/ray-core/actors.html"
    Example Output:
    {{
        answer: {{
            speak: "I need use tool to learn document."
        }},
        tools: {{
            name: "learning_documents",
            args: "./data/docs.ray.io/en/master/ray-core/actors.html"
        }},
    }} 
    """

    tools_messages = """
    ## Tools
    These are the ONLY tools you can use. Any action you perform must be possible through one of these tools:
    1. learning_documents: 整体功能是让agent学习理解这个文档的内容。实际逻辑是将用户提供的文件经过数据切分、embedding、然后存入向量数据库，让下次用户提问时，可以从向量数据库中查询出相关的知识点，帮助你回答问题。Params: (doc_path: string)
    """

    user_messages = """context:
    {context}

    Question: {question}
    """

    response_messages = """Respond with pure JSON.:
    {{
        answer: {{
        speak: string;
        }},
        tools: {{
        name: string;
        args: Record<string, any>;
        }},
    }}
        """
    # 内部AMax平台不支持system role
    # prompt = ChatPromptTemplate.from_messages(
    #     [
    #         ("system", agent_system_message),
    #         ("system", tools_messages),
    #         ("user", user_messages),
    #         ("system", response_messages)
    #     ]
    # )
    prompt = ChatPromptTemplate.from_messages(
        [
            ("user", agent_system_message),
            ("user", tools_messages),
            ("user", user_messages),
            ("user", response_messages),
        ]
    )

    def create_prefill_executor():
        # Embed a single document as a test
        vectorstore = Chroma.from_texts(
            ["harrison worked at kensho"],
            collection_name="rag-chroma",
            embedding=GteBaseEmbedding(),
        )
        retriever = vectorstore.as_retriever()

        return RunnableParallel(
            {"context": retriever, "question": RunnablePassthrough()}
        )

    # os.environ["RAGENT_RUNTIME"] = "RAY"  # "RAY"
    os.environ["RAGENT_RUNTIME"] = "LOCAL"

    flow = (
        LangchainRunnable(create_prefill_executor).options(parallelism=1)
        | LangchainRunnable(prompt)
        | AmaxLLM()
    )

    print(
        flow.invoke(
            "Please learn the documents ./data/docs.ray.io/en/master/ray-core/actors.html"
        )
    )

    print(flow.invoke("What's Actor?"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
