import os
from langchain_community.vectorstores import Chroma
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough


# Example for document loading (from url), splitting, and creating vectostore
from ragent import Tool
from ragent.core.runnables.langchain_runnable import LangchainRunnable
from ragent.core.runnables.runnable import RagentRunnable
from ragent.embeddings.gte_base_embedding import GteBaseEmbedding
from ragent.llms.amax_llm import AmaxLLM
from udf import MockLLM

"""
# Load
from langchain_community.document_loaders import WebBaseLoader
loader = WebBaseLoader("https://lilianweng.github.io/posts/2023-06-23-agent/")
data = loader.load()

# Split
from langchain.text_splitter import RecursiveCharacterTextSplitter
text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=0)
all_splits = text_splitter.split_documents(data)

# Add to vectorDB
vectorstore = Chroma.from_documents(documents=all_splits, 
                                    collection_name="rag-chroma",
                                    embedding=OpenAIEmbeddings(),
                                    )
retriever = vectorstore.as_retriever()
"""

# Embed a single document as a test
vectorstore = Chroma.from_texts(
    ["harrison worked at kensho"],
    collection_name="rag-chroma",
    embedding=GteBaseEmbedding(),
)
retriever = vectorstore.as_retriever()

# RAG prompt
template = """Answer the question based only on the following context:
{context}

Question: {question}
"""
prompt = ChatPromptTemplate.from_template(template)

# LLM
model = MockLLM()

question = "介绍一下蚂蚁集团"


def create_prefill_executor():
    # Embed a single document as a test
    vectorstore = Chroma.from_texts(
        ["harrison worked at kensho"],
        collection_name="rag-chroma",
        embedding=GteBaseEmbedding(),
    )
    retriever = vectorstore.as_retriever()

    return RunnableParallel({"context": retriever, "question": RunnablePassthrough()})


class MyLLM(RagentRunnable):
    def __init__(self, model_name):
        super().__init__()
        self._model_name = model_name

    def invoke(self, input, config):
        return f"Model[{self._model_name}]: Hello world {input}"


@Tool(desc="")
def UDFPassthrough(input, config):
    return input


os.environ["RAGENT_RUNTIME"] = "RAY"  # "RAY"
# os.environ["RAGENT_RUNTIME"] = "LOCAL"

flow = (
    LangchainRunnable(create_prefill_executor).options(parallelism=1)
    | LangchainRunnable(prompt)
    | AmaxLLM()
    | UDFPassthrough
    | LangchainRunnable(StrOutputParser())
)


output = flow.invoke(question)
print(f"Final output: {output}")
