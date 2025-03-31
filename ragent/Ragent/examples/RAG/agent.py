from ragent.agents.cot_agent import CoTAgent
from ragent.llms import AntLLM, AmaxLLM
from ragent.prompts.tool_prompt import INTERESTING_EXAMPLES
from ragent.tools.ant_sort import ant_sort
from ragent.tools.index_docs import index_documents
from ragent.tools.semantic_search import semantic_search

if __name__ == "__main__":
    agent = CoTAgent().profile(
        {
            "llm": AmaxLLM(model_name="Qwen_72B_Chat_vLLM"),
            # "llm": AmaxLLM(model_name="qwen_14b_chat_hf"),
            "examples": INTERESTING_EXAMPLES,
        }
    )
    agent.register_tools(index_documents, ant_sort, semantic_search)

    # Task 1: Learn from documents
    # EFS_DIR = Path("/Users/paer/Documents/work/codes/RAG_ray")
    # DOCS_DIR = Path(EFS_DIR, "docs.ray.io/small/master/")
    # data = [{"path": path._str} for path in DOCS_DIR.rglob("*.html") if not path.is_dir()]
    # data = data[:2]
    # result = agent.invoke(f"Learn information from the following documents: {data}")
    # print(f"Agent result: {type(result)} {result}")

    # Task 2: Question Answering
    result = agent.ask(f"What is RayDP in ray 3.0.0?", None)
    print(f"Agent result: {type(result)} {result}")
