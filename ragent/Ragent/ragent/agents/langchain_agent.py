from ragent.core.agent import BaseAgent
from langchain.agents import AgentExecutor, create_react_agent
from langchain_openai import OpenAI
from langchain import hub
import ray


class LangChainAgent(BaseAgent):
    def __init__(self, llm, tools, prompt_name):
        if llm == "openai":
            self._llm = OpenAI()
        else:
            raise ValueError("Unsupported LLM")
        self._tools = tools
        self._prompt = hub.pull(prompt_name)
        self._agent = create_react_agent(self._llm, tools, self._prompt)
        self._agent_executor = AgentExecutor(
            agent=self._agent, tools=tools, verbose=True
        )

    def profile(self, profile):
        self._profile = profile
        return self

    def step(self, task, **kwargs):
        pass

    def reason(self, goal, strategy="Chain-of-Thoughts"):
        pass

    def action(self, llm_output):
        pass

    def construct_memory(self, session_id: str):
        pass

    def update_memory(self, session_id, memory):
        pass

    def tool_using(self, tool_name: str, input_variables):
        pass

    def invoke(self, goal):
        return self._agent_executor.invoke(
            {
                "input": goal,
                "chat_history": "",
            }
        )
