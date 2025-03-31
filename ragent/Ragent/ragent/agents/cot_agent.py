import logging

from colorama import Fore, Style
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate

from ragent.core.agent import BaseAgent
from ragent.core.constants import LLM_FINAL_ANSWER, LLM_GIVE_UP
from ragent.core.runnables import LangchainRunnable
from ragent.core.schemas import AgentTask
from ragent.core.utils import parse_thought_action, parse_function_call
from ragent.prompts.tool_prompt import TOOL_SELECT_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


class CoTAgent(BaseAgent):
    """
    A built-in agent that uses "Chain-of-Thoughts" strategy to complete a user
    task. The agent will keep the <reason-act> loop until the task is achieved
    or the agent gives up or the agent reaches the max loop limit.

    The memory format is a triplet of <thought, action, action_result>, where
    the thought and action is the output of the LLM and the action_result is
    the execution result of the determined action.
    """

    @property
    def task_example(self):
        examples = ""
        for i, example in enumerate(self._profile["examples"]):
            examples += f"\n\nExample {i}:\n{example}"
        return examples

    def profile(self, profile: dict):
        self._llm = profile.get("llm")
        self._profile = profile
        self._name = profile.get("name", None)
        return self

    def reason(self, prompt_template, goal, memory, strategy="Chain-of-Thoughts"):
        # NOTE: ragent LLM use this
        llm_chain = (
            LangchainRunnable(prompt_template)
            | self._llm
            | LangchainRunnable(StrOutputParser())
        )
        llm_output = llm_chain.invoke({"user_query": goal, "history": memory})
        logger.debug(f"Raw LLM output: {llm_output}")
        return parse_thought_action(llm_output)

    def construct_memory(self, session_id: str):
        if session_id not in self._memory:
            self._memory[session_id] = {"memory": []}

        session = self._memory.get(session_id)
        history = session["memory"]
        scratchpad = ""
        for i, (thought, action, action_result) in enumerate(history):
            scratchpad += f"Thought {i}: " + thought + "\n"
            scratchpad += f"Action {i}: " + action + "\n"
            scratchpad += f"Action result: " + action_result + "\n"
        return scratchpad

    def update_memory(self, session_id, memory):
        self._memory[session_id]["memory"].append(memory)

    def action(self, llm_output):
        logger.debug(f"LLM chooses action: {llm_output}")
        func_name, inputs = parse_function_call(llm_output)
        logger.debug(f"Parsed tools from llm, func_name: {func_name}, inputs: {inputs}")
        result = self.use_tool(func_name, inputs)
        logger.debug(f"Function calling result: {result}")
        return result

    def use_tool(self, tool_name: str, input_variables):
        result = self._tool_manager.use_tool(tool_name, **input_variables)
        return result

    def is_task_comlete(self, thought, action):
        if LLM_FINAL_ANSWER in thought or LLM_GIVE_UP in thought:
            return True
        if LLM_FINAL_ANSWER in action or LLM_GIVE_UP in action:
            return True
        return False

    def invoke(self, task: AgentTask, config=None):
        session_id = task.id
        logger.debug(f"Invoke task {task.id} goal: {task.goal}.")
        examples = ""
        for i, example in enumerate(self._profile["examples"]):
            examples += f"\n\nExample {i}:\n{example}"
        tools = self.elaborate_tools()

        final_answer = ""
        for sub_task_num in range(1, self.max_capability):
            prompt = PromptTemplate(
                template=TOOL_SELECT_PROMPT_TEMPLATE,
                partial_variables={
                    "tool_list": tools,
                    "examples": examples,
                    "answer_symbol": LLM_FINAL_ANSWER,
                    "give_up_symbol": LLM_GIVE_UP,
                },
                input_variables=["user_query", "history"],
            )
            memory = self.construct_memory(session_id)
            # thought + action
            thought, action = self.reason(prompt, task.goal, memory)
            print(f"{Fore.RED}Thought {sub_task_num}: {thought}{Style.RESET_ALL}")
            print(f"{Fore.GREEN}Action {sub_task_num}: {action}{Style.RESET_ALL}")
            if self.is_task_comlete(thought, action):
                final_answer = thought
                break
            result = self.action(action)
            print(f"{Fore.BLUE}Action {sub_task_num} result: {result}{Style.RESET_ALL}")
            self.update_memory(session_id, (thought, action, result))

        if final_answer == "":
            final_answer = "Exceed max capability, I give up."

        return final_answer

    def _act(self):
        pass
