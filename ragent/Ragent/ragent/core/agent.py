import asyncio
import logging
import secrets
import queue
import time
import threading

from ray.util.queue import Queue
from collections import deque
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union
from langchain.llms.base import LLM

# from ragent.core.environment import Environment
from ragent.core.runnables import ToolRunnable, RagentRunnable
from ragent.core.utils import setup_logger
from ragent.core.tools.tools_manager import ToolsManager
from ragent.core.tools.msg_poller import MessagePoller
from ragent.core.schemas import AgentTask, EnvContext


logger = logging.getLogger(__name__)


class BaseAgent(RagentRunnable, ABC):
    def __init__(
        self, name: str = None, env_context: EnvContext = None, log_level="INFO"
    ):
        """Initialize the agent."""
        super().__init__()
        self._name = name
        if name is not None:
            self.options(name=name)

        setup_logger(log_level=log_level, agent_name=self._name)

        self._llm: Optional[LLM] = None
        # TODO: local mode shouldn't use ray queue, but Agent can't
        # sense the local/remote mode, so the code here can't switch
        # the queue type based on the mode.
        self._task_queue = Queue()
        self._reply_queue: List[asyncio.Task] = []
        self._memory: Dict = {}
        self._tool_manager: ToolsManager = ToolsManager()
        self._msg_poller = MessagePoller(
            self.run_task,  # msg handler
            self._task_queue,
            thread_name=f"agent_{name}_polling_thread",
        )
        self._env_context = env_context
        self._env_msg_queue = (
            env_context._shared_msg_queue
            if hasattr(env_context, "_shared_msg_queue")
            else None
        )

    @property
    def name(self) -> str:
        """The name of the agent."""
        return self._name

    @property
    def parallelism(self) -> int:
        """The parallelism of the agent. The agent should be an Actor,
        instead a executable function, so the parallelism should only be 1"""
        return 1

    @abstractmethod
    def profile(self, profile):
        """The profile of the agent. The profile is the 'role' of the agent
        to play.

        Args:
            profile: The 'role' of the agent to play.
        """

    @abstractmethod
    def reason(self, goal, strategy="Chain-of-Thoughts"):
        """Reasoning is the process of finding a sequence of actions
        that will lead to a goal.

        Args:
            goal: The goal to achieve.
            strategy: The strategy to use for reasoning. Available
                strategies are:
                - Chain-of-Thought: The agent will try to find
                    a sequence of actions in one shot that will
                    lead to the goal.
                - Tree-of-Thought: The search space of actions
                    is a tree, where agent will take each of the
                    possible actions and then recursively search
                    for the next action.
                - Random: The agent will randomly pick an action
                    from the tool set.
        """

    @abstractmethod
    def action(self, task):
        """Perform the specific action of current goal, e.g. calling
        a tool.

        Args:
            task: The sub-task, i.e. the single action this
                agent should perform.
        """

    @abstractmethod
    def construct_memory(self, session_id: str):
        """Construct the memory of the agent. The memory is the
        sequence of thoughts and actions the agent has performed.

        Args:
            session_id: The id for maintaining the session
        """

    @abstractmethod
    def update_memory(self, session_id, memory):
        """Update the memory of the agent."""

    def use_tool(self, tool_name: str, input_variables):
        """Use a tool with the given name. Result type must be understandable to
        LLM, e.g. a string, a json."""
        self._tool_manager.use_tool(tool_name, input_variables)

    def register_tools(self, *tool: ToolRunnable):
        """Register a tool to the agent with tool's name as the key."""
        self._tool_manager.register_tools(*tool)

    def elaborate_tools(self):
        """Elaborate the tools registered to the agent."""
        tool_specs = "\n"
        for i, tool in enumerate(self._tool_manager.elaborate_tools()):
            tool_specs += f"\nTool {i}:\n{tool}"
        return tool_specs

    @property
    def max_capability(self):
        """The max task completion times in one session.
        One "Thought-Action" count as one task completion"""
        return 20

    # @PublicAPI
    def query(self, query: str, config=None):
        """
        User interface, directly send a task to the agent
        and get the result.

        Args:
            query: The instruction to the agent.
            config: The configuration of the agent.
        """
        return self._receive_task(
            task=query, sender="User", receiver=self.name, need_reply=True
        )

    def _receive_task(
        self,
        task: Union[str, AgentTask],
        sender: str = None,
        receiver: str = None,
        need_reply: bool = True,
    ):
        """
        Receive an agent task from a dispatcher: environment, other
        agents, or the User(via `ask`), and put it to the task queue. The task
        will be populated and replied to the dispatcher when completed.

        Args:
            task: The task to be processed by the agent.
        """
        if isinstance(task, str):
            assert sender is not None, "sender must be specified when task is a string."
            session_id = secrets.token_hex(4)
            task = AgentTask(
                id=session_id,
                goal=task,
                sender=sender,
                receiver=self.name if receiver is None else receiver,
                need_reply=need_reply,
            )
        assert isinstance(
            task, AgentTask
        ), "Task must be a string or an AgentTask object."
        self._task_queue.put(task, timeout=10)
        return task.id

    def run_task(self, agent_task: AgentTask = None):
        """
        Poll the task from the task list. Kinda like "observe".
        - If the task list is empty, wait for the task to come
            until the timeout.
        - If the task is newly added, cast it to the main thread
            for execution.
        - If the task is completed, i.e. has result value, send
            the result to the dispatcher.
        """
        if agent_task is None:
            return None

        logger.info(f"Agent {self.name} starts running task {agent_task.id}")
        # result is also an AgentTask object
        result: AgentTask = self.invoke(agent_task)
        logger.info(f"Agent {self.name} finishes task {agent_task.id}")
        if result is not None:
            result.caused_by = agent_task.id
        self.reply_task(result)
        return result.id if result is not None else None

    def reply_task(self, agent_task: AgentTask):
        """
        Reply the task to the dispatcher. The reply will be polled
        from the environment side.
        """
        try:
            if agent_task is not None:
                logger.info(f"Task reply {agent_task.id}.")
                # TODO: Handle the case that no environment is
                # involved, i.e. the agent is running in local mode.
                self._env_msg_queue.put(agent_task, timeout=60)
            return True
        except Exception as e:
            logger.error(
                f"Put task {agent_task.id} reply into environment"
                f" got {e}. There maybe network issue"
                " or the enviroment is currently overloaded."
                " putting it back for later retry."
            )
            # TODO: set timer for later queue.put? I'm tired of maintaining
            # a separate reply queue anymore.
            return None  # return False will end the polling thread

    @abstractmethod
    def invoke(self, task, config=None):
        """
        Execute the task and return the result. Overwrite this method
        to implement the specific task execution logic, e.g. executing
        in a "Chain-of-Thought" strategy, or "Tree-of-Thought" strategy.
        The result will be put into the task list and replied to the
        dispatcher.

        Args:
            task: The task to be executed.
        Returns:
            The result of the task.
        """

    def join(self, env):
        """Join the environment."""
        pass
        # self._env = env

    def run(self):
        self._msg_poller.start()

    def stop(self):
        self._msg_poller.stop()
