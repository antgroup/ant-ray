import asyncio
import inspect
import secrets
import logging
from autogen import UserProxyAgent
from typing import List, Dict, Optional, Union

from ragent.core.schemas import AgentTask
from ragent.core.agent import BaseAgent

logger = logging.getLogger(__name__)


# TODO(paer): Align LLM initialization
class AutogenConversableAgent(BaseAgent):
    def __init__(self, object_or_func, name=None, **kwargs):
        super().__init__(name=name, **kwargs)
        if inspect.isfunction(object_or_func):
            self.agent_create_func = object_or_func
            self.agent = self.agent_create_func()
        else:
            self.agent = object_or_func
        self._inner_user_proxy = None

    def query(self, query, config=None):
        """
        User interface, directly send a task to the agent
        and get the result.
        AutoGen consider any message is sent by an Agent,
        including the User, so the user message in proxied
        by an inner UserProxyAgent.
        Args:
            query: The instruction to the agent.
            config: The configuration of the agent.
        """
        if self._inner_user_proxy is None:
            self._inner_user_proxy = UserProxyAgent(
                self.name + "_inner_user_proxy",
                human_input_mode="NEVER",
                code_execution_config=False,
                default_auto_reply="",
                is_termination_msg=lambda x: True,
            )

        return self._receive_task(
            task=query,
            sender=self._inner_user_proxy,
            receiver=self.name,
            need_reply=True,
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
        # self._task_queue.put(task)
        # return task.id
        # NOTE(paer): Adapt `AgentTask` to MetaGPT's `Message` schema
        task = task.__dict__
        task["content"] = task["goal"]
        return self.invoke(messages=task, sender=task["sender"])

    def invoke(self, *args, **kwargs):
        if "messages" in kwargs and type(kwargs["messages"]) is not list:
            kwargs["messages"] = [kwargs["messages"]]
        return self.agent.generate_reply(*args, **kwargs)
        # return asyncio.run(self.ainvoke(*args, **kwargs))

    def profile(self, profile):
        pass

    def reason(self, goal, strategy="Chain-of-Thoughts"):
        pass

    def action(self, task):
        pass

    def construct_memory(self, session_id: str):
        pass

    def update_memory(self, session_id, memory):
        pass
