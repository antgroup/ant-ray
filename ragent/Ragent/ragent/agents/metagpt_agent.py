import asyncio
import inspect
import secrets
import logging
import threading
from typing import List, Dict, Optional, Union
from metagpt.actions import UserRequirement
from metagpt.schema import Message

from ragent.core.schemas import AgentTask, EnvContext
from ragent.core.agent import BaseAgent

logger = logging.getLogger(__name__)


# TODO(paer): Align LLM initialization
class MetaGptAgent(BaseAgent):
    def __init__(
        self,
        object_or_func,
        name: str = None,
        env_context: EnvContext = None,
        log_level="INFO",
    ):
        if isinstance(object_or_func, MetaGptAgent):  # 拷贝构造，只拷贝 func 否则 role 无法 remote
            super().__init__(object_or_func.name, env_context, log_level)
            self.role_create_func = object_or_func.role_create_func
            self.role = None
        else:
            super().__init__(name, env_context, log_level)
            if inspect.isfunction(object_or_func):
                self.role_create_func = object_or_func
                self.role = None
            else:
                self.role = object_or_func

        # A separate thread for executing async task
        self._task_handle_event_loop = None
        self._task_handle_thread = None

    def init_metagpt_role(self):
        if self.role is None:
            assert hasattr(self, "role_create_func")
            assert inspect.isfunction(self.role_create_func)
            assert hasattr(
                self._env_context, "metagpt_context"
            ), "Metagpt context must be provided"
            self.role = self.role_create_func(self._env_context.metagpt_context)

    def start_event_loop(self):
        asyncio.set_event_loop(self._task_handle_event_loop)
        self._task_handle_event_loop.run_forever()

    def invoke(self, task, *args, **kwargs):
        assert isinstance(task, AgentTask)

        message = self.agent_task_to_message(task)
        coro = self.role.run(with_message=message)
        future = asyncio.run_coroutine_threadsafe(coro, self._task_handle_event_loop)
        result = future.result()
        return self.message_to_agent_task(result)

    # NOTE(paer): If deploying as an actor on Ray, Ray will recognize
    # this actor as AsyncActor instead of ThreadedActor when there is
    # at least one async def method in actor definition. See
    # https://docs.ray.io/en/latest/ray-core/actors/async_api.html
    # for more details.
    # async def ainvoke(self, *args, **kwargs):
    #     # TODO: Wrap return value to some standard format
    #     return await self.role.run(*args, **kwargs)

    def run(self):
        super().run()
        self.init_metagpt_role()
        if self._task_handle_event_loop is None:
            self._task_handle_event_loop = asyncio.new_event_loop()
            self._task_handle_thread = threading.Thread(target=self.start_event_loop)
        self._task_handle_thread.start()

    def stop(self):
        super().stop()
        if self._task_handle_event_loop is not None:
            # NOTE(paer): Must stop the event loop inside the event loop,
            # details see https://docs.python.org/3/library/asyncio-dev.html#concurrency-and-multithreading  # noqa
            self._task_handle_event_loop.call_soon_threadsafe(
                lambda: self._task_handle_event_loop.stop()
            )
            self._task_handle_thread.join()

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

    def agent_task_to_message(self, agent_task: AgentTask):
        if agent_task is None:
            return None

        msg = Message(
            id=agent_task.id,
            content=agent_task.goal,
            sent_from=agent_task.sender,
            send_to=agent_task.receiver,
            instruct_content=agent_task.metadata.get("instruct_content", None),
            cause_by=agent_task.metadata.get("cause_by", UserRequirement),
        )
        return msg

    def message_to_agent_task(self, message: Message):
        if message is None:
            # FIXME(paer): Currently need an AgentTask with `need_reply=False`
            # to mark the Task is completed.
            return None
        metadata = {}
        if message.instruct_content is not None:
            metadata["instruct_content"] = message.instruct_content
        if message.cause_by is not None:
            metadata["cause_by"] = message.cause_by

        return AgentTask(
            id=message.id,
            goal=message.content,
            sender=message.sent_from,
            receiver=message.send_to,
            need_reply=True,
            metadata=metadata,
        )
