import os
import ray
import queue
import logging
import importlib
import threading
import secrets
import time
from ray.util.queue import Queue
from typing import Union

from ragent.agents.metagpt_agent import MetaGptAgent
from ragent.agents.utils import select_agent_wrapper
from ragent.core.runnables import ToolRunnable, RagentRunnable
from ragent.core.utils import load_yaml_config, setup_logger
from ragent.core.constants import ENV_CONFIG_KEY, RAGENT_RUNTIME_ENV_VAR
from ragent.core.schemas import AgentTask, EnvContext
from ragent.core.agent import BaseAgent
from ragent.core.tools.msg_poller import MessagePoller

logger = logging.getLogger(__name__)


def load_env_config():
    env_config_path = os.getenv(ENV_CONFIG_KEY)
    if env_config_path is None:
        env_config_path = os.path.join(os.getcwd(), "default_env_config.yaml")

    logger.info(f"Loading environment config from {env_config_path}")
    env_config = load_yaml_config(env_config_path)
    return env_config


class Environment:
    def __init__(self, env_config: dict = None, context: EnvContext = None):
        super().__init__()

        if env_config is None:
            env_config = load_env_config()
        self._env_config = env_config

        if context is None:
            context = EnvContext.from_config(self._env_config)
        self._env_context = context

        self._name = self._env_context.name
        # self._name = self._env_config.get('name', 'Environment')

        setup_logger(
            log_level=self._env_config.get("log_level", "INFO"),
            log_file=self._env_config.get("log_file", None),
            agent_name=self._name,
        )

        self._is_local_mode = self._env_context._runtime == "LOCAL"

        self._task_queue = queue.Queue(
            maxsize=self._env_config.get("max_inflight_task", 0)
        )
        self._task_publisher = MessagePoller(
            self.publish_task,  # msg handler
            self._task_queue,
            thread_name="env_task_publish_thread",
        )

        # This is ray.util.queue.Queue, for distributed message passing
        self._reply_queue = self._env_context._shared_msg_queue
        self._task_reply_poller = MessagePoller(
            self.receive_task_reply,
            self._env_context._shared_msg_queue,
            thread_name="env_reply_polling_thread",
        )

        self._agents = []
        self._name2agent = {}
        self._history = []  # Should use persistent storage

    @property
    def name(self) -> str:
        """The name of the environment."""
        return self._name

    def run(self):
        """
        Start sub-thread to poll the message queue.
        """
        if self._task_publisher.is_started():
            assert (
                self._task_reply_poller.is_started()
            ), "The task publisher and reply poller MUST be started together"
            return

        if len(self._name2agent) == 0:
            logger.warning(
                "No agent joined before running the environment, "
                "the received tasks will still be polled and wasted"
            )

        logger.info(
            f"Starting environment {self._name} with {len(self._agents)} agents."
        )
        if not self._is_local_mode:
            run_ref = {name: agent.run.remote() for name, agent in self._name2agent.items()}
            _, failed = ray.wait(list(run_ref.values()), timeout=60)
            if failed:
                # Search for the agent name of unready task
                for name, task_ref in run_ref.items():
                    if task_ref in failed:
                        logger.warning(
                            f"Failed to start agent {name} under {60} timeout second."
                        )

        else:
            for agent in self._agents:
                agent.run()
        logger.info(f"Successfully started the environment {self._name}")
        # self.task_poll_thread.start()
        self._task_publisher.start()
        self._task_reply_poller.start()

    def stop(self):
        """
        Stop the sub-thread, and clean up the resources.
        """
        self._task_publisher.stop()
        self._task_reply_poller.stop()
        if self._is_local_mode:
            for agent in self._agents:
                agent.stop()
        else:
            stop_ref = {name: agent.stop.remote() for name, agent in self._name2agent.items()}
            _, failed = ray.wait(list(stop_ref.values()), timeout=60)
            if failed:
                # Search for the agent name of failed task
                for name, task_ref in stop_ref.items():
                    if task_ref in failed:
                        logger.warning(
                            f"Failed to stop agent {name} under {60} timeout second."
                        )

    def join(self, agent: BaseAgent):
        if agent in self._agents:
            return True
        agent_name = agent._name
        if agent_name in self._name2agent:
            raise ValueError(f"Agent name confict with: {self._name2agent[agent_name]}")
        if len(self._agents) >= self._env_config.get("max_agent_amount", 100):
            raise ValueError("Exceeds the maximum agent amount limit")

        agent_type = select_agent_wrapper(agent)
        # Temporary solution for adapt MetaGPT agent
        if isinstance(agent, MetaGptAgent):
            # 这个没法序列化
            if not hasattr(self._env_context, "metagpt_context"):
                logger.info(
                    "No MetaGPT Context found when joining its agent, initializing it."
                )
                setattr(
                    self._env_context,
                    "metagpt_context",
                    importlib.import_module("metagpt.context").Context(),
                )

        if not self._is_local_mode:
            agent = (
                ray.remote(agent_type)
                .options(**agent._options)
                .remote(
                    agent,
                    env_context=self._env_context,
                    log_level=self._env_config.get("log_level", "INFO"),
                )
            )
        else:
            agent._env_msg_queue = self._reply_queue
            agent._env_context = self._env_context

        self._agents.append(agent)
        self._name2agent[agent_name] = agent

    def publish_task(self, task: AgentTask):
        """
        Task handler where the polled task get executed. The task is published
        to all corresponding receivers and put into another queue waiting for
        the result.
        """
        receive_task_result = {}
        receivers = self._select_task_receiver(task)
        logger.info(
            f"Publishing task {task.id} to {list(receivers.keys())}"
        )
        # logger.info(f"Publishing Task {task.id} detail: {task}")
        for name, agent in receivers.items():
            # High risk of race condition when multi-agents, because
            # they share the same object whose attribute, i.e. `receiver`
            # get modified frequently.
            # task.receiver = name
            if self._is_local_mode:
                receive_task_result[name] = agent._receive_task(task)
            else:
                receive_task_result[name] = agent._receive_task.remote(task)

        if not self._is_local_mode:
            ready, unready = ray.wait(list(receive_task_result.values()), timeout=10)
            if unready:
                # Search for the agent name of unready task
                for name, task_ref in receive_task_result.items():
                    if task_ref in unready:
                        logger.warning(
                            f"Didn't get response from agent {name} of Task {task.id} under {10} timeout second."
                        )

        return True

    def _select_task_receiver(self, task: AgentTask):
        """
        Select the receiver agents for the task.
        """
        if task is None:
            return {}
        if isinstance(task.receiver, list):
            task_required_receiver = task.receiver
        elif isinstance(task.receiver, str):
            task_required_receiver = [task.receiver]
        elif isinstance(task.receiver, set):
            task_required_receiver = list(task.receiver)
        else:
            raise ValueError(f"Invalid receiver type: {type(task.receiver)}")

        task_required_receiver = set(task_required_receiver)
        if "<all>" in task_required_receiver:
            return self._name2agent
        return {
            name: agent
            for name, agent in self._name2agent.items()
            if name in task_required_receiver
        }

    def submit_query(self, query: AgentTask):
        """
        Submit a query to the environment.

        Args:
            query: The query task.
        """
        self.run()
        self._receive_task(query)
        return query.id

    def query(
        self,
        query: str,
        receiver=None,
        timeout_s=0,
        poll_interval_s=0.1,
        err_when_timeout=False,
    ):
        """
        Ask a question to the environment and wait for the reply
        until timeout.

        Args:
            query: The question string
            timeout_s: Timeout in seconds, -1 for infinite, 0 for instant return.
        """
        self.run()
        receiver = receiver or "<all>"
        query_id = self._receive_task(
            query, sender="User", receiver=receiver, need_reply=True
        )

        trace, is_query_completed = self.get_query_result(
            query_id, timeout_s=timeout_s, poll_interval_s=poll_interval_s
        )
        if not is_query_completed and timeout_s > 0:
            query_uncomplete_msg = (
                f"Query {query_id} didn't complete after " f"{timeout_s} seconds."
            )
            logger.warning(query_uncomplete_msg)
            if err_when_timeout:
                raise TimeoutError(query_uncomplete_msg)

        return trace

    def _receive_task(
        self,
        task: Union[AgentTask, str],
        sender: str = None,
        receiver: str = None,
        need_reply: bool = True,
    ):
        if isinstance(task, str):
            assert sender is not None, "sender must be specified when task is a string."
            session_id = secrets.token_hex(4)
            task = AgentTask(
                id=session_id,
                goal=task,
                sender=sender,
                receiver=self.name if receiver is None else receiver,
                need_reply=need_reply,
                caused_by=None,
            )
        self._task_queue.put(task, timeout=10)
        self._history.append(task)
        return task.id

    def receive_task_reply(self, task: Union[AgentTask, str]):
        logger.debug(f"Receive task reply from agent: {task.sender}.")
        if task is None:
            return None

        if task.caused_by is None:
            logger.error(f"The received task reply {task.id} doesn't have `caused_by`.")
            return

        if task.need_reply:
            logger.info(
                f"Task reply {task.id} need next round, putting into task queue."
            )
            logger.info(f"Task reply {task.id} detail: {task}")
            self._receive_task(task)
        else:
            self._history.append(task)
        return task.id

    def get_agent(self, name):
        return self._name2agent.get(name, None)

    def get_query_result(self, query_id, timeout_s=0, poll_interval_s=0.1):
        """
        Get the query status by query id.

        Args:
            query_id: The query id.
            timeout_s: Timeout in seconds,
                    - -1 for infinite
                    - 0 for instant return.
            poll_interval_s: The interval to poll the task status.
        """
        if timeout_s > 0 and poll_interval_s > timeout_s:
            logger.warning(
                f"Poll interval {poll_interval_s} is larger"
                f"than timeout {timeout_s}."
            )

        time_left_s = timeout_s
        logger.info(
            f"Start getting query {query_id} result with timeout {timeout_s} seconds, "
            f"interval {poll_interval_s}s."
        )
        # Query result long polling loop until get the task final result or timeout reached
        while True:
            # Find the initial query
            task = None
            trace = []
            is_query_completed = False
            if query_id is None:
                logger.warning("Query id is None, return directly.")
                break
            for completed_task in self._history:
                if completed_task.id == query_id:
                    task = completed_task
                    break
            if not task:
                logger.debug(
                    f"Query {query_id} not found in history, this "
                    "could happened when the query is not yet been "
                    "published."
                )
            else:
                # Find all solving traces
                target_task_id = task.id
                while True:
                    for trace_task in self._history:
                        if trace_task.caused_by == target_task_id:
                            trace.append(trace_task)
                            break
                    if len(trace) == 0 or target_task_id == trace[-1].id:
                        break
                    target_task_id = trace[-1].id
                # NOTE(paer): Task is completed if the last task doesn't need
                # reply. This could happend when:
                # - The initial task, i.e. `task`, doesn't need reply;
                # - The last task reply in the trace doesn't need reply.
                is_query_completed = (not task.need_reply) or (
                    len(trace) > 0 and not trace[-1].need_reply
                )

            if is_query_completed:
                logger.info(f"Query {query_id} is completed.")
                break
            else:
                if timeout_s == 0:
                    logger.info(
                        f"Query {query_id} is not completed yet. " "Directly return."
                    )
                    break

                logger.debug(
                    f"Query {query_id} didn't complete yet, "
                    f"will poll after {poll_interval_s}s..."
                )
                time.sleep(poll_interval_s)
                if timeout_s < 0:
                    continue
                else:
                    time_left_s -= poll_interval_s
                    if time_left_s <= 0:
                        logger.info(
                            f"Query {query_id} didn't completed "
                            f"after {timeout_s} seconds."
                        )
                        break
        # Build the execution chain
        execution_chain = ([task] if task else []) + trace
        return execution_chain, is_query_completed
