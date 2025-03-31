import importlib
import os
import queue

from dataclasses import dataclass, field

from ragent.core.constants import RAGENT_RUNTIME_ENV_VAR


@dataclass
class AgentTask:
    id: int
    goal: str
    sender: str
    receiver: str
    need_reply: bool = field(default=True)
    # The last AgentTask that triggers this
    caused_by: str = field(default=None)
    metadata: dict = field(default_factory=dict)


@dataclass
class ToolSpec:
    name: str
    desc: str
    type: str = field(default="PythonCode")
    input: dict = field(default_factory=dict)
    output: str = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    def __hash__(self):
        return hash((self.desc,))

    def __eq__(self, other):
        if not isinstance(other, ToolSpec):
            return NotImplemented
        return self.desc == other.desc


class EnvContext:
    def __init__(self, env_msg_queue: queue.Queue = None) -> None:
        self._shared_msg_queue = env_msg_queue

    @property
    def name(self):
        return self._name

    @staticmethod
    def from_config(config: dict):
        env_context = EnvContext()
        env_context._name = config.get("name", "Environment")
        env_context._runtime = config.get(
            "runtime", os.getenv(RAGENT_RUNTIME_ENV_VAR, "LOCAL")
        ).upper()

        RemoteQueue = importlib.import_module("ray.util.queue")
        env_context._shared_msg_queue = RemoteQueue.Queue(
            maxsize=config.get("max_inflight_task", 0)
        )
        return env_context
