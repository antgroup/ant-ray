import logging
import os
import sys
from typing import Optional, List

import ray

from ragent.core.constants import RuntimeEngineType, RAGENT_RUNTIME_ENV_VAR
from ragent.core.logic_executors.local_logic_executor import LocalLogicExecutor
from ragent.core.logic_executors.ray_logic_executor import RayLogicExecutor
from ragent.core.runnables.runnable import RagentRunnable

logger = logging.getLogger(__name__)


class RagentGraph:
    def __init__(self, *steps, name: Optional[str] = None) -> None:
        """Create a new RunnableSequence.

        Args:
            steps: The steps to include in the sequence.
        """
        self._logic_executors = []
        self.runtime_engine = None
        self.is_initialized = False

        steps_flat: List[RagentRunnable] = []
        for step in steps:
            if isinstance(step, RagentGraph):
                steps_flat.extend(step.steps)
            else:
                steps_flat.append(step)

        self._steps = steps_flat
        self.name = name

    def __or__(self, other):
        """Compose this runnable with another object to create a
        RagentRunnableSequence."""
        if isinstance(other, RagentGraph):
            return RagentGraph(
                self,
                other,
                name=self.name or other.name,
            )
        else:
            return RagentGraph(
                self,
                other,
                name=self.name,
            )

    @property
    def steps(self):
        """All the runnables that make up the sequence in order."""
        return self._steps

    def invoke(self, input, config=None):
        """
        Only calling `ray.get` on the last step to avoid sequential execution.
        """
        if not self.is_initialized:
            self.init()

        for logic_executor in self._logic_executors:
            # calling `__call__` instead of `invoke`, to avoid calling `ray.get` sequentially
            # step is a list of executors corresponding to the same `Runnable`
            input = logic_executor.execute(input, config)
        return input

    def ainvoke(self, input, config=None):
        """
        Only return the remote ObjectRef instead waiting for the result.
        """
        if not self.is_initialized:
            self.init()

        for logic_executor in self._logic_executors:
            # calling `__call__` instead of `invoke`, to avoid calling `ray.get` sequentially
            # step is a list of executors corresponding to the same `Runnable`
            input = logic_executor.submit(input, config)
            # print(input)
        return input

    def init(self, runtime_engine: RuntimeEngineType = None):
        """Initiate the logic executor for the runnable. All executors
        should be appended into `self._executors` so that they can be used.

        Args:
            runtime_engine: The type of the runtime engine. Available names:
               ['RAY', 'RAY_DATA', LOCAL'].
        """
        if runtime_engine:
            self.runtime_engine = runtime_engine
        if not self.runtime_engine:
            self.runtime_engine = RuntimeEngineType[
                os.environ.get(RAGENT_RUNTIME_ENV_VAR, "RAY").upper()
            ]
        logger.info(
            f"Initializing RagentGraph with runtime engine: {self.runtime_engine} ..."
        )

        if self.runtime_engine == RuntimeEngineType.RAY:
            logic_executor_cls = RayLogicExecutor
            if not ray.is_initialized:
                ray.init()
        elif self.runtime_engine == RuntimeEngineType.LOCAL:
            logic_executor_cls = LocalLogicExecutor
        else:
            raise NotImplementedError(
                f"Not support runtime engine: {self.runtime_engine}"
            )

        for step in self.steps:
            self._logic_executors.append(logic_executor_cls(step))

        self.is_initialized = True

    # NOTE(paer): The following methods are compromised for `BaseAgent`
    # since `RagentRunnable`(including `BaseAgent`) must be wrapped
    # in `RagentGraph` to be executed remotely.
    def query(self, input, config=None):
        assert len(self._steps) == 1, "Only one Agent is allowed in RagentGraph"
        return self._steps[0].ask(input, config)

    def _receive_task(
        self, task, sender: str = None, receiver: str = None, need_reply: bool = True
    ):
        assert len(self._steps) == 1, "Only one Agent is allowed in RagentGraph"
        return self._steps[0]._receive_task(task, sender, receiver, need_reply)
