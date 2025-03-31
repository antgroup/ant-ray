from abc import ABC, abstractmethod
import random
from typing import List

from ragent.core.executors.executor import Executor
from ragent.core.runnables.runnable import RagentRunnable


class LogicExecutor(ABC):
    def __init__(self, runnable: RagentRunnable):
        self._runnable: RagentRunnable = runnable
        self._executors: List[Executor] = []

    @property
    def parallelism(self):
        if self._runnable:
            return self._runnable.parallelism
        return 1

    def get_executor(self):
        rand_index = random.randint(0, len(self._executors) - 1)
        return self._executors[rand_index]

    def execute(self, input, config):
        """Submit the `invoke` as a task to the executor and get the result.

        Args:
            input: The input to the runnable.
            config: A config to use when invoking the runnable.
               The config supports standard keys like 'tags', 'metadata' for
               tracing purposes, 'max_concurrency' for controlling how much
               work to do in parallel, and other keys. Please refer to the
               RunnableConfig for more details.

        Returns:
            The output of the `invoke` method.
        """
        res = self.get_executor().execute(input, config)
        return res

    def submit(self, input, config):
        """Submit the `invoke` as a task to the executor and return the future
        standing for the result that can be retrived when the execution finish.

        Args:
            input: The input to the runnable.
            config: A config to use when invoking the runnable.
               The config supports standard keys like 'tags', 'metadata' for
               tracing purposes, 'max_concurrency' for controlling how much
               work to do in parallel, and other keys. Please refer to the
               RunnableConfig for more details.

        Returns:
            The future result of the `invoke` method.
        """
        res_future = self.get_executor().submit(input, config)
        return res_future
