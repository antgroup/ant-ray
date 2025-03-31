import asyncio

import ray
import logging

from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class RagentRunnable(ABC):
    def __init__(self):
        self._options = {}

    @property
    def parallelism(self):
        if hasattr(self, "_options"):
            return self._options.get("parallelism", 1)
        return 1

    def options(self, **kwargs):
        self._options.update(kwargs)
        return self

    def __or__(self, other):
        """Compose this runnable with another object to create a
        RunnableSequence."""
        from ragent.core.graph.graph import RagentGraph

        return RagentGraph(self, other)

    def __ror__(self, other):
        """Compose this runnable with another object to create a
        RunnableSequence."""
        from ragent.core.graph.graph import RagentGraph

        return RagentGraph(other, self)

    @abstractmethod
    def invoke(self, input, config=None):
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

    async def ainvoke(self, input, config):
        return asyncio.get_running_loop().run_in_executor(
            None, self.invoke, input, config
        )

    #
    # @abstractmethod
    # def batch(self, inputs, config):
    #
    # @abstractmethod
    # async def abatch(self, inputs, config):

    @classmethod
    def remote(cls, *args, **kwargs):
        """Create a remote actor for this runnable.

        Returns:
            A handle of the remote actor.
        """
        return ray.remote(cls).remote(*args, **kwargs)
