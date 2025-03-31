import ray
import logging

from ragent.core.executors.executor import Executor
from ragent.core.executors.ray_executor_container import ExecutorContainer

logger = logging.getLogger(__name__)

from ragent.core.config import ragent_config


class RayExecutor(Executor):
    def __init__(self, runnable):
        self.runnable = runnable
        self._ray_actor_handle = ExecutorContainer.options(runnable._options).remote(
            runnable, log_level=ragent_config.log_level
        )

    def execute(self, *args, **kwargs):
        timeout = kwargs.get("timeout", 30)
        return ray.get(self.submit(*args, **kwargs), timeout=timeout)

    def submit(self, *args, **kwargs):
        """
        Only return the remote ObjectRef instead waiting for the result.
        """
        return self._ray_actor_handle.execute.remote(*args, **kwargs)

    def OutputType(self):
        return self.runnable.OutputType()
