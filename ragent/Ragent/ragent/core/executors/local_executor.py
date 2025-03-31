import logging

from ragent.core.executors.executor import Executor
from ragent.core.runnables.runnable import RagentRunnable

logger = logging.getLogger(__name__)


class LocalExecutor(Executor):
    def __init__(self, runnable: RagentRunnable):
        self.runnable = runnable

    def execute(self, *args, **kwargs):
        return self.runnable.invoke(*args, **kwargs)

    def submit(self, *args, **kwargs):
        return self.runnable.ainvoke(*args, **kwargs)
