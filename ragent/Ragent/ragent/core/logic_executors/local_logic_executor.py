import logging

from ragent.core.executors.local_executor import LocalExecutor
from ragent.core.logic_executors.logic_executor import LogicExecutor
from ragent.core.runnables.runnable import RagentRunnable

logger = logging.getLogger(__name__)


class LocalLogicExecutor(LogicExecutor):
    def __init__(self, runnable: RagentRunnable):
        super().__init__(runnable)
        self.init_executor()

    def init_executor(self):
        """Initiate the runtime executor for the runnable. All executors
        should be appended into `self._executors` so that they can be used.
        """
        logger.info(
            f"Start to init executor of {repr(self)}, parallelism: {self.parallelism}"
        )
        for _ in range(self.parallelism):
            self._executors.append(LocalExecutor(self._runnable))
