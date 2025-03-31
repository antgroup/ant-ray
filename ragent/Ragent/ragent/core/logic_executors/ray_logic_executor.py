from ragent.core.executors.ray_executor import RayExecutor
from ragent.core.logic_executors.logic_executor import LogicExecutor
from ragent.core.runnables.runnable import RagentRunnable


class RayLogicExecutor(LogicExecutor):
    def __init__(self, runnable: RagentRunnable):
        super().__init__(runnable)

        self.init_executor()

    def init_executor(self):
        """Initiate the runtime executor for the runnable. All executors
        should be appended into `self._executors` so that they can be used.
        """
        for _ in range(self.parallelism):
            self._executors.append(RayExecutor(self._runnable))
