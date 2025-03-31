import inspect

from ragent.core.runnables.runnable import RagentRunnable


class LambdaRunnable(RagentRunnable):
    def __init__(self, func):
        """Langchain instance cannot be serialized directly
        as the other Runnable instances do. Therefore, a factory
        function is needed which will be serialized and get called
        in Executor.
        """
        super().__init__()
        self._func = func

    def invoke(self, input, config=None):
        return self._func(input, config)
