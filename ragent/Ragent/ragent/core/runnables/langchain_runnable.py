import inspect

from ragent.core.runnables.runnable import RagentRunnable


class LangchainRunnable(RagentRunnable):
    def __init__(self, class_or_func_def):
        """Langchain instance cannot be serialized directly
        as the other Runnable instances do. Therefore, a factory
        function is needed which will be serialized and get called
        in Executor.
        """
        super().__init__()
        self._runnable_def = class_or_func_def
        self.is_initialized = False
        self.runnable = None

    def invoke(self, input, config):
        if not self.is_initialized:
            self.init()
        return self.runnable.invoke(input, config)

    def init(self):
        if inspect.isfunction(self._runnable_def):
            self.runnable = self._runnable_def()
        else:
            self.runnable = self._runnable_def
        self.is_initialized = True
