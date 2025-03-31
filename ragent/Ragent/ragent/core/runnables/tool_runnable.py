import inspect

from ragent.core.schemas import ToolSpec
from ragent.core.runnables.lambda_runnable import LambdaRunnable


class ToolRunnable(LambdaRunnable):
    def __init__(self, func, tool_spec: ToolSpec):
        """Langchain instance cannot be serialized directly
        as the other Runnable instances do. Therefore, a factory
        function is needed which will be serialized and get called
        in Executor.
        """
        super().__init__(func)
        self._tool_spec = tool_spec

    def __hash__(self):
        return hash(self._tool_spec)

    def __eq__(self, other):
        return self._tool_spec == other._tool_spec

    @property
    def tool_spec(self):
        return self._tool_spec

    @property
    def name(self):
        return self._tool_spec.name
