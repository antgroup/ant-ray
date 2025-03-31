import inspect
import functools
import logging

from ragent.core.runnables.tool_runnable import ToolRunnable, ToolSpec

logger = logging.getLogger(__name__)


def Tool(*args, **kwargs):
    def decorator(function_or_class, **kwargs):
        if inspect.isfunction(function_or_class):
            # Get the function's docstring
            docstring = inspect.getdoc(function_or_class)

            if "desc" in kwargs or docstring is not None:
                if "desc" in kwargs and docstring and kwargs.get("desc") != docstring:
                    logger.warning(
                        f"Overwriting function {function_or_class.__name__} "
                        f"description with `desc` instead of the docstring."
                    )
            else:
                logger.warning(
                    f"No description provided for {function_or_class.__name__}, "
                    f"making it hard to be understand and called by Agent."
                )
            desc = kwargs.get("desc", docstring)

            parameters = inspect.signature(function_or_class).parameters
            input_param_desc = {
                param[0]: str(param[1])  # (name, annotation)
                for param in parameters.items()
            }

            tool_spec = ToolSpec(
                function_or_class.__name__,
                desc=desc,
                input=input_param_desc,
                output=kwargs.get("output", "Any"),
                metadata=kwargs.get("metadata", {}),
            )
            return ToolRunnable(function_or_class, tool_spec)
        raise TypeError("The @Tool decorator must be applied to a function.")

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @Tool
        return decorator(args[0])
    assert len(args) == 0 and len(kwargs) > 0, "Remote args error."
    return functools.partial(decorator, **kwargs)
