import inspect
import ray
import logging


@ray.remote
class ExecutorContainer:
    def __init__(self, class_or_func_def, log_level=logging.INFO):
        logging.basicConfig(level=log_level)
        if inspect.isfunction(class_or_func_def):
            self.runnable = class_or_func_def()
        else:
            self.runnable = class_or_func_def

    def execute(self, *args, **kwargs):
        return self.runnable.invoke(*args, **kwargs)
