import enum
import logging
import os

logger = logging.getLogger(__name__)


class ExceptionInjectorType(enum.IntEnum):
    ABNORMAL_EXIT = 0
    OUT_OF_MEMORY = 1
    NULL_POINTER_EXCEPTION = 2
    MASTER_EXIT = 3
    MASTER_CONTAINER_EXIT = 4
    WORKER_CONTAINER_EXIT = 5


class ExceptionInjector:
    def __init__(self, exception_injector_type):
        self.exception_injector_type = \
            ExceptionInjectorType(exception_injector_type) \
            if type(exception_injector_type) is int \
            else exception_injector_type

    def execute(self):
        if self.exception_injector_type is ExceptionInjectorType.ABNORMAL_EXIT:
            logger.info("Throw abnormal exit.")
            os._exit(1)
        elif self.exception_injector_type is \
                ExceptionInjectorType.NULL_POINTER_EXCEPTION:
            raise TypeError
        elif self.exception_injector_type is \
                ExceptionInjectorType.CONTAINER_EXIT:
            logger.info(
                "Kill parent process because of container exit injection.")
            ppid = os.getppid()
            os.system("kill -9 {}".format(ppid))
        else:
            raise RuntimeError
