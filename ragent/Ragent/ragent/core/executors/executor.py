from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class Executor(ABC):
    def OutputType(self):
        pass

    @abstractmethod
    def submit(self, input, *args, **kwargs):
        """Submit the execution task and return the result future"""

    @abstractmethod
    def execute(self, input, config):
        """Submit the execution task, wait for execution, and
        return the final result.
        """
