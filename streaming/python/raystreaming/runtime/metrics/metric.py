from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta, abstractmethod


class Meter(metaclass=ABCMeta):
    @abstractmethod
    def mark(self, value: int = 1):
        raise NotImplementedError("method is mot implemented")

    @abstractmethod
    def get_count(self):
        raise NotImplementedError("method is mot implemented")

    @abstractmethod
    def get_rate(self):
        raise NotImplementedError("method is mot implemented")

    @abstractmethod
    def update(self):
        raise NotImplementedError("method is mot implemented")


class Gauge(metaclass=ABCMeta):
    @abstractmethod
    def record(self, value: float, tags: dict = None) -> None:
        raise NotImplementedError("method is mot implemented")


class Sum(metaclass=ABCMeta):
    @abstractmethod
    def record(self, value: float, tags: dict = None) -> None:
        raise NotImplementedError("method is mot implemented")


class Histogram(metaclass=ABCMeta):
    @abstractmethod
    def record(self, value: float, tags: dict = None) -> None:
        raise NotImplementedError("method is mot implemented")
