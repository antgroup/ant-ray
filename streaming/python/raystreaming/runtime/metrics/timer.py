from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta, abstractmethod


class Timer(metaclass=ABCMeta):
    @abstractmethod
    def start_duration(self):
        raise NotImplementedError("method is mot implemented")

    @abstractmethod
    def start_duration_with_time(self, start_time):
        raise NotImplementedError("method is mot implemented")

    @abstractmethod
    def observe_duration(self):
        raise NotImplementedError("method is mot implemented")
