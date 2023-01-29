from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta, abstractmethod


class MetricGroup(metaclass=ABCMeta):
    """
    Abstract metric client
    """

    @abstractmethod
    def get_counter(self, metric_name, tags={}):
        """
        Initiate a counter metric
        A counter is a cumulative metric that represents a single monotonically
        increasing counter whose value can only increase or be reset to zero
        on restart.
        :param metric_name: the value of current metric
        """
        raise NotImplementedError("method not implemented!")

    @abstractmethod
    def get_gauge(self, metric_name, tags={}):
        """
        Initiate a gauge metric
        A gauge is a metric that represents a single numerical value that can
        arbitrarily go up and down
        :param metric_name: the value of current metric
        """
        raise NotImplementedError("method not implemented!")

    @abstractmethod
    def get_meter(self, metric_name, tags={}):
        """
        Initiate a meter metric
        :param metric_name: the value of current metric
        :param labelvalues: the label values of current metrics
        """
        raise NotImplementedError("method not implemented!")

    @abstractmethod
    def get_timer(self, metric_name, tags={}):
        """
        Initiate a  timer metric
        :param metric_name: the value of current metric
        :param labelvalues: the label values of current metrics
        :return: Timer
        """
        raise NotImplementedError("method not implemented!")

    @abstractmethod
    def get_scope(self):
        raise NotImplementedError("method not implemented!")

    @abstractmethod
    def get_metrics_snapshot(self) -> dict:
        """
        Fetch all of registerd metric information
        :return: MetricDump
        """
        raise NotImplementedError("method not implemented!")
