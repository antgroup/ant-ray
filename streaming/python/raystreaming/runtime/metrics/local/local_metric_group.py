from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

from raystreaming.runtime.metrics.metric_group import MetricGroup
from raystreaming.runtime.metrics.timer import Timer

logger = logging.getLogger(__name__)


class LocalMetricGroup(MetricGroup):
    def __init__(self, metric_scope, conf):
        logger.info(
            "Local metrics client initializing with conf: {}".format(conf))

    def get_counter(self, metric_name, tags={}):
        pass

    def get_gauge(self, metric_name, tags={}):
        pass

    def get_meter(self, metric_name, tags={}):
        pass

    def get_timer(self, metric_name, tags={}):
        pass

    def get_metrics_snapshot(self) -> dict:
        return {}


class LocalTimer(Timer):
    def _current_milli_time(self):
        return int(round(time.time() * 1000))

    def __init__(self):
        self._start_time = self._current_milli_time()

    def start_duration(self):
        self._start_time = self._current_milli_time()

    def start_duration_with_time(self, start_time):
        self._start_time = start_time

    def observe_duration(self):
        duration = self._current_milli_time() - self._start_time
        logger.debug("Local timer duration: {}".format(duration))
