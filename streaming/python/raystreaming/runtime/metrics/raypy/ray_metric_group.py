from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
import threading

from ray.util.metrics import Gauge as RayGauge, Sum as RaySum
from raystreaming.constants import StreamingConstants
from raystreaming.runtime.metrics.metric_group import MetricGroup
from raystreaming.runtime.metrics.timer import Timer
from raystreaming.runtime.metrics.metric import Meter, Gauge, Sum
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class RayMetricGroup(MetricGroup):
    def __init__(self, scope, conf):
        logger.info(
            "Ray metrics client initializing with conf: {}".format(conf))
        self._global_tags = {}
        self._global_tag_keys = (StreamingConstants.METRICS_LABEL_JOB_NAME,
                                 StreamingConstants.METRICS_LABEL_HOST_IP,
                                 StreamingConstants.METRICS_LABEL_OP_NAME,
                                 StreamingConstants.METRICS_LABEL_WORKER_ID,
                                 StreamingConstants.METRICS_LABEL_WORKER_NAME,
                                 StreamingConstants.METRICS_LABEL_PID)
        self.scope = scope
        for key in self._global_tag_keys:
            if key in conf:
                self._global_tags[key] = conf[key]
        self._metric_map = {}
        self._lock = threading.Lock()

    def get_global_tags(self):
        return self._global_tags

    def register_metric(func):
        """
        Metric register decorator for caching all of metric instances.
        The mandatory parameters are metric_name, the optional parameters
        are tags and metric_prefix, and their default values are '{}' and
        'StreamingConstants.METRIC_WK_PREFIX'.
        """

        def inner(*args, **kwargs):
            instance = args[0]
            metric_name = args[1]
            tags = args[2] if len(args) > 2 and args[2] is not None else {}
            metric_prefix = args[3] if len(args) > 3 \
                else StreamingConstants.METRIC_WK_PREFIX
            full_metric_name = instance.__build_full_metric_name(
                metric_name, tags)
            # Making metric map in thread-safe.
            if instance._lock.acquire():
                if full_metric_name in instance._metric_map:
                    return instance._metric_map[full_metric_name]
                logger.info(f"Register metric : {full_metric_name}")
                metric = func(instance, metric_name, tags, metric_prefix)
                instance._metric_map[full_metric_name] = metric
                instance._lock.release()
                return metric
            return None

        return inner

    def merge_tag_keys(self, tags):
        tag_keys = self._global_tag_keys
        for key, value in tags.items():
            tag_keys = tag_keys + (key, ) if key not in tag_keys else tag_keys
        return tag_keys

    def get_scope(self):
        return self.scope

    @register_metric
    def get_counter(self,
                    metric_name,
                    tags=None,
                    metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        return RayMetricsSum(
            name=metric_wk_prefix + metric_name,
            description="A count metric",
            metrics_tags={
                **tags,
                **self._global_tags
            },
            tag_keys=self.merge_tag_keys(tags))

    @register_metric
    def get_gauge(self,
                  metric_name,
                  tags=None,
                  metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        return RayMetricsGauge(
            name=metric_wk_prefix + metric_name,
            description="A gauge metric",
            metrics_tags={
                **tags,
                **self._global_tags
            },
            tag_keys=self.merge_tag_keys(tags))

    @register_metric
    def get_timer(self,
                  metric_name,
                  tags=None,
                  metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        return RayMetricTimer(
            RayMetricsGauge(
                name=metric_wk_prefix + metric_name,
                description="A timer metric",
                metrics_tags={
                    **tags,
                    **self._global_tags
                },
                tag_keys=self.merge_tag_keys(tags)), self.__merge_tags(tags))

    @register_metric
    def get_meter(self,
                  metric_name,
                  tags=None,
                  metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        return RayMetricsMeter(
            RayGauge(
                name=metric_wk_prefix + metric_name,
                description="A meter metric",
                tag_keys=self.merge_tag_keys(tags)), self.__merge_tags(tags))

    def __build_full_metric_name(self, metric_name, tags):
        return metric_name + "\n".join(key + ":" + value
                                       for key, value in tags.items())

    def __merge_tags(self, tags):
        mixed_tags = self._global_tags.copy()
        mixed_tags.update(tags)
        return mixed_tags

    def inc_counter(self, counter, tags=None):
        if tags is None:
            tags = {}
        counter.record(1, self.__merge_tags(tags))

    def add_gauge(self, gauge, value, tags=None):
        if tags is None:
            tags = {}
        gauge.record(value, self.__merge_tags(tags))

    def add_meter(self, meter, value, tags=None):
        if tags is None:
            tags = {}
        meter.mark(value)
        meter.update(tags)

    def get_metrics_snapshot(self) -> dict:
        dump = {}
        if self._lock.acquire():
            for name, metric in self._metric_map.items():
                dump[name] = metric.value
            self._lock.release()
        return dump


class RayMetricTimer(Timer):
    def _current_milli_time(self):
        return int(round(time.time() * 1000))

    def __init__(self, gauge, metrics_tags):
        self._gauge = gauge
        self._metrics_tags = metrics_tags
        self._start_time = self._current_milli_time()

    def start_duration(self):
        self._start_time = self._current_milli_time()

    def start_duration_with_time(self, start_time):
        self._start_time = start_time

    def observe_duration(self):
        duration = self.value
        self._gauge.record(duration, self._metrics_tags)
        return duration

    @property
    def value(self):
        return self._current_milli_time() - self._start_time


class RayMetricsMeter(Meter):
    def _current_milli_time(self):
        return int(round(time.time() * 1000))

    def __init__(self, gauge, metrics_tags):
        self._gauge = gauge
        self._metrics_tags = metrics_tags
        self.__counter = 0
        self.__last_update_timestamp_ms = self._current_milli_time()
        self.__rate = 0.0

    def mark(self, value: int = 1):
        assert isinstance(value, int)
        self.__counter += value

    def get_count(self):
        return self.__counter

    def get_rate(self):
        cur_timestamp_ms = self._current_milli_time()
        elapsed_time_ms = cur_timestamp_ms - self.__last_update_timestamp_ms
        if elapsed_time_ms > 0:
            self.__rate = self.__counter * 1000 / elapsed_time_ms
            return self.__rate
        else:
            return 0

    @property
    def value(self):
        return self.__rate

    def report(self, tags=None):
        if tags is None:
            tags = {}
        logger.debug("update meter: {}-{}".format(self._gauge.info["name"],
                                                  self.get_rate()))
        self._gauge.record(self.get_rate(), {**self._metrics_tags, **tags})
        self.__counter = 0
        self.__last_update_timestamp_ms = self._current_milli_time()

    def update(self, value, tags=None):
        if tags is None:
            tags = {}
        self.mark(value)
        self.report(tags)


class RayMetricsGauge(RayGauge, Gauge):
    def __init__(self,
                 name: str,
                 description: str = "",
                 metrics_tags=None,
                 tag_keys: Optional[Tuple[str]] = None):
        super().__init__(name, description, tag_keys)
        self._metrics_tags = metrics_tags if metrics_tags is not None \
            else {}
        self.__value = None

    @property
    def value(self):
        return self.__value

    def record(self, value: float, tags: dict = None) -> None:
        super().record(value, tags)
        self.__value = value

    def update(self, value, tags=None):
        if tags is None:
            tags = {}
        self.record(value, {**self._metrics_tags, **tags})


class RayMetricsSum(RaySum, Sum):
    def __init__(self,
                 name: str,
                 description: str = "",
                 metrics_tags=None,
                 tag_keys: Optional[Tuple[str]] = None):
        super().__init__(name, description, tag_keys)
        self._metrics_tags = metrics_tags if metrics_tags is not None \
            else {}
        self.__value = None

    @property
    def value(self):
        return self.__value

    def record(self, value: float, tags: dict = None) -> None:
        super().record(value, tags)
        self.__value = value

    def update(self, value=1, tags=None):
        if type(value) is dict:
            tags = value
            value = 1
        if tags is None:
            tags = {}
        self.record(value, {**self._metrics_tags, **tags})
