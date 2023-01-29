import logging
import copy

from raystreaming.generated.remote_call_pb2 import (
    MetricDump,
    MetricResult,
)
from raystreaming.runtime.metrics.raypy.ray_metric_group import (
    RayMetricsGauge,
    RayMetricsMeter,
    RayMetricsSum,
    RayMetricTimer,
)
from raystreaming.constants import StreamingConstants as constants

logger = logging.getLogger(__name__)


class MetricQueryService:
    def __init__(self):
        self.metrics = {}

    def add_metric(self, metric, metric_name, metric_group):
        scope = copy.copy(metric_group.get_scope())
        self.metrics[metric] = (scope, metric_name)

    def remove_metric(self, metric):
        self.metrics.pop(metric, f"{metric} isn't existed in metrics list")

    def create_counter_dump(self, scope, metric_name, count):
        counter_dump = MetricDump()
        counter_dump.query_scope.CopyFrom(scope)
        counter_dump.name = metric_name
        counter_dump.value = count
        counter_dump.category = constants.METRICS_CATEGORY_COUNTER
        return counter_dump

    def create_gauge_dump(self, scope, metric_name, value):
        gauge_dump = MetricDump()
        gauge_dump.query_scope.CopyFrom(scope)
        gauge_dump.name = metric_name
        gauge_dump.value = value
        gauge_dump.category = constants.METRICS_CATEGORY_GAUGE
        return gauge_dump

    def create_meter_dump(self, scope, metric_name, rate):
        meter_dump = MetricDump()
        meter_dump.query_scope.CopyFrom(scope)
        meter_dump.name = metric_name
        meter_dump.value = rate
        meter_dump.category = constants.METRICS_CATEGORY_METER
        return meter_dump

    def create_timer_dump(self, scope, metric_name, duration_time):
        timer_dump = MetricDump()
        timer_dump.query_scope.CopyFrom(scope)
        timer_dump.name = metric_name
        timer_dump.value = duration_time
        timer_dump.category = constants.METRICS_CATEGORY_GAUGE
        return timer_dump

    def query_metrics(self):
        metrics_result = MetricResult()
        switch = {
            RayMetricsSum: self.create_counter_dump,
            RayMetricsGauge: self.create_gauge_dump,
            RayMetricsMeter: self.create_meter_dump,
            RayMetricTimer: self.create_timer_dump,
        }
        for metric, (scope, metric_name) in self.metrics.items():
            choice = type(metric)
            if metric.value is None:
                logger.debug(f"metric was not send. {metric_name}")
                continue
            else:
                logger.debug(f"metric was send. {metric_name}")
            if choice == RayMetricTimer:
                continue
            metrics_result.dumps.append(
                switch.get(choice)(scope, metric_name.replace("_", "."),
                                   str(metric.value)))
        logger.debug(f"metrics result is {metrics_result}")
        return metrics_result
