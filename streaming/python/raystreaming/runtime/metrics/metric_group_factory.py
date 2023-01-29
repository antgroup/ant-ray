from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from raystreaming.config import ConfigHelper
from raystreaming.constants import StreamingConstants
from raystreaming.runtime.metrics.raypy.ray_metric_group \
    import RayMetricGroup
from raystreaming.runtime.metrics.local.local_metric_group \
    import LocalMetricGroup


class MetricGroupFactory:
    @staticmethod
    def produce(metric_scope, conf):
        metric_group = None
        client_type = ConfigHelper.get_metrics_type(conf)
        if client_type in [
                StreamingConstants.METRICS_KMONITOR,
                StreamingConstants.METRICS_RAY_METRIC
        ]:
            metric_group = RayMetricGroup(metric_scope, conf)
        elif client_type == StreamingConstants.METRICS_LOCAL:
            metric_group = LocalMetricGroup(metric_scope, conf)
        else:
            raise ValueError("Unsupported metrics client type:" + client_type)
        return metric_group
