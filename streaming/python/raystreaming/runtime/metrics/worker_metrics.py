from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import logging
import os
import threading

from raystreaming.constants import StreamingConstants
from raystreaming.generated.remote_call_pb2 import (
    WorkerQueryScope, )
from raystreaming.runtime.graph import NodeType
from raystreaming.runtime.metrics.metric_group_factory \
    import MetricGroupFactory
from raystreaming.runtime.metrics.metric_query_service \
    import MetricQueryService
from raystreaming.utils import EnvUtil

logger = logging.getLogger(__name__)


class WorkerMetrics:
    """
    worker metrics
    """

    # worker metrics keys
    WK_CP_SUCCESS = "streaming_wk_cp_success"
    WK_CP_FAIL = "streaming_wk_cp_fail"
    WK_PULL_MSG = "streaming_wk_pull_msg"
    WK_PULL_NULL_MSG = "streaming_wk_pull_null_msg"
    WK_CONSUME_MSG = "streaming_wk_consume_msg"
    WK_PRODUCE_MSG = "streaming_wk_produce_msg"
    WK_CONSUME_MSG_RATE = "streaming_wk_consume_msg_rate"
    WK_PRODUCE_MSG_RATE = "streaming_wk_produce_msg_rate"
    WK_LATENCY_MSG = "streaming_wk_msg_latency_ms"
    WK_CONSUME_BARRIER_COUNT = "streaming_wk_consume_barrier_count"
    WK_PRODUCE_BARRIER_COUNT = "streaming_wk_produce_barrier_count"
    WK_CP_TIME = "streaming_wk_cp_time"
    WK_HEARTBEAT = "streaming_wk_heartbeat"
    WK_COMMIT_COUNT = "streaming_wk_commit_count"
    WK_REQUEST_ROLLBACK_COUNT = "streaming_wk_request_rollback_count"
    WK_ROLLBACK_COUNT = "streaming_wk_rollback_count"
    WK_BACKPRESSURE_RATIO = "steaming_wk_produce_backpressure_ratio"

    METIRC_UPDATE_INTERVAL_KEY = "metric_update_interval"
    METRIC_UPDATE_DEFAULT_INTERVAL = 5

    def __init__(self, job_name, op_name, worker_name, worker_id, conf):
        if not WorkerMetrics._is_metrics_enabled():
            logger.warning("Metrics is disabled, use local metrics client.")
            conf[StreamingConstants.
                 METRICS_TYPE] = StreamingConstants.METRICS_LOCAL
        self.metric_scope = self.generate_scope(
            EnvUtil.get_hostname(), job_name, str(os.getpid()), op_name,
            worker_id, worker_name)
        self.queue_scope = copy.copy(self.metric_scope)
        labelvalues = {
            StreamingConstants.METRICS_LABEL_JOB_NAME: job_name,
            StreamingConstants.METRICS_LABEL_HOST_IP: EnvUtil.get_hostname(),
            StreamingConstants.METRICS_LABEL_OP_NAME: op_name,
            StreamingConstants.METRICS_LABEL_WORKER_NAME: worker_name,
            StreamingConstants.METRICS_LABEL_PID: str(os.getpid()),
            StreamingConstants.METRICS_LABEL_WORKER_ID: worker_id
        }
        logger.info("Start to init worker metrics, job name {}, host {}, "
                    "op name {}, worker name: {}, pid: {}.".format(
                        job_name, EnvUtil.get_host_address(), op_name,
                        worker_name, os.getpid()))
        self.metric_query_service = MetricQueryService()
        self.metric_group = MetricGroupFactory.produce(self.metric_scope, {
            **conf,
            **labelvalues
        })

        self.queue_metric_group = MetricGroupFactory.produce(
            self.queue_scope, {
                **conf,
                **labelvalues
            })
        self.global_tags = self.metric_group.get_global_tags()
        self.metric_update_interval = \
            WorkerMetrics.METRIC_UPDATE_DEFAULT_INTERVAL
        if WorkerMetrics.METIRC_UPDATE_INTERVAL_KEY in conf:
            self.metric_update_interval = \
                 conf[WorkerMetrics.METIRC_UPDATE_INTERVAL_KEY]

    def open(self, node_type=NodeType.TRANSFORM):
        logger.info("Open worker metric.")
        self.node_type = node_type
        self.cp_success_counter = self._get_counter(
            WorkerMetrics.WK_CP_SUCCESS, self.metric_group)
        self.cp_fail_counter = self._get_counter(WorkerMetrics.WK_CP_FAIL,
                                                 self.metric_group)
        self.cp_timer = self.init_cp_timer()
        self.commit_counter = self._get_counter(WorkerMetrics.WK_COMMIT_COUNT,
                                                self.metric_group)
        self.request_rollback_counter = self._get_counter(
            WorkerMetrics.WK_REQUEST_ROLLBACK_COUNT, self.metric_group)
        self.rollback_counter = self._get_counter(
            WorkerMetrics.WK_ROLLBACK_COUNT, self.metric_group)
        self.produce_barrier_counter = self._get_counter(
            WorkerMetrics.WK_PRODUCE_BARRIER_COUNT, self.metric_group)
        self.consume_barrier_counter = self._get_counter(
            WorkerMetrics.WK_CONSUME_BARRIER_COUNT, self.metric_group)
        self.consume_msg_timer = self._get_timer(WorkerMetrics.WK_CONSUME_MSG,
                                                 self.metric_group)
        self.produce_msg_timer = self._get_timer(WorkerMetrics.WK_PRODUCE_MSG,
                                                 self.metric_group)
        self.latency_msg_timer = self._get_timer(WorkerMetrics.WK_LATENCY_MSG,
                                                 self.metric_group)

        self.consume_msg_meter = None
        self.produce_msg_meter = None

        if node_type == NodeType.SOURCE:
            logger.info("Init source metric.")
            self.produce_msg_meter = self._get_meter(
                WorkerMetrics.WK_PRODUCE_MSG_RATE, self.metric_group)
        elif node_type == NodeType.TRANSFORM:
            logger.info("Init transform metric.")
            self.produce_msg_meter = self._get_meter(
                WorkerMetrics.WK_PRODUCE_MSG_RATE, self.metric_group)
            self.consume_msg_meter = self._get_meter(
                WorkerMetrics.WK_CONSUME_MSG_RATE, self.metric_group)
        elif node_type == NodeType.SINK:
            logger.info("Init sink metric.")
            self.consume_msg_meter = self._get_meter(
                WorkerMetrics.WK_CONSUME_MSG_RATE, self.metric_group)
        else:
            pass

        meters = {
            WorkerMetrics.WK_CONSUME_MSG_RATE: self.consume_msg_meter,
            WorkerMetrics.WK_PRODUCE_MSG_RATE: self.produce_msg_meter
        }
        self.metric_updater = MetricUpdateTimer(60,
                                                self.metric_update_interval)
        self.metric_updater.set_meters(meters)
        self.metric_updater.execute()

    def close(self):
        logger.info("Close worker metric.")
        self.metric_updater.stop()

    @staticmethod
    def _is_metrics_enabled():
        sys_env_metrics_enable = os.getenv(
            StreamingConstants.METRICS_ENABLE_KEY,
            StreamingConstants.METRICS_ENABLE_DEFAULT)
        # keep same with java
        if sys_env_metrics_enable.lower() == "false":
            logger.warning(
                "System env {} is set to False, metrics will not be pushed to "
                "remote server.".format(StreamingConstants.METRICS_ENABLE_KEY))
            return False
        return True

    def get_metrics_query_service(self):
        return self.metric_query_service

    def query_metrics(self):
        return self.metric_query_service.query_metrics()

    def generate_scope(self, hostname, job_name, pid, op_name, worker_id,
                       worker_name):
        workerQueryScope = WorkerQueryScope()
        workerQueryScope.host = hostname
        workerQueryScope.job_name = job_name
        workerQueryScope.op_name = op_name
        workerQueryScope.worker_name = worker_name
        logger.info(f"hostname is {hostname}, job_name is {job_name}, \
                    pid {pid}, op_name {op_name}, workerid is  {worker_id} \
                    {str.encode(worker_id)} worker_name {worker_name}")
        workerQueryScope.worker_id.id = str.encode(worker_id)
        workerQueryScope.pid = pid
        return workerQueryScope

    def _get_counter(self,
                     counter_key,
                     metric_group,
                     tags=None,
                     metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        _counter = metric_group.get_counter(counter_key, tags,
                                            metric_wk_prefix)
        self.metric_query_service.add_metric(_counter, counter_key,
                                             self.metric_group)
        return _counter

    def get_counter(self, counter_key, tags=None):
        return self.metric_group.get_counter(counter_key, tags,
                                             StreamingConstants.METRIC_PREFIX)

    def update_counter(self, counter, tags=None):
        self.metric_group.inc_counter(counter, tags)

    def _get_gauge(self,
                   gauge_key,
                   metric_group,
                   tags=None,
                   metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        _gauge = metric_group.get_gauge(gauge_key, tags, metric_wk_prefix)
        self.metric_query_service.add_metric(_gauge, gauge_key, metric_group)
        return _gauge

    def get_gauge(self, gauge_key, tags=None):
        return self.metric_group.get_gauge(gauge_key, tags,
                                           StreamingConstants.METRIC_PREFIX)

    def update_gauge(self, gauge, value, tags=None):
        self.metric_group.add_gauge(gauge, value, tags)

    def _get_meter(self,
                   meter_key,
                   metric_group,
                   tags=None,
                   metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        _meter = metric_group.get_meter(meter_key, tags, metric_wk_prefix)
        self.metric_query_service.add_metric(_meter, meter_key,
                                             self.metric_group)
        return _meter

    def get_meter(self, meter_key, tags=None):
        return self.metric_group.get_meter(meter_key, tags,
                                           StreamingConstants.METRIC_PREFIX)

    def update_meter(self, meter, value, tags=None):
        self.metric_group.add_meter(meter, value, tags)

    def _get_timer(self,
                   timer_key,
                   metric_group,
                   tags=None,
                   metric_wk_prefix=StreamingConstants.METRIC_WK_PREFIX):
        _timer = metric_group.get_timer(timer_key, tags, metric_wk_prefix)
        self.metric_query_service.add_metric(_timer, timer_key,
                                             self.metric_group)
        return _timer

    def _inc_counter(self, counter, counter_key):
        self.metric_group.inc_counter(counter)

    def register_metric_func(self, func):
        self.metric_updater.register_func(func)

    # -------------  worker metric function  ------------- #

    def set_output_queue_ids(self, output_queue_ids):
        self.output_queue_ids = output_queue_ids

    def init_backpressure_ratio_metric(self):

        if self.node_type == NodeType.SINK or \
                self.node_type == NodeType.SOURCE_AND_SINK:
            logger.info("Sink node haven't backpressure.")
            return

        self.backpressure_ratio_gauge_map = {}
        for queue_id in self.output_queue_ids:
            self.queue_metric_group.get_scope().queue_id = queue_id
            self.backpressure_ratio_gauge_map[queue_id] = self._get_gauge(
                WorkerMetrics.WK_BACKPRESSURE_RATIO, self.queue_metric_group,
                {StreamingConstants.METRICS_LABEL_QUEUE_ID: queue_id},
                StreamingConstants.METRIC_WK_BP_RATIO_PREFIX)
        logger.info("Init backpressure ratio metric gauge.")

    def register_bp_ratio_func(self, func):
        if self.node_type == NodeType.SINK or \
                self.node_type == NodeType.SOURCE_AND_SINK:
            logger.info("There is no backpressure ratio at the sink node.")
            return
        self.register_metric_func(func)

    def report_bp_ratio_metric(self, queue_id_list, bp_ratio_map):
        if self.backpressure_ratio_gauge_map is None:
            raise Exception("The backpressure ratio's gauge map is undefined.")

        for key, value in bp_ratio_map.items():
            key = bytes.hex(key)
            self.queue_metric_group.add_gauge(
                self.backpressure_ratio_gauge_map[key], value,
                {StreamingConstants.METRICS_LABEL_QUEUE_ID: key})

    def inc_cp_success(self):
        self._inc_counter(self.cp_success_counter, WorkerMetrics.WK_CP_SUCCESS)

    def inc_cp_fail(self):
        self._inc_counter(self.cp_fail_counter, WorkerMetrics.WK_CP_FAIL)

    def init_cp_timer(self):
        return self._get_timer(WorkerMetrics.WK_CP_TIME, self.metric_group)

    def inc_commit_count(self):
        self._inc_counter(self.commit_counter, WorkerMetrics.WK_COMMIT_COUNT)

    def inc_request_rollback_count(self):
        self._inc_counter(self.request_rollback_counter,
                          WorkerMetrics.WK_REQUEST_ROLLBACK_COUNT)

    def inc_rollback_count(self):
        self._inc_counter(self.rollback_counter,
                          WorkerMetrics.WK_ROLLBACK_COUNT)

    def inc_produce_barrier_count(self):
        self._inc_counter(self.produce_barrier_counter,
                          WorkerMetrics.WK_PRODUCE_BARRIER_COUNT)

    def inc_consume_barrier_count(self):
        self._inc_counter(self.consume_barrier_counter,
                          WorkerMetrics.WK_CONSUME_BARRIER_COUNT)

    def start_latency_timer(self, start_time):
        self.latency_msg_timer.start_duration_with_time(start_time)

    def observe_latency_timer(self):
        self.latency_msg_timer.observe_duration()

    def start_consume_msg_timer(self):
        self.consume_msg_timer.start_duration()

    def observe_consume_msg_timer(self):
        self.consume_msg_timer.observe_duration()

    def start_produce_msg_timer(self):
        self.produce_msg_timer.start_duration()

    def observe_produce_msg_timer(self):
        self.produce_msg_timer.observe_duration()

    def mark_consume_msg_meter(self):
        self.consume_msg_meter.mark()

    def mark_produce_msg_meter(self):
        self.produce_msg_meter.mark()

    def get_metrics_snapshot(self):
        return self.metric_group.get_metrics_snapshot()


class MetricUpdateTimer:
    def __init__(self, update_interval, timer_interval=5):
        self.update_interval = update_interval
        self.timer_interval = timer_interval
        self.func_list = []
        self.is_close = False
        self.func_list_mutex = threading.Lock()

    def set_meters(self, meters):
        self.meters = meters
        self.register_func(self.add_and_update_meter)

    def execute(self):
        logger.debug("Start update timer.")
        task = threading.Timer(
            self.timer_interval, self.update, args=(self.func_list_mutex, ))
        task.start()

    def stop(self):
        logger.info("Stop meter updater.")
        self.is_close = True

    # Register metric function to metric update timer.
    def register_func(self, func):
        with self.func_list_mutex:
            self.func_list.append(func)

    def update(self, func_list_mutex):
        with func_list_mutex:
            execute_func = copy.copy(self.func_list)
        for func in execute_func:
            func()

    def add_and_update_meter(self):
        for metric_label, meter in self.meters.items():
            try:
                if meter:
                    meter.report()
            except Exception as e:
                logger.error("Failed to update meter: {}. Error: {}.".format(
                    metric_label, e))
        if not self.is_close:
            self.execute()
