import ray

from raystreaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition
from ray.streaming.function import SinkFunction
from ray.streaming.function import SourceFunction
from ray.streaming.runtime.metrics.worker_metrics import WorkerMetrics
import logging
import time
import os

logger = logging.getLogger(__name__)


@ray.remote
class Worker:
    def __init__(self, conf):
        self.worker_metrics = WorkerMetrics("Job", "Op", "Name", "Id", conf)
        logger.info("Worker Init.")

    def open(self):
        logger.info("Worker metrics open.")
        self.worker_metrics.open()

    def close(self):
        logger.info("Worker metrics closed.")
        self.worker_metrics.close()

    def report_cp_time(self):
        self.worker_metrics.cp_timer.start_duration()
        time.sleep(0.5)
        self.worker_metrics.cp_timer.observe_duration()

    def report_produce_tps(self):
        for i in range(5):
            self.worker_metrics.mark_produce_msg_meter()
        self.worker_metrics.produce_msg_meter.report()

    def get_metric_dump(self):
        return self.worker_metrics.get_metrics_snapshot()


def test_worker_metrics():
    test_utils.start_ray_python_only()
    conf = {WorkerMetrics.METIRC_UPDATE_INTERVAL_KEY: 1}
    logger.info("Test worker metrics.")
    worker = Worker.remote(conf)
    ray.get(worker.open.remote())
    ray.get(worker.report_cp_time.remote())
    ray.get(worker.report_produce_tps.remote())
    metric_dump = ray.get(worker.get_metric_dump.remote())
    ray.get(worker.close.remote())
    print(metric_dump)
    assert metric_dump[WorkerMetrics.WK_CONSUME_MSG_RATE] < 1e-5
    assert metric_dump[WorkerMetrics.WK_PRODUCE_MSG_RATE] > 1
    assert metric_dump[WorkerMetrics.WK_CP_TIME] >= 500
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_master_fetch_metric():
    test_utils.start_ray()

    class MySourceFunction(SourceFunction):
        def init(self, parallel, index):
            self.times = 5

        def fetch(self, ctx, checkpoint_id: int):
            while self.times > 0:
                self.times -= 1
                ctx.collect("a")

    class MySinkFunction(SinkFunction):
        def __init__(self, sink_file):
            self.sink_file = sink_file

        def open(self, runtime_context):
            pass

        def sink(self, value):
            try:
                actor_name = "1-PythonOperator-0|0"
                self.actor = ray.get_actor(actor_name)
            except ValueError:
                print("============== not found")
            time.sleep(30)
            metrics = self.actor.fetch_metrics.remote()
            with open(self.sink_file, "w") as f:
                f.write(str(ray.get(metrics)))
                f.flush()
            logger.info(f"metrics is {ray.get(metrics)}")

    sink_file = "/tmp/test_master_fetch_metric.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()

    ctx.source(MySourceFunction()) \
        .sink(MySinkFunction(sink_file)).set_parallelism(2)

    ctx.submit("test_master_fetch_metric")

    def check_succeed():
        if os.path.exists(sink_file):
            with open(sink_file) as f:
                result = f.read()
                print(f"result {result}")
                if "steaming.wk.produce.backpressure.ratio" in result:
                    return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")


@test_utils.skip_if_no_streaming_jar()
def test_user_define_metric():
    test_utils.start_ray()

    total_counter_name = "total_counter_name"
    total_gauge_name = "total_gauge_name"
    total_meter_name = "total_meter_name"

    class MySourceFunction(SourceFunction):
        def init(self, parallel, index):
            self.times = 0

        def open(self, runtime_context):
            self.metric = runtime_context.get_metric()
            self.total_counter = \
                self.metric.get_counter(total_counter_name, {"image_type": ""})
            self.total_gauge = \
                self.metric.get_gauge(total_gauge_name, {"image_type": ""})
            self.total_meter = \
                self.metric.get_meter(total_meter_name, {"image_type": ""})

        def fetch(self, ctx, checkpoint_id: int):
            while self.times < 5:
                self.times += 1
                ctx.collect("a")
                self.total_counter.update({"image_type": "1"})
                self.total_gauge.update(self.times, {"image_type": "1"})
                self.total_meter.update(self.times, {"image_type": "1"})

    class MySinkFunction(SinkFunction):
        def __init__(self, sink_file):
            self.sink_file = sink_file

        def open(self, runtime_context):
            pass

        def sink(self, value):
            try:
                actor_name = "1-PythonOperator-0|0"
                self.actor = ray.get_actor(actor_name)
            except ValueError:
                print("============== not found")
            time.sleep(30)
            metrics = self.actor.fetch_metrics.remote()
            with open(self.sink_file, "w") as f:
                f.write(str(ray.get(metrics)))
                f.flush()
            logger.info(f"metrics is {ray.get(metrics)}")

    sink_file = "/tmp/test_user_define_metric.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    ctx = StreamingContext.Builder() \
        .build()

    ctx.source(MySourceFunction()) \
        .sink(MySinkFunction(sink_file)).set_parallelism(2)

    ctx.submit("test_user_define_metric")

    def check_succeed():
        if os.path.exists(sink_file):
            with open(sink_file) as f:
                result = f.read()
                print(f"result {result}")
                if "steaming.wk.produce.backpressure.ratio" in result:
                    return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
