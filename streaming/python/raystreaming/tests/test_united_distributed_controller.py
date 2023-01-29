import os

import logging
import threading
from ray.actor import ActorHandle
from raystreaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition
from ray.streaming.function import SinkFunction
from ray.streaming.function import SourceFunction

import ray

logger = logging.getLogger(__name__)


class IntegrationTestSinkFunction(SinkFunction):
    def __init__(self):
        self.file_path = "/tmp/testUDCIntegrationTestSinkFunction.txt"

    def write_value(self, value):
        with open(self.file_path, "a") as f:
            f.write(value)
            f.flush()

    def on_prepare(self, msg):
        logger.info("call on_prepare.")
        self.write_value("prepare->")
        return True

    def on_commit(self):
        logger.info("call on_commit.")
        self.write_value("commit->")
        super().commit_callback(True)

    def on_disposed(self) -> bool:
        logger.info("call on_disposed.")
        self.write_value("dispose")
        return True

    def on_cancel(self):
        logger.info("call on_cancel.")
        self.write_value("cancel")
        return True

    def sink(self, value):
        print(f"IntegrationTestSinkFunction sink a value: {value}")


class MockChiefWorkerFunction(SinkFunction):
    def __init__(self):
        self.file_path = "/tmp/testPsRescalingUpSink.txt"

    def on_prepare(self, msg):
        logger.info("call on_prepare.")
        import pyfury
        fury_ = pyfury.Fury(
            language=pyfury.Language.X_LANG, reference_tracking=True)

        self.data_payload = fury_.deserialize(msg.data_payload)
        assert self.data_payload["type"] == "RESCALE_UP"
        assert isinstance(self.data_payload, dict)
        self.data_payload["new_born_actors"] = \
            fury_.deserialize(self.data_payload["new_born_actors"])
        assert isinstance(self.data_payload["new_born_actors"], list)
        return True

    def on_commit(self):
        logger.info("call on_commit.")
        for x in self.data_payload["new_born_actors"]:
            actor = ActorHandle._deserialization_helper(x)
            actor.write_value.remote("a")
        logger.info("chief worker commit.")
        super().commit_callback(True)

    def on_disposed(self) -> bool:
        logger.info("call on_disposed.")
        return True

    def on_cancel(self):
        logger.info("call on_cancel.")
        return True

    def sink(self, value):
        logger.info("hello world.")


class MockSourceFunction(SourceFunction):
    def init(self, parallel, index):
        self.times = 1

    def fetch(self, ctx, checkpoint_id: int):
        while self.times > 0:
            self.times -= 1
            ctx.collect("a")


class MockSinkFunction(SinkFunction):
    def __init__(self, file_path):
        self.file_path = file_path

    def open(self, runtime_context):
        self._file_lock = threading.Lock()
        self.index = runtime_context.task_id
        print(f"sink {self.index}")
        try:
            # Task 1 calls actor '1|2', task 2 calls actor '0|1'.
            actor_name = "2-PythonOperator-" +\
                str(2 - self.index) + "|" + str(3 - self.index)
            self.actor1 = ray.get_actor(actor_name)
        except ValueError:
            print(f"{self.index}============== not found")
        print("open end")

    def write_value(self, value):
        self._file_lock.acquire()
        with open(self.file_path, "a") as f:
            f.write(value)
            f.flush()
        self._file_lock.release()

    def on_prepare(self, msg):
        logger.info("call on_prepare.")
        self.write_value("1")
        return True

    def on_commit(self) -> bool:
        logger.info("call on_commit.")
        self.write_value("2")
        return True

    def on_disposed(self) -> bool:
        logger.info("call on_disposed.")
        self.write_value("3")
        return True

    def on_cancel(self) -> bool:
        logger.info("call on_cancel.")
        self.write_value("4")
        return True

    def sink(self, value):
        print("start call")
        try:
            msg = "".encode()
            a = self.actor1.udc_prepare.remote(msg)
            self.actor1.udc_commit.remote()
            self.actor1.udc_disposed.remote()
            self.actor1.udc_cancel.remote()
            print(ray.get(a))
        except Exception as e:
            print(e)
        print("end call")


@test_utils.skip_if_no_streaming_jar()
def test_base_tcc_call():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.partition.enable_dr", "false")\
        .build()
    file_name = "/tmp/ray_streaming_test_base_call.txt"

    if os.path.exists(file_name):
        os.remove(file_name)

    a = MockSourceFunction()
    b = MockSinkFunction(file_name)

    ctx.source(a) \
        .sink(b) \
        .set_parallelism(2)

    ctx.submit("test_base_tcc_call")

    def check_succeed():
        res = False
        if os.path.exists(file_name):
            with open(file_name) as f:
                result = f.read()
                print(f"result {result}")
                res = result == "1234"
        return res

    wait_for_condition(check_succeed, timeout=30, retry_interval_ms=1000)
    print("Execution succeed")
