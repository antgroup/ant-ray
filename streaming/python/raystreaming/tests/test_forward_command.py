import os

from raystreaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition
from ray.streaming.function import SinkFunction
from ray.streaming.function import SourceFunction
from ray.streaming.runtime.control_message import (
    ControlMessageType,
    ControlMessage,
)
import ray


class MockSourceFunction(SourceFunction):
    def init(self, parallel, index):
        self.times = 10

    def fetch(self, ctx, checkpoint_id: int):
        while self.times > 0:
            self.times -= 1
            ctx.collect("a")


class MockSinkFunction(SinkFunction):
    def __init__(self, value):
        self.message_map = value

    def open(self, runtime_context):
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

    def forward_command(self, message):
        print(f"The call is successful, message is {message.message}.")
        file_path = message.message
        print(f"file_path : {file_path}")
        with open(file_path, "w") as f:
            f.write(message.message)
            f.flush()
        return message.message

    def sink(self, value):
        message_value = self.message_map["file" + str(self.index)]
        message = ControlMessage(ControlMessageType.OPERATOR_COMMAND,
                                 message_value)
        print("start forward_command")
        try:
            a = self.actor1.insert_control_message.remote(message)
            print(ray.get(a))
        except Exception as e:
            print(e)
        print("end forward_command")


@test_utils.skip_if_no_streaming_jar()
def test_forward_command():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.partition.enable_dr", "false")\
        .build()
    message_value = {
        "file1": "/tmp/ray_streaming_test_forward_command_1.txt",
        "file2": "/tmp/ray_streaming_test_forward_command_2.txt"
    }

    if os.path.exists(message_value["file1"]):
        os.remove(message_value["file1"])
    if os.path.exists(message_value["file2"]):
        os.remove(message_value["file2"])

    a = MockSourceFunction()
    b = MockSinkFunction(message_value)

    ctx.source(a) \
        .sink(b) \
        .set_parallelism(2)

    ctx.submit("test_forward_command")

    def check_succeed():
        res = True
        file_num = 0
        if os.path.exists(message_value["file1"]):
            file_num += 1
            with open(message_value["file1"]) as f:
                result = f.read()
                print(f"result {result}")
                res &= result == message_value["file1"]
        if os.path.exists(message_value["file2"]):
            file_num += 1
            with open(message_value["file2"]) as f:
                result = f.read()
                print(f"result {result}")
                res &= result == message_value["file2"]

        return res and file_num == 2

    wait_for_condition(check_succeed, timeout=20, retry_interval_ms=1000)
    print("Execution succeed")
