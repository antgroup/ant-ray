import os

from raystreaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition
from ray.streaming.function import SinkFunction
from ray.streaming.function import SourceFunction


class MockSourceFunction(SourceFunction):
    def init(self, parallel, index):
        self.times = 10
        self.index = index

    def fetch(self, ctx, checkpoint_id: int):
        while self.times > 0:
            self.times -= 1
            ctx.collect(self.index)


class MockSinkFunction(SinkFunction):
    def open(self, runtime_context):
        self.index = runtime_context.task_id
        # print(f"sink {self.index}")

    def sink(self, value):
        file1 = file_path + str(self.index) + ".txt"
        with open(file1, "w") as f:
            f.write(str(value))
            f.flush()


file_path = "/tmp/ray_streaming_test_dynamic_division_"


@test_utils.skip_if_no_streaming_jar()
def test_default_divide():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.partition.enable_dr", "false")\
        .option("streaming.scheduler.dynamic.division.enable", "true") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    for i in range(2, 7):
        remove_file_path = file_path + str(i) + ".txt"
        if os.path.exists(remove_file_path):
            os.remove(remove_file_path)
    a = MockSourceFunction()
    b = MockSinkFunction()

    ctx.source(a) \
        .set_parallelism(2) \
        .sink(b) \
        .set_parallelism(4)

    ctx.submit("test_dynamic_division")

    def check_succeed():
        res = True
        file_num = 0
        for i in range(2, 4):
            if os.path.exists(file_path + str(i) + ".txt"):
                file_num += 1
                with open(file_path + str(i) + ".txt") as f:
                    result = f.read()
                    print(f"result {result}")
                    res &= result == str(0)
        for i in range(4, 6):
            if os.path.exists(file_path + str(i) + ".txt"):
                file_num += 1
                with open(file_path + str(i) + ".txt") as f:
                    result = f.read()
                    print(f"result {result}")
                    res &= result == str(1)
        return res and file_num == 4

    wait_for_condition(check_succeed, timeout=20, retry_interval_ms=1000)
    print("Execution succeed")


@test_utils.skip_if_no_streaming_jar()
def test_set_group_num():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.partition.enable_dr", "false")\
        .option("streaming.scheduler.dynamic.division.enable", "true") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    a = MockSourceFunction()
    b = MockSinkFunction()
    for i in range(2, 6):
        remove_file_path = file_path + str(i) + ".txt"
        if os.path.exists(remove_file_path):
            os.remove(remove_file_path)
    ctx.source(a) \
        .set_parallelism(2) \
        .set_dynamic_division_num(2) \
        .sink(b) \
        .set_parallelism(3) \
        .set_dynamic_division_num(2)

    ctx.submit("test_set_group_num")

    def check_succeed():
        res = True
        file_num = 0
        for i in range(2, 3):
            if os.path.exists(file_path + str(i) + ".txt"):
                file_num += 1
                with open(file_path + str(i) + ".txt") as f:
                    result = f.read()
                    print(f"result {result}")
                    res &= result == str(0)
        for i in range(3, 5):
            if os.path.exists(file_path + str(i) + ".txt"):
                file_num += 1
                with open(file_path + str(i) + ".txt") as f:
                    result = f.read()
                    print(f"result {result}")
                    res &= result == str(1)
        print(f"file_num {file_num}")
        return res and file_num == 3

    wait_for_condition(check_succeed, timeout=20, retry_interval_ms=1000)
    print("Execution succeed")
