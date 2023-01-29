import os

import ray
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition


@test_utils.skip_if_no_streaming_jar()
def test_word_count():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.partition.enable_dr", "false") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    ctx.read_text_file(__file__) \
        .set_parallelism(1) \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .filter(lambda x: "ray" not in x) \
        .sink(lambda x: print("result", x))
    ctx.submit("word_count")
    import time
    time.sleep(10)
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_simple_word_count():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.partition.enable_dr", "false") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    sink_file = "/tmp/ray_streaming_test_simple_word_count.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            line = "{}:{},".format(x[0], x[1])
            print("sink_func", line)
            f.write(line)

    ctx.from_values("a", "b", "c") \
        .set_parallelism(1) \
        .flat_map(lambda x: [x, x]) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .sink(sink_func)
    ctx.submit("word_count")
    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                assert "a:2" in result
                assert "b:2" in result
                assert "c:2" in result
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)

    time.sleep(5)
    is_job_finished = ctx.is_job_finished()
    print("is_job_finished: {}".format(is_job_finished))
    assert is_job_finished
    assert ctx.get_job_status().name == "FINISHED"
    assert ctx.get_failover_info() == []
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_write_values(shutdown_only):
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.reliability.level", "EXACTLY_ONCE") \
        .option("streaming.partition.enable_dr", "false") \
        .option("streaming.external.interaction.enable", "false") \
        .build()

    sink_file = "/tmp/ray_streaming_test_write_values.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            f.write(str(x))
            f.flush()

    ctx.from_values(1, 2, 3) \
        .map(lambda x: x * 2) \
        .set_parallelism(3) \
        .sink(sink_func) \
        .set_parallelism(1)
    ctx.submit("test_write_values")

    def check_succeed():
        if ctx._gateway_client.is_job_finished():
            with open(sink_file) as f:
                result = f.read()
                print(f"result {result}")
                return set(result) == {"2", "4", "6"}
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")


@test_utils.skip_if_no_streaming_jar()
def test_dynamic_rebalance(shutdown_only):
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.reliability.level", "EXACTLY_ONCE") \
        .option("streaming.partition.enable_dr", "true") \
        .option("streaming.external.interaction.enable", "false") \
        .build()

    sink_file = "/tmp/ray_streaming_test_dynamic_rebalance.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            f.write(str(x))
            f.flush()

    ctx.from_values(1, 2, 3) \
        .map(lambda x: x * 2) \
        .set_parallelism(3) \
        .sink(sink_func) \
        .set_parallelism(1)
    ctx.submit("test_dynamic_rebalance")

    def check_succeed():
        if ctx._gateway_client.is_job_finished():
            with open(sink_file) as f:
                result = f.read()
                print(f"result {result}")
                return set(result) == {"2", "4", "6"}
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")


@test_utils.skip_if_no_streaming_jar()
def test_split_stream(shutdown_only):
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.reliability.level", "EXACTLY_ONCE") \
        .option("streaming.external.interaction.enable", "false") \
        .build()

    sink_file_1 = "/tmp/test_split_stream_1.txt"
    sink_file_2 = "/tmp/test_split_stream_2.txt"
    sink_file_3 = "/tmp/test_split_stream_3.txt"
    sink_files = [sink_file_1, sink_file_2, sink_file_3]

    for f in sink_files:
        if os.path.exists(f):
            os.remove(f)

    def sink_func_1(x):
        with open(sink_file_1, "a") as f:
            f.write(x)

    def sink_func_2(x):
        with open(sink_file_2, "a") as f:
            f.write(x)

    def sink_func_3(x):
        with open(sink_file_3, "a") as f:
            f.write(x)

    from ray.streaming.function import FlatMapFunction

    class SplitOperator(FlatMapFunction):
        def flat_map(self, value, collector):
            if value == "a":
                collector.collect_by_index(0, "a")
                collector.collect_by_index(1, "a")
            elif value == "b":
                collector.collect_by_index(1, "b")
            else:
                collector.collect_by_index(2, "c")

    source_stream = ctx.from_values("a", "b", "c").map(lambda x: x)
    stream_1, stream_2, stream_3 = source_stream.split(3, SplitOperator())
    stream_1.disable_chain().sink(sink_func_1)
    stream_2.disable_chain().sink(sink_func_2)
    stream_3.disable_chain().sink(sink_func_3)
    ctx.submit("test_split_stream")

    def check_succeed():
        gateway_client = ctx._gateway_client
        execution_vertex_size = gateway_client.get_execution_job_vertex_size()
        print(execution_vertex_size)
        if ctx._gateway_client.is_job_finished():
            with open(sink_file_1) as f:
                result = f.read()
                assert result.count("a") == 1
            with open(sink_file_2) as f:
                result = f.read()
                assert result.count("a") == 1 and result.count("b") == 1
            with open(sink_file_3) as f:
                result = f.read()
                assert result.count("c") == 1
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    ray.shutdown()


if __name__ == "__main__":
    test_word_count()
    test_simple_word_count()
