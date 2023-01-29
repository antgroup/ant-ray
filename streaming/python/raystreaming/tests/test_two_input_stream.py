import os
import ray
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
import json


@test_utils.skip_if_no_streaming_jar()
def test_union_stream():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    sink_file = "/tmp/test_union_stream.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            print("sink_func", x)
            f.write(str(x))

    stream1 = ctx.from_values(1, 2)
    stream2 = ctx.from_values(3, 4)
    stream3 = ctx.from_values(5, 6)
    stream1.union(stream2, stream3).with_name("TestName").sink(sink_func)
    ctx.submit("test_union_stream")
    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                print("sink result", result)
                assert set(result) == {"1", "2", "3", "4", "5", "6"}
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_join_stream():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    sink_file = "/tmp/test_join_stream.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            print("sink_func", x)
            f.write(str(x))

    stream1 = ctx.from_values("a", "b")
    stream2 = ctx.from_values("a", "b")
    from ray.streaming.function import JoinFunction

    class UserDefinedJoinOperator(JoinFunction):
        def join(self, left_value, right_value):
            return str(left_value) + str(right_value)

    stream1.join(stream2).where(lambda x: x).equal(lambda x: x).apply(
        UserDefinedJoinOperator()).disable_chain().sink(sink_func)
    ctx.submit("test_join_stream")
    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                print("sink result", result)
                assert result == "aabb"
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_join_order():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    sink_file = "/tmp/test_join_order.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            print("sink_func", x)
            f.write(str(x))

    def sink_func_print(x):
        print("sink_func_print", x)

    from ray.streaming.function import FlatMapFunction

    class SplitOperator(FlatMapFunction):
        def flat_map(self, value, collector):
            mm = json.loads(value)
            if "a" in mm.keys():
                collector.collect_by_index(0, value)
            else:
                collector.collect_by_index(1, value)

    data1 = """ {"id":1, "a" : "a", "res" : "1"} """
    data2 = """ {"id":1, "b" : "b", "res" : "2"} """
    source_stream = ctx.from_values(data1, data2).map(lambda x: x)
    stream_1, stream_2 = source_stream.split(2, SplitOperator())
    stream_1.map(lambda x: x).sink(sink_func_print)
    base_frame = stream_1.join_streams_by_primary_key("id", ["a", "b", "res"],
                                                      [stream_2])
    base_frame.sink(sink_func)
    ctx.submit("test_join_order")
    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                print("sink result", result)
                mm = json.loads(result)
                assert mm["0"]["res"] == "1"
                assert mm["1"]["res"] == "2"
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_join_stream_advantage_api():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.external.interaction.enable", "false") \
        .build()
    sink_file = "/tmp/test_join_stream_advantage.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            print("sink_func", x)
            f.write(str(x))

    stream1 = ctx.from_values(""" {"id":1, "a" : "a", "res" : "1"} """)
    stream2 = ctx.from_values(
        """ {"id":1, "b" : "b", "res" : "2", "unique" : "ex2"} """)
    stream3 = ctx.from_values(
        """ {"id":1, "c" : "c", "res" : "3", "unique" : "ex3"} """)
    stream4 = ctx.from_values(""" {"id":1, "d" : "d", "res" : "4"} """)
    stream1.join_streams_by_primary_key(
        "id", ["a", "b", "c", "d", "res", "unique"],
        [stream2, stream3, stream4]).sink(sink_func)
    ctx.submit("test_join_stream_advantage")
    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                print("sink result", result)
                result_map = json.loads(result)
                assert result_map["0"]["a"] == "a"
                assert result_map["3"]["d"] == "d"
                assert result_map["1"]["unique"] == "ex2"
                assert result_map["2"]["unique"] == "ex3"
                assert result_map["3"]["res"] == "4"
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)
    ray.shutdown()


if __name__ == "__main__":
    test_union_stream()
