import os

import ray
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
import json


@test_utils.skip_if_no_streaming_jar()
def test_merge_stream():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    sink_file = "/tmp/test_merge_stream.txt"
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
    stream5 = stream1.map(lambda x: x)

    from ray.streaming.function import MergeFunction
    from ray.streaming.function import KeyFunction

    class UserDefinedMergeOperator(MergeFunction):
        def merge(self, values):
            assert len(values) == 4
            assert json.loads(values[0])["res"] == "1"
            assert json.loads(values[1])["res"] == "2"
            assert json.loads(values[2])["res"] == "3"
            assert json.loads(values[3])["res"] == "4"
            return str(values)

    class UserDefinedKeyOperator(KeyFunction):
        def key_by(self, value):
            return json.loads(value)["id"]

    stream5.merge(stream2, stream3, stream4). \
        extract_key(UserDefinedKeyOperator()). \
        apply(UserDefinedMergeOperator()).disable_chain().sink(sink_func)
    ctx.submit("test_merge_stream")

    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                print("sink result", result)
                # Note: We have checked result in `UserDefinedMergeOperator`.
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)

    ray.shutdown()


if __name__ == "__main__":
    test_merge_stream()
