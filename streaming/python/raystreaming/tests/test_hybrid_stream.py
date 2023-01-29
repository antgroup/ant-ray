import os

import ray
from ray.streaming import StreamingContext
from ray.streaming.function import ProcessFunction
from raystreaming import message
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition


def map_func1(x):
    print("HybridStreamTest map_func1", x)
    return str(x)


def map_func2(x):
    print("HybridStreamTest map_func2", x)
    return str({x: 1})


def filter_func1(x):
    print("HybridStreamTest filter_func1", x)
    return "b" not in x


def sink_func1(x):
    print("HybridStreamTest sink_func1 value:", x)


class ProcessTestFunction(ProcessFunction):
    def open(self, runtime_context):
        pass

    def process(self, row, collector):
        out_row = message.ObjectRow(row.get_arity())
        for i in range(row.get_arity()):
            out_row.set_field(i, row.get_field(i))
        collector.collect(out_row)

    def close(self):
        # Clean up the thing which should be removed.
        pass

    def save_checkpoint(self, checkpoint_id):
        pass

    def load_checkpoint(self, checkpoint_id):
        pass


@test_utils.skip_if_no_streaming_jar()
def test_hybrid_stream_java_source():
    test_utils.start_ray()
    sink_file = "/tmp/ray_streaming_test_hybrid_stream_java_source.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        print("HybridStreamJavaSourceTest", x)
        with open(sink_file, "a") as f:
            f.write(str(x))
            f.flush()

    ctx = StreamingContext.Builder().build()
    java_stream_source = ctx.java_source(
        "com.alipay.streaming.runtime.integrationtest."
        "HybridStreamTest$MockJavaSource")

    java_stream_source.as_python_stream()\
        .map(map_func1).sink(sink_func)

    ctx.submit("HybridStreamJavaSourceTest")

    def check_succeed():
        if os.path.exists(sink_file):
            import time
            time.sleep(3)  # Wait all data be written
            with open(sink_file, "r") as f:
                result = f.read()
                assert "abc" in result
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_hybrid_stream():
    test_utils.start_ray()
    sink_file = "/tmp/ray_streaming_test_hybrid_stream.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        print("HybridStreamTest", x)
        with open(sink_file, "a") as f:
            f.write(str(x))
            f.flush()

    ctx = StreamingContext.Builder().build()
    ctx.from_values("a", "b", "c") \
        .as_java_stream() \
        .map("com.alipay.streaming.runtime.integrationtest."
             "HybridStreamTest$Mapper1") \
        .filter("com.alipay.streaming.runtime.integrationtest."
                "HybridStreamTest$Filter1") \
        .as_python_stream() \
        .sink(sink_func)
    ctx.submit("HybridStreamTest")

    def check_succeed():
        if os.path.exists(sink_file):
            import time
            time.sleep(3)  # Wait all data be written
            with open(sink_file, "r") as f:
                result = f.read()
                assert "a" in result
                assert "b" not in result
                assert "c" in result
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_hybrid_pojo_stream():
    test_utils.start_ray()

    class WordCount:
        def __init__(self, i):
            self.word = ["a", "b", "c"][i % 3]
            self.count = i

    sink_file = "/tmp/test_hybrid_pojo_stream.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        print(f"test_hybrid_pojo_stream {x.word, x.count}")
        with open(sink_file, "a") as f:
            f.write(f"{x.word}:{x.count}\n")

    ctx = StreamingContext.Builder().build()
    data = [WordCount(i) for i in range(3)]
    sorted_data = sorted(data, key=lambda w: (w.word, w.count))
    from itertools import groupby
    result = {
        k: sum(map(lambda x: x.count, g))
        for k, g in groupby(sorted_data, lambda w: w.word)
    }
    print(f"Expected result: {result}")

    ctx.from_collection(data) \
        .key_by(lambda x: x.word) \
        .as_java_stream() \
        .reduce("com.alipay.streaming.runtime.integrationtest."
                "HybridStreamTest$WordCountReducer") \
        .as_python_stream() \
        .sink(sink_func)
    ctx.submit("test_hybrid_pojo_stream")

    def check_succeed():
        if os.path.exists(sink_file):
            import time
            time.sleep(3)  # Wait all data be written
            with open(sink_file, "r") as f:
                lines = f.readlines()
                cnt_a = [line.split(":")[1] for line in lines if "a" in lines]
                cnt_b = [line.split(":")[1] for line in lines if "b" in lines]
                cnt_c = [line.split(":")[1] for line in lines if "c" in lines]
                if cnt_a and cnt_b and cnt_c:
                    cnt_a = int(cnt_a[0])
                    cnt_b = int(cnt_b[0])
                    cnt_c = int(cnt_c[0])
                    # at least once may have some data duplicates
                    assert cnt_a >= result["a"]
                    assert cnt_b >= result["b"]
                    assert cnt_c >= result["c"]
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


if __name__ == "__main__":
    test_hybrid_stream()
