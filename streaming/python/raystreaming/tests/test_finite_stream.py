import os
import psutil
import time

from ray.streaming.function import CollectionSourceFunction, SinkFunction
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils


def word_count(source_func, sink_func, sink_file):
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.external.interaction.enable", "false") \
        .build()

    ctx.source(source_func(("a", "b", "c", "d"))) \
        .set_parallelism(1) \
        .flat_map(lambda x: [x, x]) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .sink(sink_func())
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

    return ctx


@test_utils.skip_if_no_streaming_jar()
def test_shutdown_workers():
    class Source(CollectionSourceFunction):
        def __init__(self, values):
            super().__init__(values)

        def open(self, runtime_context):
            print("Source open")
            with open(source_pid_file, "a") as f:
                f.write(str(os.getpid()))

        def init(self, parallel, index):
            print("Source init")

    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    source_pid_file = "/tmp/ray_streaming_test_shutdown_workers_source_pid.txt"
    sink_pid_file = "/tmp/ray_streaming_test_shutdown_workers_sink_pid.txt"
    sink_file = "/tmp/ray_streaming_test_shutdown_workers.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)
    if os.path.exists(source_pid_file):
        os.remove(source_pid_file)
    if os.path.exists(sink_pid_file):
        os.remove(sink_pid_file)

    class Sink(SinkFunction):
        def open(self, runtime_context):
            print("Sink open")
            with open(sink_pid_file, "a") as f:
                f.write(str(os.getpid()))

        def sink(self, x):
            print("Sink sink value: {}".format(x))
            with open(sink_file, "a") as f:
                line = "{}:{},".format(x[0], x[1])
                print("sink_func", line)
                f.write(line)

    ctx = word_count(Source, Sink, sink_file)

    time.sleep(5)
    is_job_finished = ctx._gateway_client.is_job_finished()
    print("is_job_finished: {}".format(is_job_finished))
    assert is_job_finished

    ctx._gateway_client.shutdown_all_workers()
    time.sleep(5)

    def getpid(file):
        with open(file, "r") as f:
            return int(f.read())

    pids = psutil.pids()
    assert getpid(source_pid_file) not in pids
    assert getpid(sink_pid_file) not in pids


@test_utils.skip_if_no_streaming_jar()
def test_on_finish():
    sink_file = "/tmp/ray_streaming_test_shutdown_workers.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    class Source(CollectionSourceFunction):
        def __init__(self, values):
            super().__init__(values)

        def open(self, runtime_context):
            pass

        def init(self, parallel, index):
            print("Source init")

        def on_finish(self):
            print("on_finish called")

    class Sink(SinkFunction):
        def open(self, runtime_context):
            self.sink_lines = []
            print("Sink open")

        def sink(self, x):
            print("Sink sink value: {}".format(x))
            self.sink_lines.append("{}:{},".format(x[0], x[1]))

        def on_finish(self):
            print("Sink on_finish")
            with open(sink_file, "a") as f:
                for line in self.sink_lines:
                    print("sink_func", line)
                    f.write(line)

    word_count(Source, Sink, sink_file)
