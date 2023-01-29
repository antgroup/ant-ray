import os
import time
import ray

from ray.streaming import StreamingContext
from ray.streaming.function import SourceContext, SourceFunction
from ray.streaming.tests import test_utils


@test_utils.skip_if_no_streaming_jar()
def test_rescale_basic(shutdown_only):
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.partition.enable_dr", "false") \
        .option("streaming.scheduler.strategy.type", "random") \
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

    ctx.source(Source()) \
        .set_parallelism(1) \
        .flat_map(lambda x: [x, x]) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .sink(sink_func)
    ctx.submit("word_count_rescale")

    # rescale after 5 seconds
    time.sleep(5)
    result = ctx._gateway_client.do_rescale("{\"5\":\"2\"}")

    # wait 10s for rescaling finish
    time.sleep(20)
    assert ctx.get_failover_info() == []
    event_info = ctx._gateway_client.get_event_info(5)
    assert len(event_info) > 0
    assert result
    rescale_result = False
    for event in event_info:
        if "eventState=FINISHED, eventName=RESCALING" in event:
            rescale_result = True
    assert rescale_result
    print("Execution succeed")
    ray.shutdown()


class Source(SourceFunction):
    class House:
        def __init__(self, attrs):
            self.__dict__.update(attrs)

    def __init__(self):
        pass

    def init(self, parallel, index):
        pass

    def fetch(self, ctx: SourceContext, checkpoint_id: int):
        ctx.collect("1")


if __name__ == "__main__":
    test_utils.start_ray()
    test_rescale_basic(None)
