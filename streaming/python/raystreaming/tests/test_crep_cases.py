import os

import ray
from ray.streaming import StreamingContext
from ray.streaming.function import FlatMapFunction, MapFunction
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition
import uuid
import logging
logger = logging.getLogger(__name__)


@test_utils.skip_if_no_streaming_jar()
def test_map_map_reduce():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("image_dataset_size", "6")\
        .build()

    class MapBroadcastImage(FlatMapFunction):
        def open(self, runtime_context):
            self.image_dataset_size = int(
                runtime_context.get_job_config()["image_dataset_size"])

        def flat_map(self, value, collector):
            uuid_key = str(uuid.uuid4())
            for i in range(self.image_dataset_size):
                collector.collect((uuid_key, i, value))

    class MapImageComputing(MapFunction):
        FIXED_IMAGE_VECTOR = [(1, 2), (-1, -2), (3, 4), (3, -8), (2, -5), (-3,
                                                                           -4)]

        def open(self, runtime_context):
            self.index = runtime_context.get_task_index()
            self.para = runtime_context.get_parallelism()

        def gen_result(self, index, value):
            return (
                value[2][0] * MapImageComputing.FIXED_IMAGE_VECTOR[index][0] +
                value[2][1] * MapImageComputing.FIXED_IMAGE_VECTOR[index][1])

        def map(self, value):
            result = self.gen_result(value[1], value)
            return (value[0], value[1], result)

    class MergeImageResult(FlatMapFunction):
        def open(self, runtime_context):
            self.image_dataset_size = int(
                runtime_context.get_job_config()["image_dataset_size"])
            self.cache_result = {}

        def flat_map(self, value, collector):
            if value[0] in self.cache_result.keys():
                merge_item = self.cache_result[value[0]]
                if merge_item[1] < value[2]:
                    merge_item = (merge_item[0], value[2], value[1])
                inc_count = merge_item[0] + 1
                if inc_count >= self.image_dataset_size:
                    del self.cache_result[value[0]]
                    collector.collect((merge_item[1], merge_item[2]))
                else:
                    self.cache_result[value[0]] = (inc_count, merge_item[1],
                                                   merge_item[2])
            else:
                self.cache_result[value[0]] = (1, value[2], value[1])

    sink_file = "/tmp/ray_streaming_test_crep.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            f.write(f"{x[0]} {x[1]}\n")
            f.flush()

    ctx.from_values((1, 2), (1, -2), (0, 1), (1, 0)) \
        .set_parallelism(1) \
        .flat_map(MapBroadcastImage()) \
        .key_by(lambda x: x[1]) \
        .map(MapImageComputing()) \
        .key_by(lambda x: x[0]) \
        .flat_map(MergeImageResult()) \
        .sink(sink_func)

    ctx.submit("image_creps")

    def check_succeed():
        with open(sink_file, "r") as f:
            lines = [line.strip().split(" ") for line in f.readlines()]
            return {item[0]: item[1]
                    for item in lines} == {
                        "11": "2",
                        "19": "3",
                        "4": "2",
                        "3": "2"
                    }

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    ray.shutdown()
