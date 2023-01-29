import subprocess
import time
from typing import List
import random

import ray
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.streaming.function import SourceFunction
import pytest


class TestSource(SourceFunction):
    def init(self, parallel, index):
        pass

    def fetch(self, ctx, checkpoint_id):
        time.sleep(0.1)
        ctx.collect("hello ray")


@test_utils.skip_if_no_streaming_jar()
@pytest.mark.parametrize("hdfs_type", ["pangu", "dfs"])
def test_word_count(hdfs_type):
    try:
        jobname = "test_word_count_failover_" + str(random.random())
        remote_type = "streaming.checkpoint.state.backend.hdfs.store.type"
        test_utils.start_ray()
        ctx = StreamingContext.Builder() \
            .option("streaming.metrics.reporters", "") \
            .option("streaming.checkpoint.state.backend.type", "HDFS") \
            .option("streaming.operator.state.backend.type", "ANTKV") \
            .option("streaming.checkpoint.timeout.secs", "10") \
            .option(remote_type, hdfs_type.upper()) \
            .option("streaming.checkpoint.state.backend.{}.store.dir".format(
                hdfs_type),
                    "/tmp/ray/cp_files/" + jobname + "/") \
            .option("streaming.reliability.level", "EXACTLY_ONCE") \
            .option("streaming.scheduler.strategy.type", "random") \
            .option("streaming.external.interaction.enable", "false") \
            .build()

        print("-----------submit job-------------")

        ctx.source(TestSource()) \
            .set_parallelism(4) \
            .flat_map(lambda x: x.split()) \
            .map(lambda x: (x, 1)) \
            .key_by(lambda x: x[0]) \
            .reduce(lambda old_value, new_value:
                    (old_value[0], old_value[1] + new_value[1])) \
            .set_parallelism(1) \
            .filter(lambda x: "ray" not in x) \
            .sink(lambda x: print("####result", x))
        ctx.submit(jobname)

        print("-----------checking output-------------")
        retry_count = 180 / 10  # wait for 3min
        while not has_sink_output():
            time.sleep(10)
            retry_count -= 1
            if retry_count <= 0:
                raise RuntimeError("Can not find output")

        print("-----------killing worker-------------")
        time.sleep(5)
        kill_all_worker()

        print("-----------checking checkpoint-------------")
        cp_ok_num = checkpoint_success_num()
        retry_count = 300 / 5  # wait for 5min
        while True:
            cur_cp_num = checkpoint_success_num()
            print(
                "-----------checking checkpoint, cur_cp_num={}, old_cp_num={}-------------".  # noqa: E501
                format(cur_cp_num, cp_ok_num))
            if cur_cp_num > cp_ok_num:
                print("--------------Checkpoint OK!------------------")
                break
            time.sleep(5)
            retry_count -= 1
            if retry_count <= 0:
                raise RuntimeError(
                    "Checkpoint keeps failing after fail-over, test failed!")

        print("-----------Checking output after failover-------------")
        output_num = sink_output_num()
        retry_count = 60 / 5  # wait for 1min
        while True:
            cur_output_num = sink_output_num()
            print(
                "-----------checking output after failover, cur_output_num={}, output_num={}-------------".  # noqa: E501
                format(cur_output_num, output_num))
            if cur_output_num > output_num:
                print("--------------TEST OK!------------------")
                break
            time.sleep(5)
            retry_count -= 1
            if retry_count <= 0:
                raise RuntimeError(
                    "No new ouput after fail-over, test failed!")
    finally:
        ray.shutdown()


def run_cmd(cmd: List):
    try:
        out = subprocess.check_output(cmd).decode()
    except subprocess.CalledProcessError as e:
        out = str(e)
    return out


def grep_log(keyword: str) -> str:
    out = subprocess.check_output(
        ["grep", "-r", keyword, "/tmp/ray/session_latest/logs"])
    return out.decode()


def has_sink_output() -> bool:
    try:
        grep_log("####result")
        return True
    except Exception:
        return False


def has_sink_output_after_failover() -> bool:
    try:
        grep_log("####result")
        return True
    except Exception:
        return False


def checkpoint_success_num() -> int:
    try:
        return grep_log("Finish checkpoint").count("\n")
    except Exception:
        return 0


def sink_output_num() -> int:
    try:
        return grep_log("####result").count("\n")
    except Exception:
        return 0


def kill_all_worker():
    return subprocess.run([
        "bash",
        "-c",
        "grep -r \'Initializing job worker, exe_vert\' /tmp/ray/session_latest/logs | awk -F\'pid\' \'{print $2}\' | awk -F\'=\' \'{print $2}\' | xargs kill -9"  # noqa: E501
    ])


if __name__ == "__main__":
    test_word_count()
