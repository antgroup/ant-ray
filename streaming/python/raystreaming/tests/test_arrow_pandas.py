import os

import functools
import pytest
import ray
from ray.streaming import StreamingContext
from ray.streaming.function import SourceContext, SourceFunction
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition


def load_housing_data():
    import pandas as pd
    current_dir = os.path.abspath(os.path.dirname(__file__))
    csv_path = os.path.join(current_dir, "housing.csv")
    df = pd.read_csv(csv_path)
    df.sort_index(axis=1, inplace=True)
    return df


class Source(SourceFunction):
    class House:
        def __init__(self, attrs):
            self.__dict__.update(attrs)

    def __init__(self):
        self.housing = load_housing_data()

    def init(self, parallel, index):
        assert parallel == 1

    def fetch(self, ctx: SourceContext, checkpoint_id: int):
        if self.housing is not None:
            for index, row in self.housing.iterrows():
                ctx.collect(Source.House(row.to_dict()))
        self.housing = None


@test_utils.skip_if_no_streaming_jar()
@pytest.mark.parametrize("batch_size, timeout_milliseconds",
                         [(len(load_housing_data()), 2**31 - 1),
                          (2**31 - 1, 500)])
def test_to_arrow(batch_size, timeout_milliseconds):
    test_utils.start_ray()
    housing = load_housing_data()
    num_rows, num_columns = housing.shape
    success_flag_file = "/tmp/ray_streaming_test_to_arrow.txt"
    if os.path.exists(success_flag_file):
        os.remove(success_flag_file)

    def sink(record_batch):
        assert record_batch.num_rows == num_rows
        assert record_batch.num_columns == num_columns
        with open(success_flag_file, "w") as f:
            f.write("Succeed")

    import pyarrow as pa
    schema = pa.Schema.from_pandas(housing)
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    ctx.source(Source()) \
        .with_schema(schema) \
        .set_parallelism(1) \
        .to_arrow_stream(batch_size, timeout_milliseconds) \
        .sink(sink)
    ctx.submit("test_to_arrow")

    def check_succeed():
        if os.path.exists(success_flag_file):
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


def sink(success_flag_file, data):
    housing = load_housing_data()
    sum_statistics = housing.sum()
    keys = "housing_median_age,total_rooms,total_bedrooms,population," \
           "households,median_income,median_house_value".split(",")
    expected = {k: sum_statistics.loc[k] for k in keys}
    import pandas as pd
    df = data if isinstance(data, pd.DataFrame) else data.to_pandas()
    df_sum_statistics = df.sum()
    result = {k: df_sum_statistics.loc[k] for k in keys}
    for k, v in result.items():
        print(f"expect {expected[k]} and got {v}")
        # data may be duplicate in at least once mode, so we use `>=`
        assert v >= expected[k], f"expect {expected[k]} but got {v}"
    with open(success_flag_file, "w") as f:
        f.write("Succeed")


@test_utils.skip_if_no_streaming_jar()
@pytest.mark.parametrize("batch_size, timeout_milliseconds",
                         [(len(load_housing_data()), 2**31 - 1),
                          (2**31 - 1, 500)])
def test_to_pandas(batch_size, timeout_milliseconds):
    test_utils.start_ray()
    success_flag_file = "/tmp/ray_streaming_test_to_pandas.txt"
    if os.path.exists(success_flag_file):
        os.remove(success_flag_file)
    import pyarrow as pa
    schema = pa.Schema.from_pandas(load_housing_data())
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    ctx.source(Source()) \
        .with_schema(schema) \
        .set_parallelism(1) \
        .to_pandas_stream(batch_size, timeout_milliseconds) \
        .sink(functools.partial(sink, success_flag_file))
    ctx.submit("test_to_pandas")

    def check_succeed():
        if os.path.exists(success_flag_file):
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
@pytest.mark.parametrize("batch_size, timeout_milliseconds",
                         [(len(load_housing_data()), 2**31 - 1),
                          (2**31 - 1, 500)])
def test_to_arrow_transfer(batch_size, timeout_milliseconds):
    test_utils.start_ray()
    success_flag_file = "/tmp/test_to_arrow_transfer.txt"
    if os.path.exists(success_flag_file):
        os.remove(success_flag_file)
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .option("streaming.reliability.level", "EXACTLY_ONCE") \
        .build()
    ctx.source(Source()) \
        .set_parallelism(1) \
        .as_java_stream() \
        .filter("com.alipay.streaming.runtime.integrationtest."
                "HybridStreamTest$HouseFilter") \
        .to_arrow_stream(batch_size, timeout_milliseconds) \
        .as_python_stream() \
        .sink(functools.partial(sink, success_flag_file))
    ctx.submit("test_to_arrow_transfer")

    def check_succeed():
        if os.path.exists(success_flag_file):
            return True
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


if __name__ == "__main__":
    print(load_housing_data().head())
    test_to_arrow(len(load_housing_data()), 2**31 - 1)
    test_to_arrow(2**31 - 1, 500)
    test_to_pandas(len(load_housing_data()), 2**31 - 1)
    test_to_pandas(2**31 - 1, 500)
    test_to_arrow_transfer(len(load_housing_data()), 2**31 - 1)
    test_to_arrow_transfer(2**31 - 1, 500)
