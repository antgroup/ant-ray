from ray.streaming import function
from ray.streaming import operator
from ray.streaming.collector import Collector
from ray.streaming.context import RuntimeContextImpl
from ray.streaming.message import Record
from ray.streaming.operator import OperatorType, ArrowOperator, \
    IndependentOperator
from ray.streaming.runtime import gateway_client
import pytest
import time


def test_create_operator_with_func():
    map_func = function.SimpleMapFunction(lambda x: x)
    map_operator = operator.create_operator_with_func(map_func)
    assert type(map_operator) is operator.MapOperator


class MapFunc(function.MapFunction):
    def map(self, value):
        return str(value)


class EmptyOperator(operator.StreamOperator):
    def __init__(self):
        super().__init__(function.EmptyFunction())

    def operator_type(self) -> OperatorType:
        return OperatorType.ONE_INPUT


def test_load_operator():
    # function_bytes, module_name, class_name,
    descriptor_func_bytes = gateway_client.serialize(
        [None, __name__, MapFunc.__name__, "MapFunction"])
    descriptor_op_bytes = gateway_client.serialize(
        [descriptor_func_bytes, "", "", None, None, {}, None, []])
    map_operator = operator.load_operator(descriptor_op_bytes)
    assert type(map_operator) is operator.MapOperator
    descriptor_op_bytes = gateway_client.serialize(
        [None, __name__, EmptyOperator.__name__, None, None, {}, None, []])
    test_operator = operator.load_operator(descriptor_op_bytes)
    assert isinstance(test_operator, EmptyOperator)


def test_encoder():
    class A:
        def __init__(self, f1, f2):
            self.f1 = f1
            self.f2 = f2

    value = A("str1", 1)
    import pyarrow as pa
    input_schema = pa.schema([("f1", pa.utf8()), ("f2", pa.int32())])
    output_schema = pa.schema([("f1", pa.utf8()), ("f2", pa.int32())])

    def map_func(x):
        print(f"encoded value {x}")
        assert x.f1 == value.f1
        assert x.f2 == value.f2

    input_schema_bytes = input_schema.serialize().to_pybytes()
    output_schema_bytes = output_schema.serialize().to_pybytes()
    descriptor_func_bytes = gateway_client.serialize([
        function.serialize(function.SimpleMapFunction(map_func)), None, None,
        "MapFunction"
    ])
    descriptor_op_bytes = gateway_client.serialize([
        descriptor_func_bytes, "", "", input_schema_bytes, output_schema_bytes,
        {}, None, []
    ])
    test_operator = operator.load_operator(descriptor_op_bytes)
    assert isinstance(test_operator, operator.MapOperator)
    print(f"test_operator {test_operator}")
    import pyfury as fury
    encoder = fury.create_row_encoder(input_schema)
    test_operator.open([], RuntimeContextImpl(0, 0, 1, None, None))

    test_operator.process_element(Record(encoder.to_row(value).to_bytes()))


@pytest.mark.parametrize("batch_size, timeout_milliseconds",
                         [(4, 2**31 - 1), (2**31 - 1, 500)])
def test_arrow_operator(batch_size, timeout_milliseconds):
    import pyarrow as pa
    data = [
        pa.array([1, 2, 3, 4]),
        pa.array(["foo", "bar", "baz", None]),
        pa.array([True, None, False, True])
    ]
    batch = pa.record_batch(data, names=["f0", "f1", "f2"])
    df = batch.to_pandas()
    schema = batch.schema
    op = ArrowOperator()
    op.input_schema = schema

    result = []

    class TestCollector(Collector):
        def __init__(self):
            pass

        def collect(self, record):
            result.append(record.value)

    def get_config():
        prefix = f"{ArrowOperator.__module__}.{ArrowOperator.__name__}"
        return {
            f"{prefix}.batchSize": batch_size,
            f"{prefix}.timeoutMilliseconds": timeout_milliseconds,
        }

    ctx = RuntimeContextImpl(0, 0, 1, None, None)
    op.set_op_config(get_config())
    op.open([TestCollector()], ctx)
    import pyfury as fury
    cls = fury.record_class_factory("TestArrowOperator", schema.names)
    for index, row in df.iterrows():
        print(cls(**row.to_dict()))
        op.process_element(Record(cls(**row.to_dict())))
    while not result:
        time.sleep(0.1)
    result_batch = result.pop(0)
    print(f"result_batch {result_batch}")
    assert result_batch == batch


def test_independent_operator():
    operator = IndependentOperator({"k1": "v1"})
    assert operator is not None
    assert operator.config == {"k1": "v1"}


if __name__ == "__main__":
    test_arrow_operator(4, 2**31 - 1)
    test_arrow_operator(2**31 - 1, 500)
    test_independent_operator()
