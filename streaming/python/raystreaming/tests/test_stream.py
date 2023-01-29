import ray
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.streaming.function import Language

from raystreaming.context import IndependentOperatorDescriptor


@test_utils.skip_if_no_streaming_jar()
def test_data_stream():
    test_utils.start_ray()
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values(1, 2, 3)
    java_stream = stream.as_java_stream()
    python_stream = java_stream.as_python_stream()
    assert stream.get_id() == java_stream.get_id()
    assert stream.get_id() == python_stream.get_id()
    python_stream.set_parallelism(10)
    assert stream.get_parallelism() == java_stream.get_parallelism()
    assert stream.get_parallelism() == python_stream.get_parallelism()
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_key_data_stream():
    test_utils.start_ray()
    ctx = StreamingContext.Builder().build()
    key_stream = ctx.from_values(
        "a", "b", "c").map(lambda x: (x, 1)).key_by(lambda x: x[0])
    java_stream = key_stream.as_java_stream()
    python_stream = java_stream.as_python_stream()
    assert key_stream.get_id() == java_stream.get_id()
    assert key_stream.get_id() == python_stream.get_id()
    python_stream.set_parallelism(10)
    assert key_stream.get_parallelism() == java_stream.get_parallelism()
    assert key_stream.get_parallelism() == python_stream.get_parallelism()
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_stream_config():
    test_utils.start_ray()
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values(1, 2, 3)
    stream.with_config("k1", "v1")
    print("config", stream.get_config())
    assert stream.get_config() == {"k1": "v1"}
    stream.with_config(conf={"k2": "v2", "k3": "v3"})
    print("config", stream.get_config())
    assert stream.get_config() == {"k1": "v1", "k2": "v2", "k3": "v3"}
    java_stream = stream.as_java_stream()
    java_stream.with_config(conf={"k4": "v4"})
    config = java_stream.get_config()
    print("config", config)
    assert config == {"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4"}
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_stream_dynamic_division():
    test_utils.start_ray()
    ctx = StreamingContext.Builder().option(
        "streaming.scheduler.strategy.type", "random").build()
    stream = ctx \
        .from_values(1, 2, 3) \
        .set_parallelism(2) \
        .set_dynamic_division_num(2)
    assert stream.get_dynamic_division_num() == 2
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_stream_resource():
    test_utils.start_ray()
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values(1, 2, 3)
    stream.with_resource("CPU", 2)
    print("resource", stream.get_resource())
    assert stream.get_resource() == {"CPU": 2}
    stream.with_resource(resources={"CPU": 2, "MEM": 3})
    print("resource", stream.get_resource())
    assert stream.get_resource() == {"CPU": 2, "MEM": 3}
    java_stream = stream.as_java_stream()
    java_stream.with_resource(resources={"GPU": 0.5})
    resource = java_stream.get_resource()
    print("resource", resource)
    assert resource == {"CPU": 2, "MEM": 3, "GPU": 0.5}
    ray.shutdown()


def test_independent_operator_descriptor():
    class_name = "className"
    module_name = "moduleName"
    descriptor = IndependentOperatorDescriptor(class_name, module_name,
                                               Language.PYTHON)
    assert descriptor is not None
    assert descriptor.parallelism == 1
    assert descriptor.config == {}
    assert descriptor.resource == {}
    assert not descriptor.is_lazy_scheduling

    descriptor.set_parallelism(2)
    assert descriptor.parallelism == 2
    descriptor.with_config({"k1": "v1"})
    assert len(descriptor.config) == 1
    assert descriptor.config["k1"] == "v1"
    descriptor.with_resource({"CPU": 2})
    assert len(descriptor.resource) == 1
    assert descriptor.resource["CPU"] == 2
    descriptor.set_lazy_scheduling()
    assert descriptor.is_lazy_scheduling

    json_list = [
        '{"className":"class2","moduleName":"","language":"JAVA",'
        '"parallelism":1,"resource":{"CPU":0.1},"config":{"k1":"v1"},'
        '"isLazyScheduling":true}',
        '{"className":"class1","moduleName":"module1","language":"PYTHON",'
        '"parallelism":1,"resource":{},"config":{},"isLazyScheduling":false}'
    ]
    for str in json_list:
        descriptor = IndependentOperatorDescriptor\
            .parse_json_from_java(str)
        assert descriptor is not None
        assert descriptor.class_name == "class2" \
            or descriptor.class_name == "class1"


@test_utils.skip_if_no_streaming_jar()
def test_stream_context_with_independent_operator():
    test_utils.start_ray()
    ctx = StreamingContext.Builder().build()

    ctx.with_independent_operator("class1", "module1", Language.PYTHON)
    ctx.with_independent_operator("class2", "", Language.JAVA, 1, {"CPU": 0.1},
                                  {"k1": "v1"}, True)
    independent_operators = ctx.get_independent_operators()
    assert independent_operators is not None
    assert len(independent_operators) == 2
    for independent_operator in independent_operators:
        if independent_operator.language == Language.JAVA:
            assert independent_operator.class_name == "class2"
            assert independent_operator.config["k1"] == "v1"
        else:
            assert independent_operator.class_name == "class1"
            assert independent_operator.module_name == "module1"
            assert independent_operator.config == {}

    ray.shutdown()


if __name__ == "__main__":
    test_stream_config()
    test_stream_resource()
    test_independent_operator_descriptor()
    test_stream_context_with_independent_operator()
