from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.test_utils import wait_for_condition


@test_utils.skip_if_no_streaming_jar()
def test_chained_operator():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()

    s1 = ctx.from_values("1", "2", "3")
    s2 = s1.map(lambda x: int(x))
    s3 = s2.filter(lambda x: int(x) < 2)\
        .map(lambda x: int(x) * 2)\
        .set_parallelism(2)

    s2.filter(lambda x: int(x) > 2)\
        .union(s3)\
        .sink(lambda x: print(x))

    ctx.submit("test_chained_operator")

    def check_succeed():
        gateway_client = ctx._gateway_client
        execution_vertex_size = gateway_client.get_execution_job_vertex_size()
        return execution_vertex_size == 3

    wait_for_condition(check_succeed, timeout=20, retry_interval_ms=1000)
    print("Execution succeed")
