import sys
import pytest
import ray
import time

from ray.util import BaseExecutor, DynamicActorPool
from ray.test_utils import wait_for_condition


@pytest.fixture
def init():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@ray.remote
class MyExecutor(BaseExecutor):
    def __init__(self):
        time.sleep(0.5)

    def f(self, x):
        return x + 1

    def double(self, x):
        return 2 * x

    def fs(self, x):
        while True:
            x = x + 1
            time.sleep(1)
        return None


@ray.remote
class MyExecutor2(BaseExecutor):
    def __init__(self, p1, p2, p3="p3"):
        time.sleep(0.5)

    def f(self, x):
        return x + 1


def test_check_executor_health(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    for i in range(5):
        pool.submit(lambda a, v: a.__check_health__.remote(), i)
        assert pool.get_next() is True

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_get_next_with_init_options(init):
    pool = DynamicActorPool(
        MyExecutor,
        actor_num=3,
        num_cpus=1,
        name="my-executor",
        max_concurrency=10)

    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
        assert pool.get_next() == i + 1

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_get_next(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
        assert pool.get_next() == i + 1

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_get_next_unordered(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    total = []
    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
    while pool.has_next():
        total += [pool.get_next_unordered()]

    assert all(elem in [1, 2, 3, 4, 5] for elem in total)

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_map(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    index = 0
    for v in pool.map(lambda a, v: a.double.remote(v), range(5)):
        assert v == 2 * index
        index += 1

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_map_unordered(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    total = []
    for v in pool.map(lambda a, v: a.double.remote(v), range(5)):
        total += [v]

    assert all(elem in [0, 2, 4, 6, 8] for elem in total)

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_get_next_timeout(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    pool.submit(lambda a, v: a.fs.remote(v), 0)
    with pytest.raises(TimeoutError):
        pool.get_next_unordered(timeout=0.1)

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_get_next_unordered_timeout(init):
    pool = DynamicActorPool(MyExecutor, actor_num=3)

    pool.submit(lambda a, v: a.fs.remote(v), 0)
    with pytest.raises(TimeoutError):
        pool.get_next_unordered(timeout=0.1)

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_init_actor_with_arguments(init):
    pool = DynamicActorPool(
        MyExecutor2,
        actor_args=(
            "a1",
            "a2",
        ),
        actor_kwargs={"p3": "a3"},
        actor_num=3)

    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
        assert pool.get_next() == i + 1

    wait_for_condition(pool.is_initial_finished, timeout=10)


def test_check_executor_health_with_total_memory_updating(init):
    pool = DynamicActorPool(MyExecutor, init_total_memory_mb=500, actor_num=3)

    for i in range(5):
        pool.submit(lambda a, v: a.__check_health__.remote(), i)
        assert pool.get_next() is True

    wait_for_condition(pool.is_initial_finished, timeout=10)
    assert pool.total_memory_mb == 2000


def test_get_next_with_total_memory_updating(init):
    pool = DynamicActorPool(MyExecutor, init_total_memory_mb=500, actor_num=3)

    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
        assert pool.get_next() == i + 1

    wait_for_condition(pool.is_initial_finished, timeout=10)
    assert pool.total_memory_mb == 2000


def test_get_next_unordered_memory_updating(init):
    pool = DynamicActorPool(MyExecutor, init_total_memory_mb=500, actor_num=3)

    total = []
    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
    while pool.has_next():
        total += [pool.get_next_unordered()]

    assert all(elem in [1, 2, 3, 4, 5] for elem in total)

    wait_for_condition(pool.is_initial_finished, timeout=10)
    assert pool.total_memory_mb == 2000


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
