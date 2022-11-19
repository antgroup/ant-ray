import time

import pytest
import numpy as np

import ray
from ray._private.internal_api import memory_summary
from ray.tests.conftest import *  # noqa


def check_no_spill(ctx, pipe, prefetch_blocks: int = 0):
    # Run .iter_batches() for 10 secs, and we expect no object spilling.
    end_time = time.time() + 10
    for batch in pipe.iter_batches(batch_size=None, prefetch_blocks=prefetch_blocks):
        if time.time() > end_time:
            break
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_iter_batches_no_spilling_upon_no_transformation(shutdown_only):
    # The object store is about 300MB.
    ctx = ray.init(num_cpus=1, object_store_memory=300e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    check_no_spill(ctx, ds.repeat())
    check_no_spill(ctx, ds.repeat(), 5)

    check_no_spill(ctx, ds.window(blocks_per_window=20))
    check_no_spill(ctx, ds.window(blocks_per_window=20), 5)


def test_iter_batches_no_spilling_upon_rewindow(shutdown_only):
    # The object store is about 300MB.
    ctx = ray.init(num_cpus=1, object_store_memory=300e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    check_no_spill(
        ctx, ds.window(blocks_per_window=20).repeat().rewindow(blocks_per_window=10)
    )
    check_no_spill(
        ctx, ds.window(blocks_per_window=20).repeat().rewindow(blocks_per_window=10), 5
    )


def test_iter_batches_no_spilling_upon_prior_transformation(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # Repeat, with transformation prior to the pipeline.
    check_no_spill(ctx, ds.map_batches(lambda x: x).repeat())
    check_no_spill(ctx, ds.map_batches(lambda x: x).repeat(), 5)

    # Window, with transformation prior to the pipeline.
    check_no_spill(ctx, ds.map_batches(lambda x: x).window(blocks_per_window=20))
    check_no_spill(ctx, ds.map_batches(lambda x: x).window(blocks_per_window=20), 5)


def test_iter_batches_no_spilling_upon_post_transformation(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # Repeat, with transformation post the pipeline creation.
    check_no_spill(ctx, ds.repeat().map_batches(lambda x: x, batch_size=5))
    check_no_spill(ctx, ds.repeat().map_batches(lambda x: x, batch_size=5), 5)

    # Window, with transformation post the pipeline creation.
    check_no_spill(ctx, ds.window(blocks_per_window=20).map_batches(lambda x: x))
    check_no_spill(ctx, ds.window(blocks_per_window=20).map_batches(lambda x: x), 5)


def test_iter_batches_no_spilling_upon_transformations(shutdown_only):
    # The object store is about 700MB.
    ctx = ray.init(num_cpus=1, object_store_memory=700e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # Repeat, with transformation before and post the pipeline.
    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x, batch_size=5)
        .repeat()
        .map_batches(lambda x: x, batch_size=5),
    )
    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x, batch_size=5)
        .repeat()
        .map_batches(lambda x: x, batch_size=5),
        5,
    )

    # Window, with transformation before and post the pipeline.
    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x)
        .window(blocks_per_window=20)
        .map_batches(lambda x: x),
    )
    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x)
        .window(blocks_per_window=20)
        .map_batches(lambda x: x),
        5,
    )


def test_iter_batches_no_spilling_upon_shuffle(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    check_no_spill(ctx, ds.repeat().random_shuffle_each_window())
    check_no_spill(ctx, ds.repeat().random_shuffle_each_window(), 5)

    check_no_spill(ctx, ds.window(blocks_per_window=20).random_shuffle_each_window())
    check_no_spill(ctx, ds.window(blocks_per_window=20).random_shuffle_each_window(), 5)


def test_pipeline_splitting_has_no_spilling(shutdown_only):
    # The object store is about 800MiB.
    ctx = ray.init(num_cpus=1, object_store_memory=800e6)
    # The size of dataset is 50000*(80*80*4)*8B, about 10GiB, 50MiB/block.
    ds = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=200)

    # 2 blocks/window.
    p = ds.window(bytes_per_window=100 * 1024 * 1024).repeat()
    p1, p2 = p.split(2)

    @ray.remote
    def consume(p):
        for batch in p.iter_batches(batch_size=None):
            pass

    tasks = [consume.remote(p1), consume.remote(p2)]
    try:
        # Run it for 20 seconds.
        ray.get(tasks, timeout=20)
    except Exception:
        for t in tasks:
            ray.cancel(t, force=True)
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_pipeline_splitting_has_no_spilling_with_equal_splitting(shutdown_only):
    # The object store is about 1200MiB.
    ctx = ray.init(num_cpus=1, object_store_memory=1200e6)
    # The size of dataset is 50000*(80*80*4)*8B, about 10GiB, 50MiB/block.
    ds = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=200)

    # 150Mib/window, which is 3 blocks/window, which means equal splitting
    # will need to split one block.
    p = ds.window(bytes_per_window=150 * 1024 * 1024).repeat()
    p1, p2 = p.split(2, equal=True)

    @ray.remote
    def consume(p):
        for batch in p.iter_batches():
            pass

    tasks = [consume.remote(p1), consume.remote(p2)]
    try:
        # Run it for 20 seconds.
        ray.get(tasks, timeout=20)
    except Exception:
        for t in tasks:
            ray.cancel(t, force=True)
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_task_argument_reference(shutdown_only):

    def get_big_object(size):
        return np.random.randint(0, high=255, size=size, dtype=np.uint8)

    ray.init(
        # 150MB
        object_store_memory=150 * 1024 * 1024,
        _system_config = {
            "max_direct_call_object_size": 100 * 1024,
            # disable unlimited plasma store
            "oom_grace_period_s": 3600,
        }
    )

    # @ray.remote
    # def test_return_big_object(arg):
    #     del arg
    #     return get_big_object((80, 1024 * 1024))

    # ray.get(test_return_big_object.remote(get_big_object((80, 1024 * 1024))))
    class TestTuple(tuple):
        pass
    class TestObject:
        def __init__(self, value):
            print("__init__", self, value)
            self._value = value

        def __reduce__(self):
            print("__reduce__", self, self._value)
            return TestObject, TestTuple((self._value,))

        def __del__(self):
            print("__del__", self, self._value)


    @ray.remote
    def get_frame_locals(x):
        print("hejialing test")
        return TestObject(2)


    r = get_frame_locals.remote(TestObject(1))
    ray.get(r)



if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))