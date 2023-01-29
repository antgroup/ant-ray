import pytest
from contextlib import contextmanager

import ray
from ray.tests.conftest import get_default_fixture_ray_kwargs


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture()
def ray_start_forcibly(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@contextmanager
def _ray_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    if ray.is_initialized():
        print("Ray is already started, shutdown ray first")
        ray.shutdown()
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
