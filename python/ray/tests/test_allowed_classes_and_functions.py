# coding: utf-8
import collections
import io
import logging
import os
import re
import string
import sys
import weakref
import yaml
import tempfile

import numpy as np
import pytest
from numpy import log

import ray
import ray.cluster_utils
import ray.ray_constants as ray_constants

logger = logging.getLogger(__name__)


def test_allowed_classes(shutdown_only):

    @ray.remote
    class A:
        def __init__(self) -> None:
            pass

        def hello(self):
            return "hello"

    @ray.remote
    class B:
        def __init__(self) -> None:
            pass

        def hello(self):
            return "hello"

    @ray.remote
    class C:
        def __init__(self) -> None:
            pass

        def hello(self):
            return "hello"

    config_path = "/tmp/test_allowed_classes.yaml"
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_context = {
            "allowed_classes": [
                "test_allowed_classes_and_functions.A",
                "test_allowed_classes_and_functions.B",
                ]
        }
        yaml.safe_dump(config_context, open(config_path, "wt"))
        import os
        os.environ[ray_constants.RAY_ALLOWED_LIST_CONFIG_PATH] = config_path

        ray.init()
        a = A.remote()
        o = a.hello.remote()
        assert "hello" == ray.get(o)

        b = B.remote()
        o = b.hello.remote()
        assert "hello" == ray.get(o)

        c = C.remote()
        o = c.hello.remote()
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(o)

def test_allowed_functions(shutdown_only):

    @ray.remote
    def func1():
        return "hello"

    @ray.remote
    def func2():
        return "hello"

    @ray.remote
    def func3():
        return "hello"

    config_path = "/tmp/test_allowed_classes.yaml"
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_context = {
            "allowed_functions": [
                "test_allowed_classes_and_functions.func1",
                "test_allowed_classes_and_functions.func2",
                ]
        }
        yaml.safe_dump(config_context, open(config_path, "wt"))
        import os
        os.environ[ray_constants.RAY_ALLOWED_LIST_CONFIG_PATH] = config_path

        ray.init()
        o = func1.remote()
        assert "hello" == ray.get(o)
        o = func2.remote()
        assert "hello" == ray.get(o)
        o = func3.remote()
        with pytest.raises(ray.exceptions.WorkerCrashedError):
            ray.get(o)    


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
