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

logger = logging.getLogger(__name__)




def test_allowed_classes(shutdown_only):

    @ray.remote
    class A:
        def __init__(self) -> None:
            pass
            
        def hello(self):
            return "hello"

    
    config_path = "/tmp/test_allowed_classes.yaml"
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_context = {
            "allowed_classes": ["111"]
        }
        yaml.safe_dump(config_context, open(config_path, "wt"))
        import os
        os.environ["RAY_ALLOWED_CLASSES_AND_FUNCTIONS_CONFIG_PATH"] = config_path 
        
        ray.init()
        a = A.remote()
        o = a.hello.remote()
        print(ray.get(o))


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
