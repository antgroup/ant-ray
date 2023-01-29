# coding: utf-8
import sys

import pytest

import ray
import ray.workers.setup_worker


def test_parse_allocated_resource():
    test_1 = ray.workers.setup_worker.parse_allocated_resource(
        r'{"CPU":2,"memory":10737418240000}')
    assert test_1 == ["--cpus=2", "--memory=10737418m"]
    test_2 = ray.workers.setup_worker.parse_allocated_resource(
        r'{"CPU":[1,0,1],"memory":10737418240000}')
    assert test_2 == ["--cpus=2", "--memory=10737418m"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
