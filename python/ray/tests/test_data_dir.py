# This is the tests in out internal Ray only.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import ray
import os
import shutil


def test_data_dir():
    ray.init()

    @ray.remote
    class A(object):
        def create_data_dir(self):
            data_dir = ray._get_runtime_context().get_job_data_dir()
            if not os.path.exists(data_dir):
                raise Exception(f"Data dir {data_dir} not exists.")
            pid = str(os.getpid())
            if pid not in data_dir:
                raise Exception(
                    f"No current pid({pid}) in data dir {data_dir}.")
            return data_dir

    a = A.remote()
    data_dir = ray.get(a.create_data_dir.remote())
    print(data_dir)
    assert data_dir
    shutil.rmtree(data_dir)
    ray.shutdown()


test_data_dir()
