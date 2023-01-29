import os
import logging
import subprocess
from setuptools import setup, find_packages

logger = logging.getLogger(__name__)
file_path = os.getcwd()

# build streaming package
bazel_build_cmd = "bazel build streaming_pkg --jobs=4"
cwd = f"{file_path}/../.."
subprocess.Popen(bazel_build_cmd, cwd=cwd, shell=True)

# package raystreaming
setup(
    name="raystreaming",
    version="1.0",
    author="realtime team",
    description="streaming module",
    packages=find_packages(),
    package_data={"raystreaming": ["_streaming.so"]},
    install_requires=[
        "msgpack>=0.6.2",
        "pyfury==0.0.6.5",
        "zdfs-dfs-gcc492",
        "pyarrow==4.0.0",
        "oss2==2.15.0",
        "antkv==0.0.1.dev4",
    ])
