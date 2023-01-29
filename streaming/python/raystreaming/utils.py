import json
import logging
import os
import socket
import sys
from collections import Set, Mapping, deque
from numbers import Number
from raystreaming.generated import common_pb2

logger = logging.getLogger(__name__)

zero_depth_bases = (str, bytes, Number, range, bytearray)
iteritems = "items"


def getsize(obj_0):
    """Recursively iterate to sum size of object & members."""
    _seen_ids = set()

    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        if isinstance(obj, zero_depth_bases):
            pass  # bypass remaining control flow and return
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
            size += sum(
                inner(k) + inner(v) for k, v in getattr(obj, iteritems)())
        # Check for custom object instances - may subclass above too
        if hasattr(obj, "__dict__"):
            size += inner(vars(obj))
        if hasattr(obj, "__slots__"):  # can have __slots__ with __dict__
            size += sum(
                inner(getattr(obj, s)) for s in obj.__slots__
                if hasattr(obj, s))
        return size

    return inner(obj_0)


class EnvUtil:
    """
    env util
    """

    DEV_ENV_VALUE = "dev"
    ENV_KEY = "ALIPAY_APP_ENV"
    UT_MODE = "streaming_python_ut_mode"
    CLUSTER_NAME = "cluster_name"
    IDC_NAME = "IDCNAME"
    WORKING_DIR = "RAY_JOB_DIR"

    @staticmethod
    def is_online_env():
        env = os.getenv(EnvUtil.ENV_KEY)
        return env is not None

    @staticmethod
    def get_host_address():
        host_ip = ""
        try:
            host_name = socket.gethostname()
            host_ip = socket.gethostbyname(host_name)
        except Exception:
            logger.exception("Unable to get hostname and ip")
        return host_ip

    @staticmethod
    def set_ut_mode():
        # str type
        os.environ[EnvUtil.UT_MODE] = "true"

    @staticmethod
    def unset_ut_mode():
        del os.environ[EnvUtil.UT_MODE]

    @staticmethod
    def is_ut_mode():
        if os.environ.__contains__(EnvUtil.UT_MODE):
            return os.environ[EnvUtil.UT_MODE] == "true"
        return False

    @staticmethod
    def get_working_dir():
        return os.environ[EnvUtil.WORKING_DIR] \
            if os.environ.__contains__(EnvUtil.WORKING_DIR) else os.getcwd()

    @staticmethod
    def get_cluster_name():
        return os.getenv(EnvUtil.CLUSTER_NAME) \
            if os.environ.__contains__(EnvUtil.CLUSTER_NAME) \
            else EnvUtil.DEV_ENV_VALUE

    @staticmethod
    def get_idc_name():
        return os.getenv(EnvUtil.IDC_NAME) \
            if os.environ.__contains__(EnvUtil.IDC_NAME) \
            else EnvUtil.DEV_ENV_VALUE

    @staticmethod
    def get_pid():
        return os.getpid()

    @staticmethod
    def get_ip_address():
        address = socket.getaddrinfo(socket.gethostname(), None)
        return [item[4][0] for item in address if ":" not in item[4][0]][0]

    @staticmethod
    def get_hostname():
        return socket.gethostname()


def parse_job_config(args):
    logger.info(f"Parse args {args}")
    job_config = {}
    if "-rayagCallback" in args:
        # last index of rayagCallback to avoid conflict with users params
        callback_arg_index = len(args) - 1 - args[::-1].index("-rayagCallback")
        callback_str = args[callback_arg_index + 1]
        if callback_str.startswith("\'"):
            callback_str = callback_str[1:-1]
        callback = json.loads(callback_str)
        job_config.update(parse_antc_config(callback))
    if "-jobConf" in args:
        # last index of jobConf to avoid conflict with users params
        job_conf_index = len(args) - 1 - args[::-1].index("-jobConf")
        job_conf_value = args[job_conf_index + 1]
        if os.path.exists(job_conf_value):
            with open(job_conf_value) as f:
                job_conf = json.loads(f.read())
        else:
            job_conf = json.loads(job_conf_value)
        job_config.update(job_conf)
    return job_config


def parse_antc_config(antc_config):
    config = {
        "streaming.queue.size": "5000000",
        "streaming.health-check.interval.secs": "1200",
        "streaming.checkpoint.timeout.secs": "120",
        "streaming.checkpoint.interval.secs": "600",
        "save_checkpoint_mode_py": "save_checkpoint_async_py",
        "StreamingWriterConsumedStep": "100",
        "StreamingReaderConsumedStep": "30",
        "StreamingRingBufferCapacity": "500",
        "StreamingEmptyMessageTimeInterval": "500",
        "kepler.storage.capacity": "5000",
        "kepler.storage.hbase.client.retries.number": "10",
        "enable.first.partition.skip": "false",
        "kepler.state.backend.type": "MEMORY",
        "kepler.job.max.parallel": "1024",
        "kepler.state.table.name": "rayag_store_table",
        "kepler.strategy.type": "CHECKPOINT",
    }

    if "jobId" in antc_config and "antcUrl" in antc_config:
        config["jobId"] = antc_config["jobId"]
        config["antcUrl"] = antc_config["antcUrl"]
    if "jobName" in antc_config:
        config["jobName"] = antc_config["jobName"]
    if "stateConfig" in antc_config:
        config.update(antc_config["stateConfig"])
    if "metaConfig" in antc_config:
        config.update(antc_config["metaConfig"])
    if "runtimeConfig" in antc_config:
        config.update(antc_config["runtimeConfig"])

    keymap = {
        "kepler.state.table.name": "stateTable",
        "kepler.storage.hbase.zookeeper.quorum": "quorum",
        "kepler.storage.zookeeper.znode.parent": "zkNode",
        "kepler.tx.table": "txTable",
        "streaming.checkpoint.state.backend.pangu.store.dir": "panguRootDir",
        "streaming.checkpoint.state.backend.pangu.cluster.name": "panguClusterName"  # noqa: E501
    }

    for key in keymap.keys():
        config[key] = config.pop(keymap[key])

    if "streaming.checkpoint.state.backend.pangu.cluster.name" in config \
            and "streaming.checkpoint.state.backend.pangu.store.dir" in config:
        config["streaming.checkpoint.state.backend.type"] = "PANGU"
    else:
        config["streaming.checkpoint.state.backend.type"] = "MEMORY"

    return config


# Args[0] is class self, args[1] is the control message bytes.
# A decorator used to convert the bytes to control message.
def read_control_message(func):
    def wrapper(self, *args, **kwargs):
        united_distributed_control_message_bytes = args[0]
        udc_msg = common_pb2.UnitedDistributedControlMessage()
        udc_msg.ParseFromString(united_distributed_control_message_bytes)
        return func(self, udc_msg)

    return wrapper
