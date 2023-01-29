import logging
import os
import sys

import pytest
import ray
import ray.job_config
import ray.streaming.utils

STREAMING_JAR = "STREAMING_JAR"

logger = logging.getLogger(__name__)


def skip_if_no_streaming_jar():
    try:
        import pytide.runtime.jars  # noqa: F401
        skip = False
    except ImportError:
        skip = STREAMING_JAR not in os.environ
    return pytest.mark.skipif(skip, reason="Need streaming jar")


def start_ray():
    classpath = os.environ.get(STREAMING_JAR)
    print("classpath", os.environ.get(STREAMING_JAR))
    if ray.is_initialized():
        ray.shutdown()
    """
    add "_memory=1024*1024*1024" for local test if resource is not enough
    e.g.
    ray.init(
            job_config=ray.job_config.JobConfig(
                num_java_workers_per_process=1, code_search_path=[classpath]),
            resources={
                "MEM": 16,
                "RES-A": 4
            },
            _memory=16*1024*1024*1024)
    """

    ray.init(
        job_config=ray.job_config.JobConfig(
            num_java_workers_per_process=1, code_search_path=[
                classpath,
            ]),
        resources={
            "MEM": 16,
            "RES-A": 4
        },
        num_cpus=16)


def start_ray_python_only(load_code_from_local=True):
    if ray.is_initialized():
        ray.shutdown()
    code_search_path = sys.path if load_code_from_local else []
    """
    add "_memory=1024*1024*1024" for local test if resource is not enough
    e.g.
    ray.init(
            job_config=ray.job_config.JobConfig(
                num_java_workers_per_process=1, code_search_path=[classpath]),
            resources={
                "MEM": 16,
                "RES-A": 4
            },
            _memory=16*1024*1024*1024)
    """
    ray.init(
        job_config=ray.job_config.JobConfig(
            num_java_workers_per_process=1, code_search_path=code_search_path),
        resources={
            "MEM": 16,
            "RES-A": 4
        },
        num_cpus=16)


def test_parse_job_config():
    args = [
        "-rayagCallback",
        '\'{"jobName":"antc4mobius1169001524",'  # noqa
        '"stateConfig":{"stateTable":"antc4mobius1169001524_state",'  # noqa
        '"quorum":'  # noqa
        '"apayhbs-zk0116.em14.alipay.com,apayhbs-zk0117.em14.alipay.com,'  # noqa
        'apayhbs-zk0118.em14.alipay.com,apayhbs-zk0119.em14.alipay.com,'  # noqa
        'apayhbs-zk0120.em14.alipay.com",'  # noqa
        '"zkNode":"/hbs-em14-mix-gzone-01",'  # noqa
        '"txTable":"antc4mobius1169001524_tx"},"jobId":"1242001600",'  # noqa
        '"antcUrl":"https://antc.alipay.com","jobUid":"1169001524",'  # noqa
        '"metaConfig":{"quorum":'  # noqa
        '"apayhbs-zk0116.em14.alipay.com,apayhbs-zk0117.em14.alipay.com,'  # noqa
        'apayhbs-zk0118.em14.alipay.com,apayhbs-zk0119.em14.alipay.com,'  # noqa
        'apayhbs-zk0120.em14.alipay.com",'  # noqa
        '"metaTable":"antc4mobius1169001524_meta",'  # noqa
        '"zkNode":"/hbs-em14-mix-gzone-01"},"runtimeConfig":{'  # noqa
        '"panguRootDir":"/arc/streaming/antc4mobius1169001524",'  # noqa
        '"panguClusterName":"pangu://pangu1_analyze_sata_em14_online"}}\'',  # noqa
        "-jobConf",
        '{"rayag.actor.reconstruction.times": "10", '  # noqa
        '"health_check_interval_sec": "1200", '  # noqa
        '"checkpoint_interval_secs": "180", '  # noqa
        '"state.backend.mem.cache.capacity": "10", '  # noqa
        '"checkpoint_timeout_secs": "300", '  # noqa
        '"streaming.queue.type": "streaming_queue"}',  # noqa
    ]
    conf = ray.streaming.utils.parse_job_config(args)
    print(conf)
    assert "streaming.checkpoint.state.backend.type" in conf
    assert conf["streaming.checkpoint.state.backend.type"] == "PANGU"
    assert conf["state.backend.mem.cache.capacity"] == "10"
