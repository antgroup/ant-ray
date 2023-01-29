import os
import functools
import ray.ray_constants as ray_constants


def _check_in(o, valid_values):
    assert o in valid_values, f"{repr(o)} should be one of {valid_values}"
    return o


REPORTER_PREFIX = "RAY_REPORTER:"
# The reporter will report its statistics this often (milliseconds).
REPORTER_UPDATE_INTERVAL_MS = ray_constants.env_integer(
    "REPORTER_UPDATE_INTERVAL_MS", 15 * 1000)
# async-profiler
ASYNC_PROFILER_URL = {
    "linux": "http://ray-streaming.oss-cn-hangzhou-zmf.aliyuncs.com/perf/async-profiler-1.6-linux-x64.tar.gz",  # noqa: E501
    "darwin": "http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/po/async-profiler-1.8.3-macos-x64.tar.gz",  # noqa: E501
}
ASYNC_PROFILER_DIR = "{temp_dir}/perf_tools/async_profiler"
ASYNC_PROFILER_BIN_DIR = "{temp_dir}/perf_tools/async_profiler/bin"
ASYNC_PROFILER = os.path.join(ASYNC_PROFILER_BIN_DIR, "profiler.sh")
PROFILER_OUT_DIR = "{log_dir}/perf"

_dmesg_converter = functools.partial(
    _check_in,
    valid_values=[
        "emerg", "alert", "crit", "err", "warn", "notice", "info", "debug"
    ])

RUN_CMD = {
    "jstack": {
        "formatter": "jstack -l {pid}",
        "converter": {
            "pid": int
        },
        "systems": {
            "all",
        },
        "output": None,
    },
    "jmap": {
        "formatter": "jmap -histo {pid}",
        "converter": {
            "pid": int
        },
        "systems": {
            "all",
        },
        "output": None,
    },
    "ps": {
        "formatter": "ps -ef",
        "converter": {},
        "systems": {
            "darwin",
            "linux",
        },
        "output": None,
    },
    "jstat": {
        "formatter": "jstat -{options} {pid}",
        "converter": {
            "options": functools.partial(
                _check_in,
                valid_values=[
                    "class", "compiler", "gc", "gccapacity", "gcmetacapacity",
                    "gcnew", "gcnewcapacity", "gcold", "gcoldcapacity",
                    "gcutil", "gccause", "printcompilation"
                ]),
            "pid": int
        },
        "systems": {
            "all",
        },
        "output": None,
    },
    "dmesgsys": {
        "formatter": "dmesg -T --level {level} | tail -n {lines}",
        "converter": {
            "level": _dmesg_converter,
            "lines": int,
        },
        "systems": {
            "linux",
        },
        "output": None,
    },
    "dmesgproc": {
        "formatter": "dmesg -T --level {level} | grep {pid} | tail -n {lines}",
        "converter": {
            "level": _dmesg_converter,
            "pid": int,
            "lines": int,
        },
        "systems": {
            "linux",
        },
        "output": None,
    },
    "pystack": {
        "formatter": "py-spy dump --pid {pid}",
        "converter": {
            "pid": int,
        },
        "systems": {
            "all",
        },
        "output": None,
    },
    "pstack": {
        "formatter": "pstack {pid}",
        "converter": {
            "pid": int,
        },
        "systems": {
            "linux",
        },
        "output": None,
    },
    "async-profiler": {
        "formatter": f"{ASYNC_PROFILER} --all-user -d {{duration}} -f {PROFILER_OUT_DIR}/{{pid}}-{{uuid}}.svg {{pid}}",  # noqa: E501
        "converter": {
            "duration": int,
            "pid": int,
        },
        "systems": {
            "all",
        },
        "output": {
            "file": f"{PROFILER_OUT_DIR}/{{pid}}-{{uuid}}.svg",
            "url": f"http://{{ip}}:{{port}}/logs/perf/{{pid}}-{{uuid}}.svg",  # noqa: F541, E501
        }
    },
    "pyprofiler": {
        "formatter": f"{{sudo}} py-spy record -o {PROFILER_OUT_DIR}/{{pid}}-{{uuid}}.svg -p {{pid}} -d {{duration}}",  # noqa: E501
        "converter": {
            "duration": int,
            "pid": int,
        },
        "systems": {
            "all",
        },
        "output": {
            "file": f"{PROFILER_OUT_DIR}/{{pid}}-{{uuid}}.svg",
            "url": f"http://{{ip}}:{{port}}/logs/perf/{{pid}}-{{uuid}}.svg",  # noqa: F541, E501
        }
    },
    "pyprofiler2": {
        # The SIGPROF signal, the handler is in worker_handler.cc
        "formatter": f"kill -27 {{pid}}",  # noqa: F541
        "converter": {
            "pid": int,
        },
        "systems": {
            "all",
        },
        "output": {
            "file": f"{PROFILER_OUT_DIR}/{{pid}}_{{timestamp}}.prof",
            "url": f"http://{{ip}}:{{port}}/logs/perf/{{pid}}_{{timestamp}}.prof",  # noqa: E501, F541
        }
    }
}
RUN_CMD_TIMEOUT_SECONDS = 120

# Environment variables
ENV_JOB_NAME = "ANTC_JOB_NAME"
ENV_JOB_ID = "RAY_JOB_ID"

# System Metrics: agent
AGENT_CPU_PERCENT = "ray.agent.cpu_percent"
AGENT_MEM_RSS = "ray.agent.mem_rss"
AGENT_MEM_UTIL = "ray.agent.mem_util"

# System Metrics: raylet
RAYLET_CPU_PERCENT = "ray.raylet.cpu_percent"
RAYLET_MEM_RSS = "ray.raylet.mem_rss"
RAYLET_MEM_UTIL = "ray.raylet.mem_util"

# System Metrics: core worker
WORKER_MEM_RSS = "ray.core_worker.mem.rss"
WORKER_MEM_SHARED = "ray.core_worker.mem.shared"
WORKER_CPU_PERCENT = "ray.core_worker.cpu.percent"
WORKER_PROCESS_NUMBER = "ray.core_worker.process_number"
WORKER_GPU_MEMORY_USAGE_MB = "ray.core_worker.gpu.memory.usage_mb"

# System Metrics: gpu
NODE_GPU_PERCENT = "node_gpu_percent"
NODE_GRAM_USED = "node_gram_used"
NODE_GRAM_TOTAL = "node_gram_total"
