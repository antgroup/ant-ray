import asyncio
import datetime
import json
import logging
import numpy as np
import os
import pathlib
import uuid
import socket
import subprocess
import sys
import shutil
import time
import traceback
from urllib.parse import urlparse

import async_timeout
import msgpack

import ray
import ray.gcs_utils
import ray.new_dashboard.modules.reporter.reporter_consts as reporter_consts
from ray.new_dashboard import k8s_utils
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.utils import (create_task, async_loop_forever)
import ray._private.services
import ray._private.utils
from ray.core.generated import agent_manager_pb2
from ray.core.generated import common_pb2
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray._private.metrics_agent import MetricsAgent, Gauge, Record
from ray.ray_constants import (
    runtime_resource_scheduling_enabled,
    runtime_resources_calculation_interval_s, runtime_memory_tail_percentile,
    runtime_cpu_tail_percentile, memory_monitor_refresh_ms,
    DEBUG_AUTOSCALING_STATUS)
import psutil

logger = logging.getLogger(__name__)

# Are we in a K8s pod?
IN_KUBERNETES_POD = "KUBERNETES_SERVICE_HOST" in os.environ
# Flag to enable showing disk usage when running in a K8s pod,
# disk usage defined as the result of running psutil.disk_usage("/")
# in the Ray container.
ENABLE_K8S_DISK_USAGE = os.environ.get(
    "RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE") == "1"

try:
    import gpustat.core as gpustat
except ImportError:
    gpustat = None
    logger.warning(
        "Install gpustat with 'pip install gpustat' to enable GPU monitoring.")


def recursive_asdict(o):
    if isinstance(o, tuple) and hasattr(o, "_asdict"):
        return recursive_asdict(o._asdict())

    if isinstance(o, (tuple, list)):
        L = []
        for k in o:
            L.append(recursive_asdict(k))
        return L

    if isinstance(o, dict):
        D = {k: recursive_asdict(v) for k, v in o.items()}
        return D

    return o


def serialize_asdict(o):
    return msgpack.dumps(dashboard_utils.to_google_style(recursive_asdict(o)))


# A list of gauges to record and export metrics.
METRICS_GAUGES = {
    "node_cpu_utilization": Gauge("node_cpu_utilization",
                                  "Total CPU usage on a ray node",
                                  "percentage", ["ip"]),
    "node_cpu_count": Gauge("node_cpu_count",
                            "Total CPUs available on a ray node", "cores",
                            ["ip"]),
    "node_mem_used": Gauge("node_mem_used", "Memory usage on a ray node",
                           "bytes", ["ip"]),
    "node_mem_available": Gauge("node_mem_available",
                                "Memory available on a ray node", "bytes",
                                ["ip"]),
    "node_mem_total": Gauge("node_mem_total", "Total memory on a ray node",
                            "bytes", ["ip"]),
    "node_gpus_available": Gauge("node_gpus_available",
                                 "Total GPUs available on a ray node",
                                 "percentage", ["ip"]),
    "node_gpus_utilization": Gauge("node_gpus_utilization",
                                   "Total GPUs usage on a ray node",
                                   "percentage", ["ip"]),
    "node_gram_used": Gauge("node_gram_used",
                            "Total GPU RAM usage on a ray node", "bytes",
                            ["ip"]),
    "node_gram_available": Gauge("node_gram_available",
                                 "Total GPU RAM available on a ray node",
                                 "bytes", ["ip"]),
    "node_disk_usage": Gauge("node_disk_usage",
                             "Total disk usage (bytes) on a ray node", "bytes",
                             ["ip"]),
    "node_disk_free": Gauge("node_disk_free",
                            "Total disk free (bytes) on a ray node", "bytes",
                            ["ip"]),
    "node_disk_utilization_percentage": Gauge(
        "node_disk_utilization_percentage",
        "Total disk utilization (percentage) on a ray node", "percentage",
        ["ip"]),
    "node_network_sent": Gauge("node_network_sent", "Total network sent",
                               "bytes", ["ip"]),
    "node_network_received": Gauge("node_network_received",
                                   "Total network received", "bytes", ["ip"]),
    "node_network_send_speed": Gauge(
        "node_network_send_speed", "Network send speed", "bytes/sec", ["ip"]),
    "node_network_receive_speed": Gauge("node_network_receive_speed",
                                        "Network receive speed", "bytes/sec",
                                        ["ip"]),
    "raylet_cpu": Gauge("raylet_cpu", "CPU usage of the raylet on a node.",
                        "percentage", ["ip", "pid"]),
    "raylet_mem": Gauge("raylet_mem", "Memory usage of the raylet on a node",
                        "mb", ["ip", "pid"]),
    "cluster_active_nodes": Gauge("cluster_active_nodes",
                                  "Active nodes on the cluster", "count",
                                  ["node_type"]),
    "cluster_failed_nodes": Gauge("cluster_failed_nodes",
                                  "Failed nodes on the cluster", "count",
                                  ["node_type"]),
    "cluster_pending_nodes": Gauge("cluster_pending_nodes",
                                   "Pending nodes on the cluster", "count",
                                   ["node_type"]),
}


class WorkerRuntimeInfo:
    def __init__(self, rss, cpu_percent):
        self.rss = rss
        self.cpu_percent = cpu_percent


class ReporterAgent(dashboard_utils.DashboardAgentModule,
                    reporter_pb2_grpc.ReporterServiceServicer):
    """A monitor process for monitoring Ray nodes.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config
    """

    def __init__(self, dashboard_agent):
        """Initialize the reporter object."""
        super().__init__(dashboard_agent)
        if IN_KUBERNETES_POD:
            # psutil does not compute this correctly when in a K8s pod.
            # Use ray._private.utils instead.
            cpu_count = ray._private.utils.get_num_cpus()
            self._cpu_counts = (cpu_count, cpu_count)
        else:
            self._cpu_counts = (psutil.cpu_count(),
                                psutil.cpu_count(logical=False))

        self._ip = ray.util.get_node_ip_address()
        self._redis_address, _ = dashboard_agent.redis_address
        self._is_head_node = (self._ip == self._redis_address)
        self._hostname = socket.gethostname()
        self._workers = set()
        self._network_stats_hist = [(0, (0.0, 0.0))]  # time, (sent, recv)
        self._report_stats = None
        self._init_metrics()
        if not dashboard_agent.metrics_export_port:
            logger.info("Not start MetricsAgent.")
            self._metrics_agent = None
        else:
            self._metrics_agent = MetricsAgent(
                dashboard_agent.metrics_export_port)
        self._key = f"{reporter_consts.REPORTER_PREFIX}" \
                    f"{self._dashboard_agent.node_id}"

        # The dict from worker pid to runtime history window.
        self._worker_runtime_windows = {}
        # The dict from worker pid to whether enough samples have
        # been collected.
        self._enough_samples_collected = {}
        # The list containing each worker's pid and runtime stats
        # (e.g., memory/cpu tail)
        self._worker_runtime_stats = []
        # A copy of `self._worker_runtime_stats`, only used for
        # reporting to local raylet.
        self._local_worker_runtime_stats = []
        self._runtime_resource_scheduling_enabled = \
            runtime_resource_scheduling_enabled()
        self._runtime_resources_calculation_interval_s = \
            runtime_resources_calculation_interval_s()
        self._runtime_memory_tail_percentile = \
            runtime_memory_tail_percentile()
        self._runtime_cpu_tail_percentile = \
            runtime_cpu_tail_percentile()

    def _init_metrics(self):
        """Declarations of metrics."""
        # {pid(str): (pid, language(str), job_name(str), job_id(str))}
        self._worker_info = {}
        # agent process cpu and memory metrics
        self._agent_proc = psutil.Process()
        self._agent_cpu_percent = ray.util.metrics.Histogram(
            reporter_consts.AGENT_CPU_PERCENT, boundaries=[50.0, 90.0])
        self._agent_mem_rss = ray.util.metrics.Histogram(
            reporter_consts.AGENT_MEM_RSS,
            boundaries=[1024.0**3, 5 * 1024.0**3])
        self._agent_mem_util = ray.util.metrics.Histogram(
            reporter_consts.AGENT_MEM_UTIL, boundaries=[50.0, 90.0])

        # raylet process cpu and memory metrics
        self._raylet_proc = None
        raylet_tag_keys = (
            "HostName",
            "NodeAddress",
            "WorkerPid",
        )
        self._raylet_tags = {
            "HostName": self._hostname,
            "NodeAddress": self._ip
        }
        self._raylet_cpu_percent = ray.util.metrics.Histogram(
            reporter_consts.RAYLET_CPU_PERCENT,
            boundaries=[50.0, 90.0],
            tag_keys=raylet_tag_keys)
        self._raylet_mem_rss = ray.util.metrics.Histogram(
            reporter_consts.RAYLET_MEM_RSS,
            boundaries=[1024.0**3, 5 * 1024.0**3],
            tag_keys=raylet_tag_keys)
        self._raylet_mem_util = ray.util.metrics.Histogram(
            reporter_consts.RAYLET_MEM_UTIL,
            boundaries=[50.0, 90.0],
            tag_keys=raylet_tag_keys)

        # core workers metrics
        worker_tag_keys = (
            "JobName",
            "JobId",
            "NodeAddress",
            "HostName",
            "WorkerPid",
            "Language",
        )
        self._worker_cpu_percent = ray.util.metrics.Gauge(
            reporter_consts.WORKER_CPU_PERCENT, tag_keys=worker_tag_keys)
        self._worker_mem_rss = ray.util.metrics.Gauge(
            reporter_consts.WORKER_MEM_RSS, tag_keys=worker_tag_keys)
        self._worker_mem_shared = ray.util.metrics.Gauge(
            reporter_consts.WORKER_MEM_SHARED, tag_keys=worker_tag_keys)
        self._worker_number = ray.util.metrics.Gauge(
            reporter_consts.WORKER_PROCESS_NUMBER)
        self._worker_gpu_memory_usage = ray.util.metrics.Gauge(
            reporter_consts.WORKER_GPU_MEMORY_USAGE_MB,
            tag_keys=worker_tag_keys)

        # gpu metrics
        gpu_tag_keys = (
            "HostName",
            "NodeAddress",
        )
        self._node_gpu_percent = ray.util.metrics.Histogram(
            reporter_consts.NODE_GPU_PERCENT,
            boundaries=[50.0, 90.0],
            tag_keys=gpu_tag_keys)
        self._node_gram_used = ray.util.metrics.Histogram(
            reporter_consts.NODE_GRAM_USED,
            boundaries=[1024.0**3, 5 * 1024.0**3],
            tag_keys=gpu_tag_keys)
        self._node_gram_total = ray.util.metrics.Histogram(
            reporter_consts.NODE_GRAM_TOTAL,
            boundaries=[1024.0**3, 5 * 1024.0**3],
            tag_keys=gpu_tag_keys)

    async def GetProfilingStats(self, request, context):
        pid = request.pid
        duration = request.duration
        profiling_file_path = os.path.join(
            ray._private.utils.get_ray_temp_dir(), f"{pid}_profiling.txt")
        sudo = "sudo" if ray._private.utils.get_user() != "root" else ""
        process = await asyncio.create_subprocess_shell(
            f"{sudo} $(which py-spy) record "
            f"-o {profiling_file_path} -p {pid} -d {duration} -f speedscope",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            profiling_stats = ""
        else:
            with open(profiling_file_path, "r") as f:
                profiling_stats = f.read()
        return reporter_pb2.GetProfilingStatsReply(
            profiling_stats=profiling_stats, std_out=stdout, std_err=stderr)

    async def ReportOCMetrics(self, request, context):
        # This function receives a GRPC containing OpenCensus (OC) metrics
        # from a Ray process, then exposes those metrics to Prometheus.
        try:
            self._metrics_agent.record_metric_points_from_protobuf(
                request.metrics)
        except Exception:
            logger.error(traceback.format_exc())
        return reporter_pb2.ReportOCMetricsReply()

    async def _prepare_async_profiler(self):
        temp_dir = self._dashboard_agent.temp_dir
        log_dir = self._dashboard_agent.log_dir
        http_session = self._dashboard_agent.http_session

        async_profiler_dir = reporter_consts.ASYNC_PROFILER_DIR.format(
            temp_dir=temp_dir)
        shutil.rmtree(async_profiler_dir, ignore_errors=True)
        os.makedirs(async_profiler_dir, exist_ok=True)
        async_profiler_bin_dir = reporter_consts.ASYNC_PROFILER_BIN_DIR.format(
            temp_dir=temp_dir)
        os.makedirs(async_profiler_bin_dir, exist_ok=True)
        async_profiler_out_dir = reporter_consts.PROFILER_OUT_DIR.format(
            log_dir=log_dir)
        os.makedirs(async_profiler_out_dir, exist_ok=True)

        async_profiler_url = reporter_consts.ASYNC_PROFILER_URL.get(
            sys.platform)
        if async_profiler_url is None:
            raise Exception(
                f"async-profiler is not supported on platform {sys.platform}")
        url_path = urlparse(async_profiler_url).path
        basename_from_url = os.path.basename(url_path)
        filename = os.path.join(async_profiler_dir, basename_from_url)
        async with http_session.get(async_profiler_url, ssl=False) as response:
            with open(filename, "wb") as f:
                while True:
                    chunk = await response.content.read(10 * 1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)

        code = f"import shutil; " \
               f"shutil.unpack_archive(" \
               f"{repr(filename)}, {repr(async_profiler_bin_dir)})"
        cmd = f"{sys.executable} -c {repr(code)}"
        logger.info("[async-profiler] unzip async-profiler %s", repr(cmd))
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            if stderr:
                stderr = stderr.decode("utf-8")
                logger.error(
                    "[async-profiler] Output of unzip async-profiler: %s",
                    stderr)
                raise Exception(
                    f"Run command {repr(cmd)} exit with {proc.returncode}:\n"
                    f"{stderr}")
            raise Exception(
                f"Run command {repr(cmd)} exit with {proc.returncode}.")
        async_profiler = reporter_consts.ASYNC_PROFILER.format(
            temp_dir=temp_dir)
        assert os.path.exists(async_profiler)

    async def RunCmd(self, request, context):
        if request.cmd_name not in reporter_consts.RUN_CMD:
            return reporter_pb2.RunCmdReply(
                output=f"Unable to run command {request.cmd_name}, "
                f"available cmd: {reporter_consts.RUN_CMD}")
        systems = reporter_consts.RUN_CMD[request.cmd_name]["systems"]
        if sys.platform not in systems and "all" not in systems:
            return reporter_pb2.RunCmdReply(
                output=f"Unable to run command {request.cmd_name} on "
                f"{sys.platform}, support systems: {systems}.")
        try:
            kwargs = json.loads(request.cmd_kwargs)
            fixed_kwargs = {
                "ip": self._dashboard_agent.ip,
                "port": self._dashboard_agent.http_port,
                "temp_dir": self._dashboard_agent.temp_dir,
                "log_dir": self._dashboard_agent.log_dir,
                "uuid": str(uuid.uuid4()),
                "sudo": "sudo"
                if ray._private.utils.get_user() != "root" else "",
                "timestamp": datetime.datetime.now().replace(
                    second=0, microsecond=0).isoformat(),
            }
            cmd_converter = reporter_consts.RUN_CMD[request.cmd_name][
                "converter"]
            missing_converter = (
                kwargs.keys() - fixed_kwargs.keys() - cmd_converter.keys())
            if missing_converter:
                raise Exception(f"Invalid queries: {missing_converter}")
            # Make sure these kwargs overwrite the user inputs
            # to avoid security issue.
            kwargs.update(fixed_kwargs)
            cmd_formatter = reporter_consts.RUN_CMD[request.cmd_name][
                "formatter"]
            cmdline = cmd_formatter.format(**kwargs)
            logger.info("Run cmd %s with timeout %s seconds.", repr(cmdline),
                        request.timeout)

            async def _run(cmd, timeout, check=True):
                proc = await asyncio.create_subprocess_shell(
                    cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE)
                try:
                    async with async_timeout.timeout(timeout):
                        stdout, stderr = await proc.communicate()
                except asyncio.TimeoutError:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    raise dashboard_utils.SubprocessTimeoutExpired(
                        cmd, timeout, output=stdout, stderr=stderr)
                except:  # noqa: E722
                    proc.kill()
                    await proc.wait()
                    raise
                if check and proc.returncode:
                    raise dashboard_utils.SubprocessCalledProcessError(
                        proc.returncode, cmd, output=stdout, stderr=stderr)
                return subprocess.CompletedProcess(cmd, proc.returncode,
                                                   stdout, stderr)

            output = reporter_consts.RUN_CMD[request.cmd_name]["output"]
            if output is not None:
                output_url = output["url"].format(**kwargs)
                output_file = output["file"].format(**kwargs)
                pathlib.Path(output_file).touch(exist_ok=True)

                async def _async_wait_proc():
                    try:
                        await _run(cmdline, request.timeout)
                    except Exception as e:
                        logger.exception("Run cmd failed.")
                        with open(output_file, "w") as f:
                            f.write(str(e))

                create_task(_async_wait_proc())
                return reporter_pb2.RunCmdReply(output=output_url)
            else:
                r = await _run(cmdline, request.timeout)
                logger.info("Run cmd success, %s", r)
                return reporter_pb2.RunCmdReply(output="\n".join(
                    [r.stdout.decode("utf-8"),
                     r.stderr.decode("utf-8")]))
        except Exception:
            logger.exception("Run cmd failed.")
            return reporter_pb2.RunCmdReply(output=traceback.format_exc())

    @staticmethod
    def _get_cpu_percent():
        if IN_KUBERNETES_POD:
            return k8s_utils.cpu_percent()
        else:
            return psutil.cpu_percent()

    @staticmethod
    def _get_cpu_times_percent():
        return dict(psutil.cpu_times_percent()._asdict())

    @staticmethod
    def _get_gpu_usage():
        if gpustat is None:
            return []
        gpu_utilizations = []
        gpus = []
        try:
            gpus = gpustat.new_query().gpus
        except Exception as e:
            logger.debug("gpustat failed to retrieve GPU information: %s", e)
        for gpu in gpus:
            # Note the keys in this dict have periods which throws
            # off javascript so we change .s to _s
            gpu_data = {
                "_".join(key.split(".")): val
                for key, val in gpu.entry.items()
            }
            gpu_utilizations.append(gpu_data)
        return gpu_utilizations

    @staticmethod
    def _get_boot_time():
        if IN_KUBERNETES_POD:
            # Return start time of container entrypoint
            return psutil.Process(pid=1).create_time()
        else:
            return psutil.boot_time()

    @staticmethod
    def _get_network_stats():
        ifaces = [
            v for k, v in psutil.net_io_counters(pernic=True).items()
            if k[0] == "e"
        ]

        sent = sum((iface.bytes_sent for iface in ifaces))
        recv = sum((iface.bytes_recv for iface in ifaces))
        return sent, recv

    @staticmethod
    def _get_mem_usage():
        vm = psutil.virtual_memory()
        return vm._asdict()

    @staticmethod
    def _get_disk_usage():
        dirs = [
            os.environ["USERPROFILE"] if sys.platform == "win32" else os.sep,
            ray._private.utils.get_user_temp_dir(),
        ]
        if IN_KUBERNETES_POD and not ENABLE_K8S_DISK_USAGE:
            # If in a K8s pod, disable disk display by passing in dummy values.
            return {
                x: psutil._common.sdiskusage(
                    total=1, used=0, free=1, percent=0.0)
                for x in dirs
            }
        else:
            return {x: psutil.disk_usage(x) for x in dirs}

    def _get_workers(self):
        raylet_proc = self._get_raylet_proc()
        if raylet_proc is None:
            return []
        else:
            timestamp = time.time()
            worker_infos = []
            for w in self._workers:
                if (psutil.pid_exists(w.pid)
                        and w.status() != psutil.STATUS_ZOMBIE
                        and w.status() != psutil.STATUS_DEAD):
                    d = w.as_dict(attrs=[
                        "pid",
                        "create_time",
                        "cpu_percent",
                        "cpu_times",
                        "cmdline",
                        "memory_info",
                    ])
                    if hasattr(w, "cpu_num"):
                        d["cpu_num"] = w.cpu_num()
                    d["timestamp"] = timestamp
                    worker_infos.append(d)
            return worker_infos

    @staticmethod
    def _get_raylet_proc():
        try:
            curr_proc = psutil.Process()
            # Here, parent is always raylet because the
            # dashboard agent is a child of the raylet process.
            parent = curr_proc.parent()
            if parent is not None:
                if parent.pid == 1:
                    return None
                if parent.status() == psutil.STATUS_ZOMBIE:
                    return None
            return parent
        except (psutil.AccessDenied, ProcessLookupError):
            pass
        return None

    def _get_raylet(self):
        raylet_proc = self._get_raylet_proc()
        if raylet_proc is None:
            return {}
        else:
            return raylet_proc.as_dict(attrs=[
                "pid",
                "create_time",
                "cpu_percent",
                "cpu_times",
                "cmdline",
                "memory_info",
            ])

    def _get_load_avg(self):
        if sys.platform == "win32":
            cpu_percent = psutil.cpu_percent()
            load = (cpu_percent, cpu_percent, cpu_percent)
        else:
            load = os.getloadavg()
        per_cpu_load = tuple((round(x / self._cpu_counts[0], 2) for x in load))
        return load, per_cpu_load

    def _get_all_stats(self):
        now = dashboard_utils.to_posix_time(datetime.datetime.utcnow())
        network_stats = self._get_network_stats()

        self._network_stats_hist.append((now, network_stats))
        self._network_stats_hist = self._network_stats_hist[-7:]
        then, prev_network_stats = self._network_stats_hist[0]
        prev_send, prev_recv = prev_network_stats
        now_send, now_recv = network_stats
        network_speed_stats = ((now_send - prev_send) / (now - then),
                               (now_recv - prev_recv) / (now - then))
        return {
            "now": now,
            "hostname": self._hostname,
            "ip": self._ip,
            "cpu_percent": self._get_cpu_percent(),
            "cpu_times_percent": self._get_cpu_times_percent(),
            "cpu_count": self._cpu_counts,
            "mem": self._get_mem_usage(),
            "workers": self._get_workers(),
            "raylet": self._get_raylet(),
            "bootTime": self._get_boot_time(),
            "loadAvg": self._get_load_avg(),
            "disk": self._get_disk_usage(),
            "gpus": self._get_gpu_usage(),
            "network": network_stats,
            "network_speed": network_speed_stats,
            # Deprecated field, should be removed with frontend.
            "cmdline": self._get_raylet().get("cmdline", []),
            "timestamp": time.time(),
        }

    def _record_stats(self, stats, cluster_stats):
        records_reported = []
        ip = stats["ip"]

        # -- Instance count of cluster --
        # Only report cluster stats on head node
        if "autoscaler_report" in cluster_stats and self._is_head_node:
            active_nodes = cluster_stats["autoscaler_report"]["active_nodes"]
            for node_type, active_node_count in active_nodes.items():
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["cluster_active_nodes"],
                        value=active_node_count,
                        tags={"node_type": node_type}))

            failed_nodes = cluster_stats["autoscaler_report"]["failed_nodes"]
            failed_nodes_dict = {}
            for _node_ip, node_type in failed_nodes:
                if node_type in failed_nodes_dict:
                    failed_nodes_dict[node_type] += 1
                else:
                    failed_nodes_dict[node_type] = 1

            for node_type, failed_node_count in failed_nodes_dict.items():
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["cluster_failed_nodes"],
                        value=failed_node_count,
                        tags={"node_type": node_type}))

            pending_nodes = cluster_stats["autoscaler_report"]["pending_nodes"]
            pending_nodes_dict = {}
            for _node_ip, node_type, _status_message in pending_nodes:
                if node_type in pending_nodes_dict:
                    pending_nodes_dict[node_type] += 1
                else:
                    pending_nodes_dict[node_type] = 1

            for node_type, pending_node_count in pending_nodes_dict.items():
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["cluster_pending_nodes"],
                        value=pending_node_count,
                        tags={"node_type": node_type}))

        # -- CPU per node --
        cpu_usage = float(stats["cpu_percent"])
        cpu_record = Record(
            gauge=METRICS_GAUGES["node_cpu_utilization"],
            value=cpu_usage,
            tags={"ip": ip})

        cpu_count, _ = stats["cpu_count"]
        cpu_count_record = Record(
            gauge=METRICS_GAUGES["node_cpu_count"],
            value=cpu_count,
            tags={"ip": ip})

        # -- Mem per node --
        mem_total, mem_available, mem_used = stats["mem"]["total"], stats[
            "mem"]["available"], stats["mem"]["used"]
        mem_used_record = Record(
            gauge=METRICS_GAUGES["node_mem_used"],
            value=mem_used,
            tags={"ip": ip})
        mem_available_record = Record(
            gauge=METRICS_GAUGES["node_mem_available"],
            value=mem_available,
            tags={"ip": ip})
        mem_total_record = Record(
            gauge=METRICS_GAUGES["node_mem_total"],
            value=mem_total,
            tags={"ip": ip})

        # -- GPU per node --
        gpus = stats["gpus"]
        gpus_available = len(gpus)

        if gpus_available:
            gpus_utilization, gram_used, gram_total = 0, 0, 0
            for gpu in gpus:
                gpus_utilization += gpu["utilization_gpu"]
                gram_used += gpu["memory_used"]
                gram_total += gpu["memory_total"]

            gram_available = gram_total - gram_used

            gpus_available_record = Record(
                gauge=METRICS_GAUGES["node_gpus_available"],
                value=gpus_available,
                tags={"ip": ip})
            gpus_utilization_record = Record(
                gauge=METRICS_GAUGES["node_gpus_utilization"],
                value=gpus_utilization,
                tags={"ip": ip})
            gram_used_record = Record(
                gauge=METRICS_GAUGES["node_gram_used"],
                value=gram_used,
                tags={"ip": ip})
            gram_available_record = Record(
                gauge=METRICS_GAUGES["node_gram_available"],
                value=gram_available,
                tags={"ip": ip})
            records_reported.extend([
                gpus_available_record, gpus_utilization_record,
                gram_used_record, gram_available_record
            ])

        # -- Disk per node --
        used, free = 0, 0
        for entry in stats["disk"].values():
            used += entry.used
            free += entry.free
        disk_utilization = float(used / (used + free)) * 100
        disk_usage_record = Record(
            gauge=METRICS_GAUGES["node_disk_usage"],
            value=used,
            tags={"ip": ip})
        disk_free_record = Record(
            gauge=METRICS_GAUGES["node_disk_free"],
            value=free,
            tags={"ip": ip})
        disk_utilization_percentage_record = Record(
            gauge=METRICS_GAUGES["node_disk_utilization_percentage"],
            value=disk_utilization,
            tags={"ip": ip})

        # -- Network speed (send/receive) stats per node --
        network_stats = stats["network"]
        network_sent_record = Record(
            gauge=METRICS_GAUGES["node_network_sent"],
            value=network_stats[0],
            tags={"ip": ip})
        network_received_record = Record(
            gauge=METRICS_GAUGES["node_network_received"],
            value=network_stats[1],
            tags={"ip": ip})

        # -- Network speed (send/receive) per node --
        network_speed_stats = stats["network_speed"]
        network_send_speed_record = Record(
            gauge=METRICS_GAUGES["node_network_send_speed"],
            value=network_speed_stats[0],
            tags={"ip": ip})
        network_receive_speed_record = Record(
            gauge=METRICS_GAUGES["node_network_receive_speed"],
            value=network_speed_stats[1],
            tags={"ip": ip})

        raylet_stats = stats["raylet"]
        if raylet_stats:
            raylet_pid = str(raylet_stats["pid"])
            # -- raylet CPU --
            raylet_cpu_usage = float(raylet_stats["cpu_percent"]) * 100
            raylet_cpu_record = Record(
                gauge=METRICS_GAUGES["raylet_cpu"],
                value=raylet_cpu_usage,
                tags={
                    "ip": ip,
                    "pid": raylet_pid
                })

            # -- raylet mem --
            raylet_mem_usage = float(raylet_stats["memory_info"].rss) / 1e6
            raylet_mem_record = Record(
                gauge=METRICS_GAUGES["raylet_mem"],
                value=raylet_mem_usage,
                tags={
                    "ip": ip,
                    "pid": raylet_pid
                })
            records_reported.extend([raylet_cpu_record, raylet_mem_record])

        records_reported.extend([
            cpu_record, cpu_count_record, mem_used_record,
            mem_available_record, mem_total_record, disk_usage_record,
            disk_free_record, disk_utilization_percentage_record,
            network_sent_record, network_received_record,
            network_send_speed_record, network_receive_speed_record
        ])
        return records_reported

    def _record_agent_raylet_metrics(self):
        self._agent_cpu_percent.observe(self._agent_proc.cpu_percent())
        curr_memory_info = self._agent_proc.memory_info()
        if hasattr(curr_memory_info, "rss"):
            self._agent_mem_rss.observe(curr_memory_info.rss)
            self._agent_mem_util.observe(self._agent_proc.memory_percent())
        curr_proc = psutil.Process()
        parent = curr_proc.parent()
        if parent is not None and parent.pid != 1:
            if self._raylet_proc != parent:
                self._raylet_proc = parent
            self._raylet_tags["WorkerPid"] = str(self._raylet_proc.pid)
            memory_info = self._raylet_proc.memory_info()
            self._raylet_cpu_percent.observe(self._raylet_proc.cpu_percent(),
                                             self._raylet_tags)
            if hasattr(memory_info, "rss"):
                self._raylet_mem_rss.observe(memory_info.rss,
                                             self._raylet_tags)
                self._raylet_mem_util.observe(
                    self._raylet_proc.memory_percent(), self._raylet_tags)

    def _get_worker_info_by_stat(self, worker_stat):
        pid = str(worker_stat.get("pid", ""))
        item = self._worker_info.get(pid, ())
        if item:
            return item
        cmdline = worker_stat.get("cmdline", [])
        language = "JAVA" if cmdline and cmdline[0] == "java" else "PYTHON"
        job_name, job_id = "", ""
        return (pid, language, job_name, job_id)

    def _record_worker_metrics(self, stats):
        workers_stats = stats.get("workers", [])
        gpu_stats = {
            str(gpu.get("pid")): gpu.get("gpu_memory_usage", 0)
            for gpu in stats.get("gpus", [])
        }
        self._worker_number.set(len(workers_stats))
        for worker_stat in workers_stats:
            pid, language, job_name, job_id = \
                self._get_worker_info_by_stat(worker_stat)
            tags = {
                "JobName": job_name,
                "JobId": job_id,
                "NodeAddress": self._ip,
                "HostName": self._hostname,
                "WorkerPid": pid,
                "Language": language,
            }
            self._worker_cpu_percent.set(
                worker_stat.get("cpu_percent", 0), tags)
            pmem = worker_stat.get("memory_info")
            rss = pmem.rss if pmem else 0
            self._worker_mem_rss.set(rss, tags)
            shared = pmem.shared if pmem else 0
            self._worker_mem_shared.set(shared, tags)
            self._worker_gpu_memory_usage.set(gpu_stats.get(pid, 0), tags)

    def _record_gpu_metrics(self, stats):
        gpus = stats.get("gpus", [])
        gpus_available = len(gpus)

        if gpus_available:
            gpus_utilization, gram_used = 0, 0
            gram_total = 0
            for gpu in gpus:
                gpus_utilization += gpu["utilization_gpu"]
                gram_used += gpu["memory_used"]
                gram_total += gpu["memory_total"]

            tags = {
                "HostName": self._hostname,
                "NodeAddress": self._ip,
            }
            self._node_gpu_percent.observe(gpus_utilization, tags)
            self._node_gram_used.observe(gram_used, tags)
            self._node_gram_total.observe(gram_total, tags)

    def _record_metrics(self, stats):
        self._record_agent_raylet_metrics()
        self._record_worker_metrics(stats)
        self._record_gpu_metrics(stats)

    @async_loop_forever(reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)
    async def _perform_iteration(self, aioredis_client):
        """Get any changes to the log files and push updates to Redis."""
        try:
            formatted_status_string = await aioredis_client.hget(
                DEBUG_AUTOSCALING_STATUS, "value")
            formatted_status = json.loads(formatted_status_string.decode()
                                          ) if formatted_status_string else {}

            self._report_stats = stats = self._get_all_stats()
            if self._metrics_agent is not None:
                records_reported = self._record_stats(stats, formatted_status)
                self._metrics_agent.record_reporter_stats(records_reported)
            self._record_metrics(stats)

            for worker in stats.get("workers"):
                worker_runtime_info = WorkerRuntimeInfo(
                    worker.get("memory_info").rss, worker.get("cpu_percent"))
                pid = worker.get("pid")
                if pid not in self._worker_runtime_windows:
                    self._worker_runtime_windows[pid] = []
                    if pid not in self._enough_samples_collected:
                        self._enough_samples_collected[pid] = False
                self._worker_runtime_windows[pid].append(worker_runtime_info)

        except Exception:
            logger.exception("Error publishing node physical stats.")

    async def GetReportData(self, request, context):
        try:
            stats = self._report_stats
            self._report_stats = None
            if len(self._worker_runtime_stats) > 0:
                stats["worker_runtime"] = self._worker_runtime_stats
                self._worker_runtime_stats = []
            return reporter_pb2.GetReportDataReply(
                success=True,
                data=serialize_asdict({
                    self._dashboard_agent.node_id: stats
                }) if stats is not None else b"")
        except Exception as e:
            exc_info = (type(e), e, e.__traceback__)
            return reporter_pb2.GetReportDataReply(
                success=False,
                error="".join(traceback.format_exception(*exc_info)))

    @async_loop_forever(reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)
    async def _report_local_runtime_resources(self):
        if len(self._local_worker_runtime_stats) > 0 and \
                memory_monitor_refresh_ms() > 0:
            worker_stat_list = []
            for stat in self._local_worker_runtime_stats:
                worker_stat = common_pb2.WorkerRuntimeStat(
                    pid=stat.get("pid"),
                    memory_tail=int(stat.get("memory_tail")),
                    cpu_tail=stat.get("cpu_tail"),
                )
                worker_stat_list.append(worker_stat)
            self._local_worker_runtime_stats = []
            request = agent_manager_pb2.ReportLocalRuntimeResourcesRequest(
                worker_stat_list=worker_stat_list)
            reply = (await self._dashboard_agent.raylet_stub.
                     ReportLocalRuntimeResources(request))
            if reply.status == 0:
                logger.info("Succeeded to report local runtime resources")
            else:
                logger.info("Failed to report local runtime resources")

    async def _runtime_calc_loop(self):
        def calc_runtime_stat(worker_runtime_windows, enough_samples_collected,
                              runtime_memory_tail_percentile,
                              runtime_cpu_tail_percentile):
            worker_runtime_stats = []
            for pid, window in worker_runtime_windows.items():
                if enough_samples_collected[pid] is False:
                    # The worker has not collected enough samples this time.
                    # Make it true so next time we know enough samples
                    # will be collected (covering at least an entire period).
                    enough_samples_collected[pid] = True
                    continue
                if len(window) == 0:
                    # The worker has died.
                    enough_samples_collected.pop(pid)
                    continue
                memory_list = [info.rss for info in window]
                memory_tail = np.percentile(
                    memory_list, runtime_memory_tail_percentile * 100)
                cpu_list = [info.cpu_percent for info in window]
                cpu_tail = np.percentile(cpu_list,
                                         runtime_cpu_tail_percentile * 100)
                worker_runtime_stats.append({
                    "pid": pid,
                    "memory_tail": memory_tail,
                    "cpu_tail": cpu_tail
                })
            return worker_runtime_stats

        import copy
        while True:
            try:
                worker_runtime_windows = self._worker_runtime_windows
                self._worker_runtime_windows = {}
                enough_samples_collected = self._enough_samples_collected
                self._enough_samples_collected = {}
                loop = asyncio.get_event_loop()
                worker_runtime_stats = \
                    await loop.run_in_executor(
                            None, calc_runtime_stat,
                            worker_runtime_windows,
                            enough_samples_collected,
                            self._runtime_memory_tail_percentile,
                            self._runtime_cpu_tail_percentile)
                self._worker_runtime_stats.extend(worker_runtime_stats)
                # Make a copy for reporting to local raylet.
                self._local_worker_runtime_stats = \
                    copy.deepcopy(self._worker_runtime_stats)
                self._enough_samples_collected.update(enough_samples_collected)
            except Exception:
                logger.exception("Error calculating runtime resources.")

            await asyncio.sleep(self._runtime_resources_calculation_interval_s)

    # For workers running inside separate containers, their parents
    # are probably not the Raylet. So we rely on this loop to get
    # latest local workers' info from the Raylet.
    @async_loop_forever(reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)
    async def _update_workers_loop(self):
        try:
            if self._get_raylet_proc() is not None:
                request = agent_manager_pb2.GetWorkersInfoRequest()
                reply = (
                    await
                    self._dashboard_agent.raylet_stub.GetWorkersInfo(request))

                worker_pid_set = set()
                for worker in reply.worker_info_list:
                    worker_pid_set.add(worker.pid)
                    pid = str(worker.pid)
                    # Add the newly-created workers.
                    if pid not in self._worker_info and psutil.pid_exists(
                            worker.pid):
                        item = (pid, worker.language, worker.job_name,
                                worker.job_id)
                        self._worker_info[pid] = item
                        self._workers.add(psutil.Process(worker.pid))
                        logger.debug(
                            "Add worker info with pid: %d, " +
                            "language: %s, job_name: %s, job_id: %s",
                            worker.pid, worker.language, worker.job_name,
                            worker.job_id)

                # Remove the non-exist workers.
                for worker in self._workers.copy():
                    if worker.pid not in worker_pid_set:
                        self._workers.remove(worker)
                        self._worker_info.pop(str(worker.pid))

                logger.debug("Local node now has %d workers",
                             len(self._workers))
        except Exception:
            logger.exception("Error updating workers.")

    async def UpdateRuntimeResourceScheduleOptions(self, request, context):
        logger.info(
            "Update runtime resource schedule options: \n" +
            "runtime_resource_scheduling_enabled: %d, \n" +
            "runtime_resources_calculation_interval_s: %d, \n" +
            "runtime_memory_tail_percentile: %f, \n" +
            "runtime_cpu_tail_percentile: %f",
            request.runtime_resource_scheduling_enabled,
            request.runtime_resources_calculation_interval_s,
            request.runtime_memory_tail_percentile,
            request.runtime_cpu_tail_percentile)
        self._runtime_resource_scheduling_enabled = \
            request.runtime_resource_scheduling_enabled
        self._runtime_resources_calculation_interval_s = \
            request.runtime_resources_calculation_interval_s
        self._runtime_memory_tail_percentile = \
            request.runtime_memory_tail_percentile
        self._runtime_cpu_tail_percentile = \
            request.runtime_cpu_tail_percentile
        return reporter_pb2.UpdateRuntimeResourceScheduleOptionsReply()

    async def run(self, server):
        reporter_pb2_grpc.add_ReporterServiceServicer_to_server(self, server)
        try:
            await self._prepare_async_profiler()
        except Exception:
            reporter_consts.RUN_CMD.pop("async-profiler")
            logger.exception("[async-profiler] prepare async profiler failed, "
                             "the GET /utils/async-profiler is not supported.")

        tasks = []
        tasks.append(
            self._perform_iteration(self._dashboard_agent.aioredis_client))
        tasks.append(self._runtime_calc_loop())
        tasks.append(self._update_workers_loop())
        tasks.append(self._report_local_runtime_resources())
        await asyncio.gather(*tasks)
