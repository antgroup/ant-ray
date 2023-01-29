import asyncio
import logging
import psutil
import socket

import ray.ray_constants as ray_constants
import ray.new_dashboard.modules.metric.metric_consts as metric_consts
import ray.new_dashboard.utils as dashboard_utils

from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.datacenter import DataSource
from ray.util import metrics

logger = logging.getLogger(__name__)


class MetricHead(dashboard_utils.DashboardHeadModule):
    """A metric process for reporting metrics to external systems.
    """

    def __init__(self, dashboard_head):
        """Initialize the metrics head object."""
        super().__init__(dashboard_head)
        self._ip = dashboard_head.ip
        self._hostname = socket.gethostname()
        self._init_metrics()
        logger.info("Finished initializ metric head.")

    def _init_metrics(self):
        self._dashboard_proc = psutil.Process()
        self._dashboard_cpu_percent = metrics.Histogram(
            metric_consts.DASHBOARD_CPU_PERCENT, boundaries=[50.0, 90.0])
        self._dashboard_mem_rss = metrics.Histogram(
            metric_consts.DASHBOARD_MEM_RSS,
            boundaries=[1024.0**3, 5 * 1024.0**3])
        self._dashboard_mem_util = metrics.Histogram(
            metric_consts.DASHBOARD_MEM_UTIL, boundaries=[50.0, 90.0])
        self._gcs_proc = None
        gcs_tag_keys = (
            "HostName",
            "NodeAddress",
            "WorkerPid",
        )
        self._gcs_tags = {"HostName": self._hostname, "NodeAddress": self._ip}
        self._gcs_cpu_percent = metrics.Histogram(
            metric_consts.GCS_CPU_PERCENT,
            boundaries=[50.0, 90.0],
            tag_keys=gcs_tag_keys)
        self._gcs_mem_rss = metrics.Histogram(
            metric_consts.GCS_MEM_RSS,
            boundaries=[1024.0**3, 5 * 1024.0**3],
            tag_keys=gcs_tag_keys)
        self._gcs_mem_util = metrics.Histogram(
            metric_consts.GCS_MEM_UTIL,
            boundaries=[50.0, 90.0],
            tag_keys=gcs_tag_keys)
        tag_keys = (
            "JobName",
            "JobId",
            "NodeAddress",
            "HostName",
            "WorkerPid",
            "Language",
        )
        self._worker_acquired_mem = metrics.Gauge(
            metric_consts.WORKER_ACQUIRED_MEM, tag_keys=tag_keys)

    @async_loop_forever(metric_consts.METRIC_REPORT_INTERVAL_SECONDS)
    async def _report_worker_acquired_resources_metrics(self):
        # Notes: There are many matic strings in the following codes
        # because the values of acquired resources metrics are in the
        # DataSource.jobs, DataSource.job_resources and
        # DataSource.nodes
        # Details of the three data structures can be seen in
        # https://yuque.antfin-inc.com/ray-project/documentation/no9dvu#hBRzx
        jobs = DataSource.jobs
        job_resources = DataSource.job_resources
        nodes = DataSource.nodes
        logger.debug("There are %d jobs, %d job_resources, %d nodes",
                     len(jobs), len(job_resources), len(nodes))
        for job in jobs.values():
            job_id = job.get("jobId", "")
            job_name = job.get("name", "")
            resource = job_resources.get("resources", {}).get(job_id)
            if resource is None:
                continue
            for node in resource.get("nodes", []):
                node_id = node.get("nodeId", "")
                if nodes.get(node_id) is None:
                    continue
                for process in node.get("processResources", []):
                    tags = {
                        "JobName": job_name,
                        "JobId": job_id,
                        "NodeAddress": nodes.get(node_id, {}).get(
                            "nodeManagerAddress", ""),
                        "HostName": nodes.get(node_id, {}).get(
                            "nodeManagerHostname", ""),
                        "WorkerPid": str(process.get("pid", -1)),
                        "Language": process.get("language", "")
                    }
                    acquired_mem = process.get("acquiredResources", {}) \
                        .get("memory", 0) \
                        * ray_constants.MEMORY_RESOURCE_UNIT_BYTES
                    self._worker_acquired_mem.set(acquired_mem, tags)

    @async_loop_forever(metric_consts.METRIC_REPORT_INTERVAL_SECONDS)
    async def _record_dashboard_system_metrics(self):
        self._dashboard_cpu_percent.observe(self._dashboard_proc.cpu_percent())
        curr_memory_info = self._dashboard_proc.memory_info()
        if hasattr(curr_memory_info, "rss"):
            self._dashboard_mem_rss.observe(curr_memory_info.rss)
            self._dashboard_mem_util.observe(
                self._dashboard_proc.memory_percent())

    @async_loop_forever(metric_consts.METRIC_REPORT_INTERVAL_SECONDS)
    async def _record_gcs_system_metrics(self):
        curr_proc = psutil.Process()
        parent = curr_proc.parent()
        if parent is not None and parent.pid != 1:
            children = parent.children()
            for proc in children:
                if proc.name() == "gcs_server":
                    if self._gcs_proc != proc:
                        self._gcs_proc = proc
                    self._gcs_tags["WorkerPid"] = str(self._gcs_proc.pid)
                    self._gcs_cpu_percent.observe(self._gcs_proc.cpu_percent(),
                                                  self._gcs_tags)
                    memory_info = self._gcs_proc.memory_info()
                    if hasattr(memory_info, "rss"):
                        self._gcs_mem_rss.observe(memory_info.rss,
                                                  self._gcs_tags)
                        self._gcs_mem_util.observe(
                            self._gcs_proc.memory_percent(), self._gcs_tags)
                    break

    async def run(self, server):
        await asyncio.gather(self._report_worker_acquired_resources_metrics(),
                             self._record_dashboard_system_metrics(),
                             self._record_gcs_system_metrics())
