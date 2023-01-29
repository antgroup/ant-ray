import asyncio
import copy
import json
import logging
import yaml
import os
import aiohttp.web
import grpc.aio as aiogrpc
import msgpack

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.consts as dashboard_consts
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.new_dashboard.datacenter import DataSource
from ray.new_dashboard.modules.reporter import reporter_consts

from ray.core.generated import common_pb2
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.utils import async_loop_forever

from ray.ray_constants import (
    gcs_task_scheduling_enabled, DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS, DEBUG_AUTOSCALING_STATUS_LEGACY)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class ReportHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # In the e2e tests of runtime resource scheduling, there might be
        # multiple reporter agents in one node. Use this stubs to request
        # runtime reports from all agents.
        self._runtime_stubs = {}
        self._ray_config = None
        DataSource.agents.signal.append(self._update_stubs)
        DataSource.dead_nodes.signal.append(self._purge_node_physical_stats)

    async def _update_stubs(self, change):
        if change.old:
            node_id, agent_data = change.old
            ip = DataSource.node_id_to_ip[node_id]
            port = agent_data[dashboard_consts.DASHBOARD_AGENT_GRPC_PORT]
            self._stubs.pop(ip)
            self._runtime_stubs.pop((ip, port))
        if change.new:
            node_id, agent_data = change.new
            ip = DataSource.node_id_to_ip[node_id]
            port = agent_data[dashboard_consts.DASHBOARD_AGENT_GRPC_PORT]
            channel = aiogrpc.insecure_channel(
                f"{ip}:{port}", options=dashboard_consts.GLOBAL_GRPC_OPTIONS)
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            self._stubs[ip] = stub
            self._runtime_stubs[(ip, port)] = stub

    @routes.get("/api/launch_profiling")
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = int(req.query["pid"])
        duration = int(req.query["duration"])
        reporter_stub = self._stubs[ip]
        reply = await reporter_stub.GetProfilingStats(
            reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        profiling_info = (json.loads(reply.profiling_stats)
                          if reply.profiling_stats else reply.std_out)
        return dashboard_utils.rest_response(
            success=True,
            message="Profiling success.",
            profiling_info=profiling_info)

    @routes.get("/api/ray_config")
    async def get_ray_config(self, req) -> aiohttp.web.Response:
        if self._ray_config is None:
            try:
                config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
            except yaml.YAMLError:
                return dashboard_utils.rest_response(
                    success=False,
                    message=f"No config found at {config_path}.",
                )
            except FileNotFoundError:
                return dashboard_utils.rest_response(
                    success=False,
                    message="Invalid config, could not load YAML.")

            payload = {
                "min_workers": cfg.get("min_workers", "unspecified"),
                "max_workers": cfg.get("max_workers", "unspecified")
            }

            try:
                payload["head_type"] = cfg["head_node"]["InstanceType"]
            except KeyError:
                payload["head_type"] = "unknown"

            try:
                payload["worker_type"] = cfg["worker_nodes"]["InstanceType"]
            except KeyError:
                payload["worker_type"] = "unknown"

            self._ray_config = payload

        return dashboard_utils.rest_response(
            success=True,
            message="Fetched ray config.",
            **self._ray_config,
        )

    @routes.get("/api/cluster_status")
    async def get_cluster_status(self, req):
        """Returns status information about the cluster.

        Currently contains two fields:
            autoscaling_status (str): a status message from the autoscaler.
            autoscaling_error (str): an error message from the autoscaler if
                anything has gone wrong during autoscaling.

        These fields are both read from the GCS, it's expected that the
        autoscaler writes them there.
        """

        aioredis_client = self._dashboard_head.aioredis_client
        legacy_status = await aioredis_client.hget(
            DEBUG_AUTOSCALING_STATUS_LEGACY, "value")
        formatted_status_string = await aioredis_client.hget(
            DEBUG_AUTOSCALING_STATUS, "value")
        formatted_status = json.loads(formatted_status_string.decode()
                                      ) if formatted_status_string else {}
        error = await aioredis_client.hget(DEBUG_AUTOSCALING_ERROR, "value")
        return dashboard_utils.rest_response(
            success=True,
            message="Got cluster status.",
            autoscaling_status=legacy_status.decode()
            if legacy_status else None,
            autoscaling_error=error.decode() if error else None,
            cluster_status=formatted_status if formatted_status else None,
        )

    @routes.get("/utils/{cmd}")
    async def run_cmd(self, req) -> aiohttp.web.Response:
        cmd = req.match_info.get("cmd")
        if cmd not in reporter_consts.RUN_CMD:
            return dashboard_utils.rest_response(
                success=False,
                message=f"Unable to run command {cmd}, "
                f"available cmd: {reporter_consts.RUN_CMD}")

        kwargs = dict(req.query)
        ip = kwargs.pop("ip")
        timeout = kwargs.pop("timeout",
                             reporter_consts.RUN_CMD_TIMEOUT_SECONDS)

        for key, converter in reporter_consts.RUN_CMD[cmd][
                "converter"].items():
            value = kwargs.get(key)
            if value is not None:
                kwargs[key] = converter(value)

        reporter_stub = self._stubs[ip]
        reply = await reporter_stub.RunCmd(
            reporter_pb2.RunCmdRequest(
                cmd_name=cmd,
                cmd_kwargs=json.dumps(kwargs),
                timeout=int(timeout)))
        cmd_formatter = reporter_consts.RUN_CMD[cmd]["formatter"]
        return dashboard_utils.rest_response(
            success=True,
            message=f"cmd formatter: {cmd_formatter}, kwargs: {kwargs}",
            output=reply.output)

    @staticmethod
    async def _purge_node_physical_stats(change):
        if change.new is None and change.old:
            node_id = change.old.key
            DataSource.node_physical_stats.pop(node_id, None)

    async def _report_cluster_runtime_resources(self, nodes_to_update):
        node_runtime_resources_list = []
        for node_id in nodes_to_update:
            data = DataSource.node_physical_stats[node_id]
            worker_stat_list = []
            for stat in data.get("workerRuntime"):
                worker_stat = common_pb2.WorkerRuntimeStat(
                    pid=stat.get("pid"),
                    memory_tail=int(stat.get("memoryTail")),
                    cpu_tail=stat.get("cpuTail"))
                worker_stat_list.append(worker_stat)
            node_runtime_resources = gcs_service_pb2.NodeRuntimeResources(
                node_id=bytes.fromhex(node_id),
                worker_stat_list=worker_stat_list)
            node_runtime_resources_list.append(node_runtime_resources)
        request = gcs_service_pb2.ReportClusterRuntimeResourcesRequest(
            node_runtime_resources_list=node_runtime_resources_list)
        reply = await self._gcs_runtime_resource_info_stub.\
            ReportClusterRuntimeResources(request)
        if reply.status.code == 0:
            logger.info("Succeeded to report runtime resources")
        else:
            logger.info("Failed to report runtime resources")

    @async_loop_forever(reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)
    async def _update_node_physical_stats(self):
        runtime_stubs = copy.copy(self._runtime_stubs)
        ips = []
        tasks = []
        for key, stub in runtime_stubs.items():
            task = stub.GetReportData(
                reporter_pb2.GetReportDataRequest(), timeout=60)
            tasks.append(task)
            ips.append(key[0])

        results = await asyncio.gather(*tasks, return_exceptions=True)
        nodes_with_runtime_updated = []
        for ip, r in zip(ips, results):
            if not isinstance(r, Exception):
                if r.success and r.data:
                    data = msgpack.loads(r.data)
                    if data:
                        DataSource.node_physical_stats.update(data)
                        node_id = list(data.keys())[0]
                        if "workerRuntime" in data[node_id]:
                            nodes_with_runtime_updated.append(node_id)
                elif r.error:
                    logger.error(
                        "GetReportData from node %s encountered error:\n %s",
                        ip, r.error)
            else:
                logger.error(
                    "GetReportData from node %s encountered error: %s", ip, r)
        if len(nodes_with_runtime_updated) > 0:
            await self._report_cluster_runtime_resources(
                nodes_with_runtime_updated)

    async def run(self, server):
        if gcs_task_scheduling_enabled():
            self._gcs_runtime_resource_info_stub = gcs_service_pb2_grpc.\
                RuntimeResourceInfoGcsServiceStub(
                    self._dashboard_head.aiogrpc_gcs_channel)

        await self._update_node_physical_stats()
