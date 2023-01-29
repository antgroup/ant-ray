import time
import json
import re
import asyncio
import logging

import aiohttp.web
import ray.new_dashboard.modules.actor.actor_consts
from aioredis.pubsub import Receiver
import grpc.aio as aiogrpc

import ray.gcs_utils
import ray.new_dashboard.modules.stats_collector.stats_collector_consts \
    as stats_collector_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.consts as dashboard_consts
from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.memory_utils import GroupByType, SortingType
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import (
    DataSource,
    DataOrganizer,
)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def node_stats_to_dict(message):
    decode_keys = {
        "actorId", "jobId", "taskId", "parentTaskId", "sourceActorId",
        "callerId", "rayletId", "workerId", "placementGroupId"
    }
    core_workers_stats = message.core_workers_stats
    message.ClearField("core_workers_stats")
    try:
        result = dashboard_utils.message_to_dict(message, decode_keys)
        result["coreWorkersStats"] = [
            dashboard_utils.message_to_dict(
                m, decode_keys, including_default_value_fields=True)
            for m in core_workers_stats
        ]
        return result
    finally:
        message.core_workers_stats.extend(core_workers_stats)


class StatsCollector(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # JobInfoGcsServiceStub
        self._gcs_job_info_stub = None
        self._collect_memory_info = False
        DataSource.nodes.signal.append(self._update_stubs)
        DataSource.dead_nodes.signal.append(self._purge_node_stats)

    async def _update_stubs(self, change):
        if change.old:
            node_id = change.old.key
            self._stubs.pop(node_id, None)
        if change.new:
            node_id, node_info = change.new
            ip = node_info["nodeManagerAddress"]
            port = node_info["nodeManagerPort"]
            channel = aiogrpc.insecure_channel(
                f"{ip}:{port}", options=dashboard_consts.GLOBAL_GRPC_OPTIONS)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[node_id] = stub

    @routes.get("/nodes")
    @dashboard_utils.aiohttp_cache
    async def get_all_nodes(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        exclude_inactive = req.query.get("exclude-inactive") == "true"
        if view == "summary":
            nodes_summary = \
                await DataOrganizer.get_nodes_summary(exclude_inactive)
            return dashboard_utils.rest_response(
                success=True,
                message="Node summary fetched.",
                summary=nodes_summary)
        elif view == "details":
            nodes_details = \
                await DataOrganizer.get_nodes_details(exclude_inactive)
            return dashboard_utils.rest_response(
                success=True,
                message="All node details fetched",
                clients=nodes_details,
            )
        elif view is not None and view.lower() == "hostNameList".lower():
            alive_hostnames = [
                node["nodeManagerHostname"]
                for node in DataSource.nodes.values()
            ]
            return dashboard_utils.rest_response(
                success=True,
                message="Node hostname list fetched.",
                host_name_list=alive_hostnames)
        else:
            return dashboard_utils.rest_response(
                success=False, message=f"Unknown view {view}")

    @routes.get("/nodes/{node_id}")
    @dashboard_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        node_id = req.match_info.get("node_id")
        node_info = await DataOrganizer.get_node_info(node_id)
        return dashboard_utils.rest_response(
            success=True, message="Node details fetched.", detail=node_info)

    @routes.get("/named_node/{node_name}/actor_num")
    @dashboard_utils.aiohttp_cache
    async def get_named_node(self, req) -> aiohttp.web.Response:
        req_ip = req.match_info.get("node_name")
        req_node_id = None
        for node_id, ip in DataSource.node_id_to_ip.items():
            if ip == req_ip:
                req_node_id = node_id
                break
        if req_node_id is not None:
            node_info = await DataOrganizer.get_node_info(req_node_id)
            actors = node_info["actors"]
            alive_count = 0
            for _actor_id, actor_data in actors.items():
                if actor_data["state"] == "ALIVE":
                    alive_count += 1
            return dashboard_utils.rest_response(
                success=True,
                message="Node details fetched.",
                actor_num=alive_count)
        else:
            return dashboard_utils.rest_response(
                success=False, message=f"Can't find node with name {req_ip}")

    @routes.get("/memory/memory_table")
    async def get_memory_table(self, req) -> aiohttp.web.Response:
        group_by = req.query.get("group_by")
        sort_by = req.query.get("sort_by")
        kwargs = {}
        if group_by:
            kwargs["group_by"] = GroupByType(group_by)
        if sort_by:
            kwargs["sort_by"] = SortingType(sort_by)

        memory_table = await DataOrganizer.get_memory_table(**kwargs)
        return dashboard_utils.rest_response(
            success=True,
            message="Fetched memory table",
            memory_table=memory_table.as_dict())

    @routes.get("/memory/set_fetch")
    async def set_fetch_memory_info(self, req) -> aiohttp.web.Response:
        should_fetch = req.query["shouldFetch"]
        if should_fetch == "true":
            self._collect_memory_info = True
        elif should_fetch == "false":
            self._collect_memory_info = False
        else:
            return dashboard_utils.rest_response(
                success=False,
                message=f"Unknown argument to set_fetch {should_fetch}")
        return dashboard_utils.rest_response(
            success=True,
            message=f"Successfully set fetching to {should_fetch}")

    @routes.get("/node_logs")
    async def get_logs(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = req.query.get("pid")
        node_logs = DataSource.ip_and_pid_to_logs[ip]
        payload = node_logs.get(pid, []) if pid else node_logs
        return dashboard_utils.rest_response(
            success=True, message="Fetched logs.", logs=payload)

    @routes.get("/node_errors")
    async def get_errors(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = req.query.get("pid")
        node_errors = DataSource.ip_and_pid_to_errors[ip]
        filtered_errs = node_errors.get(pid, []) if pid else node_errors
        return dashboard_utils.rest_response(
            success=True, message="Fetched errors.", errors=filtered_errs)

    @async_loop_forever(
        stats_collector_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def _update_node_stats(self):
        # Copy self._stubs to avoid `dictionary changed size during iteration`.
        for node_id, stub in list(self._stubs.items()):
            try:
                reply = await stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(
                        include_memory_info=self._collect_memory_info),
                    timeout=2)
                reply_dict = node_stats_to_dict(reply)
                reply_dict["timestamp"] = time.time()
                DataSource.node_stats[node_id] = reply_dict
            except Exception:
                logger.exception(f"Error updating node stats of {node_id}.")

    @staticmethod
    async def _purge_node_stats(change):
        if change.new is None and change.old:
            node_id = change.old.key
            DataSource.node_stats.pop(node_id, None)

    async def _update_log_info(self):
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        channel = receiver.channel(ray.gcs_utils.LOG_FILE_CHANNEL)
        await aioredis_client.subscribe(channel)
        logger.info("Subscribed to %s", channel)

        async for sender, msg in receiver.iter():
            try:
                data = json.loads(ray._private.utils.decode(msg))
                ip = data["ip"]
                pid = str(data["pid"])
                logs_for_ip = dict(DataSource.ip_and_pid_to_logs.get(ip, {}))
                logs_for_pid = list(logs_for_ip.get(pid, []))
                logs_for_pid.extend(data["lines"])
                logs_for_ip[pid] = logs_for_pid
                DataSource.ip_and_pid_to_logs[ip] = logs_for_ip
                logger.info(f"Received a log for {ip} and {pid}")
            except Exception:
                logger.exception("Error receiving log info.")

    async def _update_error_info(self):
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        key = ray.gcs_utils.RAY_ERROR_PUBSUB_PATTERN
        pattern = receiver.pattern(key)
        await aioredis_client.psubscribe(pattern)
        logger.info("Subscribed to %s", key)

        async for sender, msg in receiver.iter():
            try:
                _, data = msg
                pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(data)
                error_data = ray.gcs_utils.ErrorTableData.FromString(
                    pubsub_msg.data)
                message = error_data.error_message
                message = re.sub(r"\x1b\[\d+m", "", message)
                match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
                if match:
                    pid = match.group(1)
                    ip = match.group(2)
                    errs_for_ip = dict(
                        DataSource.ip_and_pid_to_errors.get(ip, {}))
                    pid_errors = list(errs_for_ip.get(pid, []))
                    pid_errors.append({
                        "message": message,
                        "timestamp": error_data.timestamp,
                        "type": error_data.type
                    })
                    errs_for_ip[pid] = pid_errors
                    DataSource.ip_and_pid_to_errors[ip] = errs_for_ip
                    logger.info(f"Received error entry for {ip} {pid}")
            except Exception:
                logger.exception("Error receiving error info.")

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_job_info_stub = \
            gcs_service_pb2_grpc.JobInfoGcsServiceStub(gcs_channel)

        await asyncio.gather(
            self._update_node_stats(),
            # self._update_log_info(),
            # self._update_error_info(),
        )
