import asyncio
import logging
import time

import aiohttp.web

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.resource.resource_consts as resource_constants

from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class ResourceHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._gcs_node_info_stub = None
        self._gcs_actor_info_stub = None

    @routes.get("/resource/cluster")
    @dashboard_utils.aiohttp_cache(10)
    async def get_cluster_resources(self, req) -> aiohttp.web.Response:
        logger.info("Received a cluster resource request")
        return dashboard_utils.rest_response(
            success=True,
            message="Got cluster resources succeed.",
            **DataSource.cluster_resources)

    @routes.get("/resource/job/{job_id}")
    @dashboard_utils.aiohttp_cache(10)
    async def get_job_resources(self, req) -> aiohttp.web.Response:
        job_id = req.match_info.get("job_id")
        logger.info("Received a job request request, job id is %s", job_id)
        job_resources = DataSource.job_resources.get("resources", {})
        data = {
            "timestamp": DataSource.job_resources.get("timestamp", 0),
            "resources": job_resources.get(job_id, {})
        }
        return dashboard_utils.rest_response(
            success=True, message="Got job resources succeed.", **data)

    @routes.get("/resource/node/{node_id}")
    @dashboard_utils.aiohttp_cache(10)
    async def get_node_resources(self, req) -> aiohttp.web.Response:
        node_id = req.match_info.get("node_id")
        logger.info("Received a node resource request, node id is %s", node_id)
        node_resources = DataSource.node_resources.get("resources", {})
        data = {
            "timestamp": DataSource.node_resources.get("timestamp", 0),
            "resources": node_resources.get(node_id, {})
        }
        return dashboard_utils.rest_response(
            success=True, message="Got node resources succeed.", **data)

    @async_loop_forever(resource_constants.RESOURCE_UPDATE_INTERVAL_SECONDS)
    async def _update_cluster_resources(self):
        reply = await self._gcs_node_resource_info_stub.GetClusterResources(
            gcs_service_pb2.GetClusterResourcesRequest())
        data = dashboard_utils.message_to_dict(
            reply, {"nodeId"}, including_default_value_fields=True)
        logger.debug("Cluster resources size is %d", len(data))
        timestamp = int(time.time())
        details = {}
        for node in data.get("nodeResources"):
            total_resources = node.get("totalResources")
            available_resources = node.get("availableResources")
            for key in total_resources:
                if key in details:
                    details[key]["total"] += total_resources.get(key)
                    details[key]["available"] += available_resources.get(
                        key, 0)
                else:
                    details[key] = {
                        "total": total_resources.get(key),
                        "available": available_resources.get(key, 0)
                    }
        for key in details:
            if key.startswith("memory"):
                details[key]["total"] /= (1024**2)
                details[key]["available"] /= (1024**2)
        cluster_resources = {"timestamp": timestamp, "resources": details}
        logger.info("Updating cluster resources whose size is %d",
                    len(cluster_resources.get("resources", {})))
        DataSource.cluster_resources.reset(cluster_resources)

    @async_loop_forever(resource_constants.RESOURCE_UPDATE_INTERVAL_SECONDS)
    async def _update_job_and_node_resources(self):
        data = await self._get_job_distributions()
        logger.debug("Got job distributions whose size is %d",
                     len(data.get("nodeInfoList", [])))
        timestamp = int(time.time())
        self._update_job_resources(data, timestamp)
        self._update_node_resources(data, timestamp)

    async def _get_job_distributions(self):
        reply = await self._gcs_actor_info_stub.GetJobDistribution(
            gcs_service_pb2.GetJobDistributionRequest())
        return dashboard_utils.message_to_dict(
            reply, {"nodeId", "jobId", "workerProcessId"},
            including_default_value_fields=True)

    def _update_job_resources(self, data, timestamp):
        try:
            job_resources = {
                "timestamp": timestamp,
                "resources": self._parse_job_resources(data)
            }
            logger.debug("Updating job resources whose size is %d",
                         len(job_resources.get("resources", {})))
            DataSource.job_resources.reset(job_resources)
        except Exception:
            logger.exception("Updating job resources error.")

    def _update_node_resources(self, data, timestamp):
        try:
            node_resources = {
                "timestamp": timestamp,
                "resources": self._parse_node_resources(data)
            }
            logger.debug("Updating node resources whose size is %d",
                         len(node_resources.get("resources", {})))
            DataSource.node_resources.reset(node_resources)
        except Exception:
            logger.exception("Updating node resources error.")

    def _parse_job_resources(self, data):
        resources = {}
        for node_info in data.get("nodeInfoList"):
            node = {
                "nodeId": node_info.get("nodeId"),
                "nodeResources": {},
                "jobResources": {},
                "processResources": []
            }
            for key in node_info.get("totalResources"):
                total = node_info.get("totalResources").get(key)
                available = node_info.get("availableResources").get(key, 0)
                if key.startswith("memory"):
                    node["nodeResources"][key] = {
                        "total": total / (1024**2),
                        "available": available / (1024**2)
                    }
                else:
                    node["nodeResources"][key] = {
                        "total": total,
                        "available": available
                    }
            for job_info in node_info.get("jobInfoList"):
                job_id = job_info.get("jobId")
                if job_id not in resources:
                    resources[job_id] = {
                        "jobId": job_id,
                        "acquiredResources": {},
                        "nodes": []
                    }
                job_resources = {"acquiredResources": {}}
                process_resources = []
                for worker_info in job_info.get("workerInfoList"):
                    acquired_resources = {}
                    for key, val in worker_info.get(
                            "acquiredResources").items():
                        if key.startswith("memory"):
                            acquired_resources[key] = val / (1024**2)
                        else:
                            acquired_resources[key] = val
                    pid_resource = {
                        "pid": worker_info.get("pid"),
                        "language": worker_info.get("language"),
                        "acquiredResources": acquired_resources
                    }
                    process_resources.append(pid_resource)
                    for key, val in acquired_resources.items():
                        acquired_resources_ref = resources[job_id][
                            "acquiredResources"]
                        if key in acquired_resources_ref:
                            acquired_resources_ref[key] += val
                        else:
                            acquired_resources_ref[key] = val

                        acquired_resources_ref = job_resources[
                            "acquiredResources"]
                        if key in acquired_resources_ref:
                            acquired_resources_ref[key] += val
                        else:
                            acquired_resources_ref[key] = val
                node["jobResources"] = job_resources
                node["processResources"] = process_resources
                resources[job_id]["nodes"].append(node)
        return resources

    def _parse_node_resources(self, data):
        all_node_resources = {}
        for node_info in data.get("nodeInfoList"):
            node_id = node_info.get("nodeId")
            resources = {"nodeId": node_id, "resources": {}, "jobs": []}
            all_node_resources[node_id] = resources
            for key in node_info.get("totalResources"):
                total_val = node_info.get("totalResources").get(key)
                available_val = node_info.get("availableResources").get(key, 0)
                if key.startswith("memory"):
                    total_val = total_val / (1024**2)
                    available_val = available_val / (1024**2)
                resources["resources"][key] = {
                    "total": total_val,
                    "available": available_val
                }
            for job_info in node_info.get("jobInfoList"):
                job = {}
                job_resources = {"acquiredResources": {}}
                process_resources = []
                for worker_info in job_info.get("workerInfoList"):
                    acquired_resources = worker_info.get("acquiredResources")
                    for key, val in acquired_resources.items():
                        if key.startswith("memory"):
                            acquired_resources[key] = val / (1024**2)
                        else:
                            acquired_resources[key] = val

                    process = {
                        "pid": worker_info.get("workerProcessId"),
                        "acquiredResources": acquired_resources
                    }
                    for key, val in acquired_resources.items():
                        acquired_resources_ref = job_resources[
                            "acquiredResources"]
                        if key in acquired_resources_ref:
                            acquired_resources_ref[key] += val
                        else:
                            acquired_resources_ref[key] = val
                    process_resources.append(process)
                job = {
                    "jobId": job_info.get("jobId"),
                    "jobResources": job_resources,
                    "processResources": process_resources
                }
                resources["jobs"].append(job)
        return all_node_resources

    async def run(self, server):
        self._gcs_actor_info_stub = \
            gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_node_resource_info_stub = gcs_service_pb2_grpc.\
            NodeResourceInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)

        await asyncio.gather(self._update_cluster_resources(),
                             self._update_job_and_node_resources())
