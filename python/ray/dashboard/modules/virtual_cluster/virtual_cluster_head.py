import asyncio
import logging
from typing import Any, Dict

import aiohttp.web

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_service_pb2 import (
    ReplicaSet,
    CreateOrUpdateVirtualClusterRequest,
    CreateOrUpdateVirtualClusterReply,
    RemoveVirtualClusterRequest,
    RemoveVirtualClusterReply,
    GetAllVirtualClustersRequest,
    GetAllVirtualClustersReply
)
from ray.core.generated.gcs_pb2 import (
    JobExecMode
)

from ray._private.utils import get_or_create_event_loop
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

class VirtualClusterHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

        self._gcs_virtual_cluster_info_stub = (
            gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(
                dashboard_head.aiogrpc_gcs_channel
            )
        )


    @routes.get("/virtual_clusters")
    @dashboard_optional_utils.aiohttp_cache(10)
    async def get_all_virtual_clusters(self, req) -> aiohttp.web.Response:
        reply = await self._gcs_virtual_cluster_info_stub.GetAllVirtualClusters(
            GetAllVirtualClustersRequest())

        if reply.status.code == 0:
            data = dashboard_utils.message_to_dict(reply)

            return dashboard_optional_utils.rest_response(
                success=True,
                message="All virtual clusters fetched.",
                virtual_clusters=data
            )
        else:
            logger.info("Failed to get all virtual clusters")
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Failed to get all virtual clusters: {}".format(
                    reply.status.message
                )
            )
    

    @routes.post("/virtual_clusters")
    async def create_or_update_virtual_cluster(self, req) -> aiohttp.web.Response:
        virtual_cluster_info_json = await req.json()
        logger.info("POST /virtual_clusters %s", virtual_cluster_info_json)

        virtual_cluster_info = dict(virtual_cluster_info_json)
        virtual_cluster_id=virtual_cluster_info["virtualClusterId"]
        job_exec_mode=JobExecMode.Mixed
        if str(virtual_cluster_info.get("jobExecMode", "mixed")).lower() == "exclusive":
            job_exec_mode=JobExecMode.Exclusive

        replica_set_list = []
        for data in virtual_cluster_info["nodeTypeAndCountList"]:
            replica_set = ReplicaSet(template_id=data["nodeType"], replicas=data["nodeCount"])
            replica_set_list.append(replica_set)

        request = CreateOrUpdateVirtualClusterRequest(
            virtual_cluster_id=virtual_cluster_id,
            virtual_cluster_name=virtual_cluster_info.get("name", ""),
            mode=job_exec_mode,
            replica_set_list=replica_set_list,
            revision=int(virtual_cluster_info.get("revision", 0))
        )
        reply = await self._gcs_virtual_cluster_info_stub.CreateOrUpdateVirtualCluster(request)
        if reply.status.code == 0:
            logger.info("Virtual cluster %s created or updated", virtual_cluster_id)
            data = dashboard_utils.message_to_dict(reply)

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Virtual cluster created or updated.",
                virtual_cluster_id=virtual_cluster_id,
                revision=data.get("revision", 0),
                node_instances=data["nodeInstances"]
            )
        else:
            logger.info("Failed to create or update virtual cluster %s", virtual_cluster_id)
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Failed to create or update virtual cluster {virtual_cluster_id}: {}".format(
                    reply.status.message
                ),
                virtual_cluster_id=virtual_cluster_id
            )


    @routes.delete("/virtual_clusters/{virtual_cluster_id}")
    async def remove_virtual_cluster(self, req) -> aiohttp.web.Response:
        virtual_cluster_id = req.match_info.get("virtual_cluster_id")
        request = RemoveVirtualClusterRequest(virtual_cluster_id=virtual_cluster_id)
        reply = await self._gcs_virtual_cluster_info_stub.RemoveVirtualCluster(request)

        if reply.status.code == 0:
            logger.info("Virtual cluster %s removed", virtual_cluster_id)
            return dashboard_optional_utils.rest_response(
                success=True,
                message=f"Virtual cluster {virtual_cluster_id} removed.",
                virtual_cluster_id=virtual_cluster_id,
            )
        else:
            logger.info("Failed to remove virtual cluster %s", virtual_cluster_id)
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Failed to remove virtual cluster {virtual_cluster_id}: {}".format(
                    reply.status.message),
                virtual_cluster_id=virtual_cluster_id,
            )
    

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False