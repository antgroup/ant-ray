import os
import json
import logging
import asyncio

import aiohttp.web

import time
import ray
import ray.new_dashboard.modules.job.job_consts as job_consts
import ray.new_dashboard.utils as dashboard_utils
from ray import ray_constants
from ray.core.generated import common_pb2
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import (
    DataSource,
    GlobalSignals,
)
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.new_dashboard.modules.job.system_job_utils import (
    load_cluster_config_from_url,
    get_field_from_json,
    SystemJobConf,
)
from google.protobuf import json_format
from itertools import chain

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable
DEBUG = os.environ.get("DEBUG_JOB") == "1"
RAY_DEFAULT_NODEGROUP = "NAMESPACE_LABEL_RAY_DEFAULT"
INACTIVE_JOB_STATES = {"FINISHED", "FAILED", "CANCEL"}


class JobUpdater(dashboard_utils.PSubscribeUpdater):
    @staticmethod
    def _job_table_data_to_dict(message):
        decode_keys = {"jobId", "rayletId"}
        # Job info
        job_payload = message.job_payload
        message.ClearField("job_payload")
        job_info = {}
        try:
            job_info = json.loads(job_payload)
        except Exception:
            logger.exception(
                "Parse job payload failed, job id: %s, job payload: %s",
                binary_to_hex(message.job_id), job_payload)
        try:
            data = dashboard_utils.message_to_dict(
                message, decode_keys, including_default_value_fields=True)
            job_info.update(data)
            return job_info
        finally:
            message.job_payload = job_payload

    @classmethod
    async def _update_all(cls, gcs_channel):
        gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            gcs_channel)
        # Get all job info.
        while True:
            try:
                logger.info("Getting all job info from GCS.")
                request = gcs_service_pb2.GetAllJobInfoRequest()
                reply = await gcs_job_info_stub.GetAllJobInfo(
                    request, timeout=120)
                if reply.status.code == 0:
                    jobs = {}
                    inactive_jobs = []
                    for job_table_data in reply.job_info_list:
                        job_id = ray._raylet.JobID(job_table_data.job_id)
                        if job_id.is_submitted_from_dashboard():
                            data = cls._job_table_data_to_dict(job_table_data)
                            if data["state"] in INACTIVE_JOB_STATES:
                                inactive_jobs.append((data["jobId"], data))
                            else:
                                jobs[data["jobId"]] = data
                        else:
                            logger.info(
                                "Ignore job %s which is not submitted from"
                                " dashboard.", job_id.hex())
                    # Update jobs.
                    DataSource.jobs.reset(jobs)
                    # Sort inactive jobs by timestamp and then update.
                    inactive_jobs.sort(key=lambda kv: kv[1]["timestamp"])
                    DataSource.inactive_jobs.reset(inactive_jobs)
                    logger.info("Received %d job info from GCS.", len(jobs))
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllJobInfo: {reply.status.message}")
            except Exception:
                logger.exception("Error Getting all job info from GCS.")
                await asyncio.sleep(
                    job_consts.RETRY_GET_ALL_JOB_INFO_INTERVAL_SECONDS)

    @classmethod
    async def _update_from_psubscribe_channel(cls, receiver):
        # Receive jobs from channel.
        async for data in receiver.iter():
            try:
                data = data["data"]
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(data)
                message = ray.gcs_utils.JobTableData.FromString(
                    pubsub_message.data)
                job_id = ray._raylet.JobID(message.job_id)
                if job_id.is_submitted_from_dashboard():
                    job_table_data = cls._job_table_data_to_dict(message)
                    job_id = job_table_data["jobId"]
                    # Update jobs.
                    if job_table_data["state"] in INACTIVE_JOB_STATES:
                        # Remove inactive job from jobs if necessary
                        if job_id in DataSource.jobs:
                            del DataSource.jobs[job_id]
                        DataSource.inactive_jobs[job_id] = job_table_data
                    else:
                        DataSource.jobs[job_id] = job_table_data
                    logger.info(
                        "Recieve updated job info from psubscribe "
                        "channel: %s.", job_table_data)
                else:
                    logger.info(
                        "Ignore job %s which is not submitted from dashboard.",
                        job_id.hex())
            except Exception:
                logger.exception("Error receiving job info.")


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # NodegroupInfoGcsServiceStub
        self._gcs_nodegroup_stub = None
        # JobInfoGcsService
        self._gcs_job_info_stub = None

        if hasattr(self.get_all_jobs, "cache"):
            DataSource.jobs.signal.append(self._flush_get_all_jobs_cache)
            DataSource.inactive_jobs.signal.append(
                self._flush_get_all_jobs_cache)
        if hasattr(self.get_job, "cache"):
            DataSource.jobs.signal.append(self._flush_get_job_cache)
            DataSource.inactive_jobs.signal.append(self._flush_get_job_cache)
        DataSource.inactive_jobs.signal.append(
            self._collect_inactive_job_garbage)
        # Both channels job_consts.JOB_CHANNEL and job_consts.JOB_QUOTA_CHANNEL
        # needs to be subscribed, the former is 'JOB' and the latter is
        # 'JOB_QUOTA', so we just need to subscribe 'JOB*'.
        GlobalSignals.gcs_reconnected.append(
            JobUpdater.create(
                f"{job_consts.JOB_CHANNEL}*",
                self._dashboard_head.aioredis_client,
                self._dashboard_head.aiogrpc_gcs_channel).restart)

    async def _collect_inactive_job_garbage(self, change):
        if change.old and change.new is None:
            job_id = change.old.key
            # events
            DataSource.events.pop(job_id, None)
            DataSource.job_actor_pipeline_events.pop(job_id, None)
            DataSource.job_pipeline_events.pop(job_id, None)
            DataSource.job_worker_pipeline_events.pop(job_id, None)
            # resources
            DataSource.job_resources.pop(job_id, None)
            # dead actors
            for actor_id in DataSource.job_actors.pop(job_id, {}):
                DataSource.dead_actors.pop(actor_id)

            logger.info("Garbage collected for inactive job %s", job_id)

    async def _next_job_id(self):
        counter_str = await self._dashboard_head.aioredis_client.incr(
            job_consts.REDIS_KEY_JOB_COUNTER)
        job_id_int = int(counter_str)
        if job_id_int & (1 << 31):
            raise Exception(
                f"Job id overflow: {ray.JobID.from_int(job_id_int)}")
        job_id_int |= (1 << 31)
        return ray.JobID.from_int(job_id_int)

    async def _reserve_job_id(self, job_id_int):
        if job_id_int & (1 << 31):
            raise Exception(
                f"Job id overflow: {ray.JobID.from_int(job_id_int)}")

        while True:
            counter_str = await self._dashboard_head.aioredis_client.incr(
                job_consts.REDIS_KEY_JOB_COUNTER)
            if int(counter_str) >= job_id_int:
                job_id_int |= (1 << 31)
                return ray.JobID.from_int(job_id_int)
            elif int(counter_str) & (1 << 31):
                raise Exception(
                    f"Job id overflow: {ray.JobID.from_int(int(counter_str))}")

    @routes.get("/namespaces")
    @dashboard_utils.aiohttp_cache(30)
    async def get_all_nodegroups(self, req) -> aiohttp.web.Response:
        include_sub_nodegroups = (req.query.get("include_sub_namespaces",
                                                "0") == "1")
        show_node_spec = (req.query.get("show_node_spec", "0") == "1")
        all_nodegroups = []
        request = gcs_service_pb2.GetAllNodegroupsRequest(
            include_sub_nodegroups=include_sub_nodegroups)
        reply = await self._gcs_nodegroup_stub.GetAllNodegroups(request)
        if reply.status.code == 0:
            for nodegroup_data in reply.nodegroup_data_list:
                nodegroup_info = dict(
                    namespace_id=nodegroup_data.nodegroup_id,
                    name=nodegroup_data.nodegroup_name,
                    enable_sub_nodegroup_isolation=nodegroup_data.
                    enable_sub_nodegroup_isolation,
                    schedule_options=dict(nodegroup_data.schedule_options))
                if show_node_spec:
                    host_name_list = {}
                    for key, val in nodegroup_data.host_to_node_spec.items():
                        host_name_list[key] = json.loads(
                            json_format.MessageToJson(val))
                    nodegroup_info["host_name_list"] = host_name_list
                else:
                    nodegroup_info["host_name_list"] = list(
                        nodegroup_data.host_to_node_spec.keys())
                all_nodegroups.append(nodegroup_info)
            return dashboard_utils.rest_response(
                success=True,
                message="All namespaces fetched.",
                namespaces=all_nodegroups)
        else:
            logger.info("Failed to get all nodegroups.")
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to get all namespaces: {}".format(
                    reply.status.message),
                namespaces=all_nodegroups)

    # Deprecated.
    @routes.get("/namespaces/resources")
    async def get_resources_of_all_nodegroups(self,
                                              req) -> aiohttp.web.Response:
        aggregate_type = req.query.get("aggregate_type", None)
        all_nodegroups_resources = []
        request = gcs_service_pb2.GetResourcesOfAllNodegroupsRequest()
        reply = await self._gcs_node_resource_info_stub.\
            GetResourcesOfAllNodegroups(request)
        if reply.status.code == 0:
            if aggregate_type == "namespace":
                for ns_id in sorted(reply.nodegroup_host_resource_map):
                    host_resource_map = reply.nodegroup_host_resource_map[
                        ns_id]
                    label_total_resource_dict = {}
                    label_available_resource_dict = {}
                    for host_id in sorted(host_resource_map.host_resource_map):
                        label_resource_map = \
                            host_resource_map.host_resource_map[host_id]
                        for resource_label in sorted(
                                label_resource_map.resource_map):
                            if resource_label == "memory":
                                label_resource_map.resource_map[
                                    resource_label].total /= (1024**2)
                                label_resource_map.resource_map[
                                    resource_label].available /= (1024**2)
                            label_total_resource_dict[
                                resource_label] = label_total_resource_dict.\
                                setdefault(resource_label, 0) + \
                                label_resource_map.resource_map[
                                    resource_label].total
                            label_available_resource_dict[
                                resource_label] = \
                                label_available_resource_dict.\
                                setdefault(resource_label, 0) + \
                                label_resource_map.resource_map[
                                    resource_label].available
                    label_resource_list = []
                    for resource_label in sorted(label_total_resource_dict):
                        label_resource_list.append(
                            dict(
                                resource_name=resource_label,
                                total=label_total_resource_dict[
                                    resource_label],
                                available=label_available_resource_dict[
                                    resource_label]))
                    all_nodegroups_resources.append(
                        dict(
                            namespace_id=ns_id, resources=label_resource_list))
            elif aggregate_type == "cluster":
                label_total_resource_dict = {}
                label_available_resource_dict = {}
                host_resource_map = reply.nodegroup_host_resource_map[
                    RAY_DEFAULT_NODEGROUP]
                for host_id in sorted(host_resource_map.host_resource_map):
                    label_resource_map = host_resource_map.host_resource_map[
                        host_id]
                    for resource_label in sorted(
                            label_resource_map.resource_map):
                        if resource_label == "memory":
                            label_resource_map.resource_map[
                                resource_label].total /= (1024**2)
                            label_resource_map.resource_map[
                                resource_label].available /= (1024**2)
                        label_total_resource_dict[
                            resource_label] = label_total_resource_dict.\
                            setdefault(resource_label, 0) + \
                            label_resource_map.resource_map[
                                resource_label].total
                        label_available_resource_dict[
                            resource_label] = label_available_resource_dict.\
                            setdefault(resource_label, 0) + \
                            label_resource_map.resource_map[
                                resource_label].available
                for resource_label in sorted(label_total_resource_dict):
                    all_nodegroups_resources.append(
                        dict(
                            resource_name=resource_label,
                            total=label_total_resource_dict[resource_label],
                            available=label_available_resource_dict[
                                resource_label]))
            else:
                for ns_id in sorted(reply.nodegroup_host_resource_map):
                    host_resource_map = reply.nodegroup_host_resource_map[
                        ns_id]
                    host_resource_list = []
                    for host_id in sorted(host_resource_map.host_resource_map):
                        label_resource_map = host_resource_map.\
                            host_resource_map[host_id]
                        label_resource_list = []
                        for resource_label in sorted(
                                label_resource_map.resource_map):
                            if resource_label == "memory":
                                label_resource_map.resource_map[
                                    resource_label].total /= (1024**2)
                                label_resource_map.resource_map[
                                    resource_label].available /= (1024**2)
                            resource_list = label_resource_map.resource_map[
                                resource_label]
                            label_resource_list.append(
                                dict(
                                    resource_name=resource_label,
                                    total=resource_list.total,
                                    available=resource_list.available))
                        host_resource_list.append(
                            dict(
                                host_name=host_id,
                                resources=label_resource_list))
                    all_nodegroups_resources.append(
                        dict(
                            namespace_id=ns_id,
                            host_resources=host_resource_list))

            return dashboard_utils.rest_response(
                success=True,
                message="Resources of all namespaces fetched.",
                resource_view=all_nodegroups_resources)
        else:
            logger.info("Failed to get resources of all nodegroups.")
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to get resources of all namespaces: {}".format(
                    reply.status.message),
                resource_view=all_nodegroups_resources)

    def _parse_host_resource_map(self, host_resource_map):
        host_list = []
        label_total_resource_dict = {}
        label_available_resource_dict = {}
        shapes = {}
        shape_group_to_count_map = {}
        for host_id in sorted(host_resource_map):
            label_resource_map = host_resource_map[host_id]
            label_resource_list = []
            for resource_label in sorted(label_resource_map.resource_map):
                if resource_label == "memory":
                    label_resource_map.resource_map[resource_label].total /= (
                        1024**2)
                    label_resource_map.resource_map[
                        resource_label].available /= (1024**2)
                label_total_resource_dict[
                    resource_label] = label_total_resource_dict.\
                    setdefault(resource_label, 0) + \
                    label_resource_map.resource_map[
                        resource_label].total
                label_available_resource_dict[
                    resource_label] = label_available_resource_dict.\
                    setdefault(resource_label, 0) + \
                    label_resource_map.resource_map[
                        resource_label].available
                resource_list = label_resource_map.resource_map[resource_label]
                label_resource_list.append(
                    dict(
                        resource_name=resource_label,
                        total=resource_list.total,
                        available=resource_list.available))
            host_list.append(
                dict(
                    host_name=host_id,
                    group=label_resource_map.shape_group,
                    has_driver=label_resource_map.has_driver,
                    resources=label_resource_list))
            if label_resource_map.shape_group not in shapes:
                shape_dict = {}
                for resource in label_resource_list:
                    if resource["resource_name"] == "memory":
                        continue
                    shape_dict[resource["resource_name"]] = resource["total"]
                shapes[label_resource_map.shape_group] = shape_dict
            shape_group_to_count_map[
                label_resource_map.shape_group] = \
                shape_group_to_count_map.get(
                    label_resource_map.shape_group, 0) + 1
        return host_list, label_total_resource_dict, \
            label_available_resource_dict, shapes, shape_group_to_count_map

    @routes.get("/namespaces/layered_resources")
    @dashboard_utils.aiohttp_cache(10)
    async def get_layered_resources_of_all_nodegroups(
            self, req) -> aiohttp.web.Response:
        include_pending = req.query.get("include_pending", None)
        resource_reply = gcs_service_pb2.\
            GetPendingResourcesOfAllNodegroupsReply()
        if include_pending == "true":
            resource_request = gcs_service_pb2.\
                GetPendingResourcesOfAllNodegroupsRequest()
            resource_reply = await self._gcs_node_resource_info_stub.\
                GetPendingResourcesOfAllNodegroups(resource_request)
        include_quota = req.query.get("include_quota", None)
        quota_reply = gcs_service_pb2.\
            GetAvailableQuotaOfAllNodegroupsReply()
        if include_quota == "true":
            quota_request = gcs_service_pb2.\
                GetAvailableQuotaOfAllNodegroupsRequest()
            quota_reply = await self._gcs_job_info_stub.\
                GetAvailableQuotaOfAllNodegroups(quota_request)
        all_nodegroups_resources = {}
        request = gcs_service_pb2.GetLayeredResourcesOfAllNodegroupsRequest()
        reply = await self._gcs_node_resource_info_stub.\
            GetLayeredResourcesOfAllNodegroups(request)
        if reply.status.code == 0:
            total_unassigned_host_resource_dict = {}
            default_label_total_resource_dict = {}
            default_label_available_resource_dict = {}
            host_list, label_total_resource_dict, \
                label_available_resource_dict, shapes, _ = \
                self._parse_host_resource_map(
                    reply.total_unassigned_hosts.host_resource_map)
            label_resource_list = []
            for resource_label in sorted(label_total_resource_dict):
                label_resource_list.append(
                    dict(
                        resource_name=resource_label,
                        total=label_total_resource_dict[resource_label],
                        available=label_available_resource_dict[
                            resource_label]))
                default_label_total_resource_dict[resource_label] = \
                    default_label_total_resource_dict.\
                    setdefault(resource_label, 0) + \
                    label_total_resource_dict[resource_label]
                default_label_available_resource_dict[resource_label] = \
                    default_label_available_resource_dict.\
                    setdefault(resource_label, 0) + \
                    label_available_resource_dict[resource_label]
            total_unassigned_host_resource_dict = dict(
                resources=label_resource_list, hosts=host_list)
            nodegroup_layer_list = []
            for ns_layer in sorted(reply.nodegroup_layer_map):
                nodegroup_layer = reply.nodegroup_layer_map[ns_layer]
                unassigned_host_resource_dict = {}
                shape_group_to_count_map = {}
                resource_load_list = []
                pg_load_list = []
                quota = []
                if ns_layer in resource_reply.pending_resources:
                    pending_demands = resource_reply.\
                        pending_resources[ns_layer]
                    for demand in pending_demands.\
                            resource_load_by_shape.resource_demands:
                        resources = {}
                        for key in demand.shape:
                            if key == "memory":
                                resources[key] = \
                                    demand.shape[key] / (1024**2)
                            else:
                                resources[key] = demand.shape[key]
                        demand_dict = dict(
                            resources=resources,
                            pending_queue_size=demand.backlog_size)
                        resource_load_list.append(demand_dict)

                    for pg in pending_demands.\
                            placement_group_load.placement_group_data:
                        bundles = []
                        for bundle in pg.bundles:
                            resources = {}
                            for key in bundle.unit_resources:
                                if key == "memory":
                                    resources[key] = \
                                        bundle.unit_resources[key] / (
                                            1024**2)
                                else:
                                    resources[key] = \
                                        bundle.unit_resources[key]
                            bundle_dict = dict(
                                bundle_index=bundle.bundle_id.bundle_index,
                                resources=resources,
                                node_id=bundle.node_id,
                                is_valid=bundle.is_valid)
                            bundles.append(bundle_dict)
                        pg_dict = dict(
                            placement_group_id=pg.placement_group_id,
                            name=pg.name,
                            bundles=bundles,
                            strategy=common_pb2.PlacementStrategy.Name(
                                pg.strategy),
                            state=gcs_pb2.PlacementGroupTableData.
                            PlacementGroupState.Name(pg.state))
                        pg_load_list.append(pg_dict)

                if ns_layer in quota_reply.nodegroup_to_quota_map:
                    nodegroup_quota = quota_reply.nodegroup_to_quota_map[
                        ns_layer]
                    for resource_name, available in (
                            nodegroup_quota.quota.items()):
                        if resource_name == "memory":
                            available /= (1024**2)
                        quota_dict = dict(
                            resource_name=resource_name, available=available)
                        quota.append(quota_dict)

                host_list, label_total_resource_dict,\
                    label_available_resource_dict, cur_shapes, \
                    cur_shape_group_to_count_map = \
                    self._parse_host_resource_map(
                        nodegroup_layer.unassigned_hosts.host_resource_map)
                shapes.update(cur_shapes)
                for key in cur_shape_group_to_count_map:
                    shape_group_to_count_map[key] = \
                        shape_group_to_count_map.get(key, 0) + \
                        cur_shape_group_to_count_map[key]
                label_resource_list = []
                for resource_label in sorted(label_total_resource_dict):
                    label_resource_list.append(
                        dict(
                            resource_name=resource_label,
                            total=label_total_resource_dict[resource_label],
                            available=label_available_resource_dict[
                                resource_label]))
                unassigned_host_resource_dict = dict(
                    resources=label_resource_list, hosts=host_list)
                sub_nodegroup_host_resource_list = []
                # Process the sub-nodegroups
                for ns_id in sorted(
                        nodegroup_layer.nodegroup_host_resource_map):
                    host_resource_map = nodegroup_layer.\
                        nodegroup_host_resource_map[ns_id]
                    host_list, sub_label_total_resource_dict, \
                        sub_label_available_resource_dict, cur_shapes, \
                        cur_shape_group_to_count_map = \
                        self._parse_host_resource_map(
                            host_resource_map.host_resource_map)
                    shapes.update(cur_shapes)
                    for key in cur_shape_group_to_count_map:
                        shape_group_to_count_map[key] = \
                            shape_group_to_count_map.get(key, 0) + \
                            cur_shape_group_to_count_map[key]
                    label_resource_list = []
                    for resource_label in sorted(
                            sub_label_total_resource_dict):
                        resource_dict = dict(
                            resource_name=resource_label,
                            total=sub_label_total_resource_dict[
                                resource_label],
                            available=sub_label_available_resource_dict[
                                resource_label])
                        label_resource_list.append(resource_dict)
                        label_total_resource_dict[
                            resource_label] = \
                            label_total_resource_dict.\
                            setdefault(resource_label, 0) + \
                            sub_label_total_resource_dict[resource_label]
                        label_available_resource_dict[
                            resource_label] = \
                            label_available_resource_dict.\
                            setdefault(resource_label, 0) + \
                            sub_label_available_resource_dict[
                                resource_label]
                    sub_nodegroup_host_resource_list.append(
                        dict(
                            namespace_id=ns_id,
                            resources=label_resource_list,
                            hosts=host_list))
                label_resource_list = []
                for resource_label in sorted(label_total_resource_dict):
                    label_resource_list.append(
                        dict(
                            resource_name=resource_label,
                            total=label_total_resource_dict[resource_label],
                            available=label_available_resource_dict[
                                resource_label]))
                    default_label_total_resource_dict[
                        resource_label] = \
                        default_label_total_resource_dict.\
                        setdefault(resource_label, 0) + \
                        label_total_resource_dict[resource_label]
                    default_label_available_resource_dict[
                        resource_label] = \
                        default_label_available_resource_dict.\
                        setdefault(resource_label, 0) + \
                        label_available_resource_dict[resource_label]
                nodegroup_layer_dict = {}
                if nodegroup_layer.enable_sub_nodegroup_isolation is True:
                    nodegroup_layer_dict = dict(
                        namespace_id=ns_layer,
                        groups=shape_group_to_count_map,
                        resource_load=resource_load_list,
                        placement_group_load=pg_load_list,
                        revision=nodegroup_layer.revision,
                        resources=label_resource_list,
                        quota=quota,
                        unassigned_hosts=unassigned_host_resource_dict,
                        namespaces=sub_nodegroup_host_resource_list)
                else:
                    nodegroup_layer_dict = dict(
                        namespace_id=ns_layer,
                        groups=shape_group_to_count_map,
                        resource_load=resource_load_list,
                        placement_group_load=pg_load_list,
                        revision=nodegroup_layer.revision,
                        resources=label_resource_list,
                        quota=quota,
                        hosts=host_list)
                nodegroup_layer_list.append(nodegroup_layer_dict)

            default_label_resource_list = []
            for resource_label in sorted(default_label_total_resource_dict):
                default_label_resource_list.append(
                    dict(
                        resource_name=resource_label,
                        total=default_label_total_resource_dict[
                            resource_label],
                        available=default_label_available_resource_dict[
                            resource_label]))

            all_nodegroups_resources = dict(
                group_shapes=shapes,
                namespace_id=RAY_DEFAULT_NODEGROUP,
                resources=default_label_resource_list,
                unassigned_hosts=total_unassigned_host_resource_dict,
                namespaces=nodegroup_layer_list)

            return dashboard_utils.rest_response(
                success=True,
                message="Resources of all namespaces fetched.",
                resource_view=all_nodegroups_resources)
        else:
            logger.info("Failed to get resources of all namespaces.")
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to get resources of all namespaces: {}".format(
                    reply.status.message),
                resource_view=all_nodegroups_resources)

    def _build_schedule_options_proto(self, schedule_options):
        resource_weights = schedule_options.get("resourceWeights", {})
        schedule_options_proto = gcs_service_pb2.ScheduleOptions(
            pack_step=float(
                schedule_options.get(
                    "packStep", os.getenv("RAY_GCS_SCHEDULE_PACK_STEP",
                                          "0.0"))),
            rare_resources_schedule_pack_step=float(
                schedule_options.get(
                    "rareResourcesSchedulePackStep",
                    os.getenv("RAY_rare_resources_schedule_pack_step",
                              "1.0"))),
            num_candidate_nodes_for_scheduling=int(
                schedule_options.get(
                    "numCandidateNodesForScheduling",
                    os.getenv("NUM_CANDIDATE_NODES_FOR_SCHEDULING", "3"))),
            resource_weights=dict(resource_weights),
            runtime_resource_scheduling_enabled=str(
                schedule_options.get(
                    "runtimeResourceSchedulingEnabled",
                    os.getenv("RAY_runtime_resource_scheduling_enabled",
                              "true"))).lower() == "true",
            runtime_resources_calculation_interval_s=int(
                schedule_options.get(
                    "runtimeResourcesCalculationIntervalS",
                    os.getenv("RAY_runtime_resources_calculation_interval_s",
                              "600"))),
            runtime_memory_tail_percentile=float(
                schedule_options.get(
                    "runtimeMemoryTailPercentile",
                    os.getenv("RAY_runtime_memory_tail_percentile", "1.0"))),
            runtime_cpu_tail_percentile=float(
                schedule_options.get(
                    "runtimeCPUTailPercentile",
                    os.getenv("RAY_runtime_cpu_tail_percentile", "0.95"))),
            runtime_resources_history_window_len_s=int(
                schedule_options.get(
                    "runtimeResourcesHistoryWindowLenS",
                    os.getenv("RAY_runtime_resources_history_window_len_s",
                              "86400"))),
            runtime_memory_history_window_tail=float(
                schedule_options.get(
                    "runtimeMemoryHistoryWindowTail",
                    os.getenv("RAY_runtime_memory_history_window_tail",
                              "0.99"))),
            runtime_cpu_history_window_tail=float(
                schedule_options.get(
                    "runtimeCPUHistoryWindowTail",
                    os.getenv("RAY_runtime_cpu_history_window_tail", "0.95"))),
            overcommit_ratio=float(
                schedule_options.get("overcommitRatio",
                                     os.getenv("RAY_overcommit_ratio",
                                               "1.5"))),
            node_overcommit_ratio=float(
                schedule_options.get(
                    "nodeOvercommitRatio",
                    os.getenv("RAY_node_overcommit_ratio", "1.5"))),
            job_resource_requirements_max_min_ratio_limit=float(
                schedule_options.get(
                    "jobResourceRequirementsMaxMinRatioLimit",
                    os.getenv(
                        "RAY_job_resource_requirements_max_min_ratio_limit",
                        "10.0"))))
        return schedule_options_proto

    @routes.post("/namespaces")
    async def new_nodegroup(self, req) -> aiohttp.web.Response:
        nodegroup_info_json = await req.json()
        logger.info("POST /namespaces %s", nodegroup_info_json)
        nodegroup_info = dict(nodegroup_info_json)
        nodegroup_id = nodegroup_info["namespaceId"]
        nodegroup_name = nodegroup_info.get("name", "")
        schedule_options_proto = self._build_schedule_options_proto(
            nodegroup_info.get("scheduleOptions", {}))

        shape_and_count_list = []
        shape_and_count = gcs_service_pb2.NodeShapeAndCount(
            node_count=len(nodegroup_info["hostNameList"]))
        shape_and_count_list.append(shape_and_count)

        isolation = str(
            nodegroup_info.get("enableSubNamespaceIsolation",
                               False)).lower() == "true"
        enable_job_quota = str(
            nodegroup_info.get("enableJobQuota",
                               os.getenv("RAY_enable_job_quota",
                                         "true"))).lower() == "true"
        request = gcs_service_pb2.CreateOrUpdateNodegroupRequest(
            nodegroup_id=nodegroup_id,
            nodegroup_name=nodegroup_name,
            enable_sub_nodegroup_isolation=isolation,
            node_shape_and_count_list=shape_and_count_list,
            schedule_options=schedule_options_proto,
            enable_job_quota=enable_job_quota)
        reply = await self._gcs_nodegroup_stub.CreateOrUpdateNodegroup(request)
        if reply.status.code == 0:
            logger.info("Succeeded to add nodegroup %s", nodegroup_id)
            return dashboard_utils.rest_response(
                success=True,
                message="Namespace created or updated.",
                namespace_id=nodegroup_id)
        else:
            logger.info("Failed to create or update nodegroup %s",
                        nodegroup_id)
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to create or udpate namespace: {}".format(
                    reply.status.message),
                namespace_id=nodegroup_id)

    @routes.post("/namespaces_v2")
    async def create_or_update_nodegroup_v2(self, req) -> aiohttp.web.Response:
        nodegroup_info_json = await req.json()
        logger.info("POST /namespaces_v2 %s", nodegroup_info_json)

        nodegroup_info = dict(nodegroup_info_json)
        nodegroup_id = nodegroup_info["namespaceId"]
        nodegroup_name = nodegroup_info.get("name", "")
        schedule_options_proto = self._build_schedule_options_proto(
            nodegroup_info.get("scheduleOptions", {}))

        shape_and_count_list = []
        for data in nodegroup_info["nodeShapeAndCountList"]:
            node_shape_pb = gcs_pb2.NodeShape(
                shape_group=data.get("group", ""),
                resource_shape=data.get("nodeShape", {}))
            shape_and_count = gcs_service_pb2.NodeShapeAndCount(
                node_count=data["nodeCount"], node_shape=node_shape_pb)
            shape_and_count_list.append(shape_and_count)

        isolation = str(
            nodegroup_info.get("enableSubNamespaceIsolation",
                               False)).lower() == "true"
        enable_job_quota = str(
            nodegroup_info.get("enableJobQuota",
                               os.getenv("RAY_enable_job_quota",
                                         "true"))).lower() == "true"
        request = gcs_service_pb2.CreateOrUpdateNodegroupRequest(
            nodegroup_id=nodegroup_id,
            nodegroup_name=nodegroup_name,
            enable_sub_nodegroup_isolation=isolation,
            node_shape_and_count_list=shape_and_count_list,
            schedule_options=schedule_options_proto,
            enable_job_quota=enable_job_quota)
        reply = await self._gcs_nodegroup_stub.CreateOrUpdateNodegroup(request)
        if reply.status.code == 0:
            logger.info("Succeeded to add nodegroup %s", nodegroup_id)
            data = dashboard_utils.message_to_dict(
                reply, including_default_value_fields=True)
            return dashboard_utils.rest_response(
                success=True,
                message="Namespace created or updated.",
                namespace_id=nodegroup_id,
                host_to_shape=data["hostToNodeSpec"])
        else:
            logger.info("Failed to create or update nodegroup %s",
                        nodegroup_id)
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to create or update namespace: {}".format(
                    reply.status.message),
                namespace_id=nodegroup_id)

    @routes.delete("/namespaces/{namespace_id}")
    async def remove_nodegroup(self, req) -> aiohttp.web.Response:
        """ If the namespace_id contains a special string (e.g. /),
        the DELETE may be failed. This is to compatible with antc,
        antc can't create a DELETE request with body.
        """
        nodegroup_id = req.match_info.get("namespace_id")
        request = gcs_service_pb2.RemoveNodegroupRequest(
            nodegroup_id=nodegroup_id)
        reply = await self._gcs_nodegroup_stub.RemoveNodegroup(request)
        if reply.status.code == 0:
            logger.info("Succeeded to remove namespace %s", nodegroup_id)
            return dashboard_utils.rest_response(
                success=True,
                message=f"Succeeded to remove namespace {nodegroup_id}.",
                namespace_id=nodegroup_id)
        else:
            logger.info("Failed to remove namespace %s", nodegroup_id)
            return dashboard_utils.rest_response(
                success=False,
                message=f"Failed to remove namespace {nodegroup_id}, "
                f"{reply.status.message}",
                namespace_id=nodegroup_id)

    @routes.put("/namespaces/release_idle_nodes/{namespace_id}")
    async def release_idle_nodes(self, req) -> aiohttp.web.Response:
        """ If the namespace_id contains a special string (e.g. /),
        the DELETE may be failed. This is to compatible with antc,
        antc can't create a DELETE request with body.
        """
        nodegroup_id = req.match_info.get("namespace_id")
        if nodegroup_id == "default":
            nodegroup_id = RAY_DEFAULT_NODEGROUP
        request = gcs_service_pb2.ReleaseIdleNodesRequest(
            nodegroup_id=nodegroup_id)
        reply = await self._gcs_nodegroup_stub.ReleaseIdleNodes(request)
        if reply.status.code == 0:
            data = dashboard_utils.message_to_dict(
                reply, {}, including_default_value_fields=True)
            logger.info(
                "Succeeded in releasing idle nodes from nodegroup %s,\
                reply: %s", nodegroup_id, data)
            return dashboard_utils.rest_response(
                success=True,
                message=f"Succeeded in releasing idle nodes from namespace: "
                f"{nodegroup_id}",
                namespace_id=nodegroup_id,
                released_node_shape_and_count_list=data[
                    "releasedNodeShapeAndCountList"],
                remaining_node_shape_and_count_list=data[
                    "remainingNodeShapeAndCountList"])
        else:
            logger.info(
                "Failed to release idle nodes from nodegroup %s,\
                reason: %s", nodegroup_id, reply.status.message)
            return dashboard_utils.rest_response(
                success=False,
                message=f"Failed to release idle nodes from namespace "
                f"{nodegroup_id}: {reply.status.message}",
                namespace_id=nodegroup_id)

    @routes.post("/jobs")
    @dashboard_utils.check_body_signature()
    async def new_job(self, req) -> aiohttp.web.Response:
        return await self._internal_new_job(req, job_id=None)

    async def _internal_new_job(self, req,
                                job_id=None) -> aiohttp.web.Response:
        job_info = dict(await req.json())
        logger.info("Submit job: %s", repr(job_info))
        language = common_pb2.Language.Value(job_info["language"])
        if job_id is None:
            job_id = await self._next_job_id()
        min_resource_requirements = {}
        max_resource_requirements = {}
        if "totalMemoryMb" in job_info:
            min_resource_requirements["totalMemoryMb"] = job_info[
                "totalMemoryMb"]
        else:
            job_config = ray.job_config.JobConfig()
            min_resource_requirements[
                "totalMemoryMb"] = job_config.total_memory_mb
        if "maxTotalMemoryMb" in job_info:
            max_resource_requirements["maxTotalMemoryMb"] = \
                job_info["maxTotalMemoryMb"]
        else:
            max_resource_requirements["maxTotalMemoryMb"] = \
                min_resource_requirements["totalMemoryMb"]
        # Support job sub nodegroup for test
        job_env = job_info.get("env", {})
        node_count = job_env.get("JOB_NODE_COUNT", 0)
        shape_group = job_env.get("JOB_SHAPE_GROUP", "")
        resource_shape = job_env.get("JOB_NODE_SHAPE", {})
        warm_up_download = str(
            job_info.get(
                "preInitializeJobRuntimeEnvEnabled",
                job_env.get("PRE_INITIALIZE_JOB_RUNTIME_ENV_ENABLED",
                            "true"))).lower() == "true"
        node_shape_and_count_list = []
        if node_count != 0:
            node_shape_pb = gcs_pb2.NodeShape(
                shape_group=shape_group, resource_shape=resource_shape)
            node_shape_and_count_list.append(
                gcs_service_pb2.NodeShapeAndCount(
                    node_count=node_count, node_shape=node_shape_pb))
        request = gcs_service_pb2.SubmitJobRequest(
            job_id=job_id.binary(),
            job_name=job_info["name"],
            language=language,
            nodegroup_id=job_info["namespaceId"],
            job_payload=json.dumps(job_info),
            min_resource_requirements=min_resource_requirements,
            max_resource_requirements=max_resource_requirements,
            node_shape_and_count_list=node_shape_and_count_list,
            pre_initialize_job_runtime_env_enabled=warm_up_download)
        reply = await self._gcs_job_info_stub.SubmitJob(request)
        if reply.status.code == 0:
            logger.info("Succeeded to submit job %s", job_id.hex())
            return dashboard_utils.rest_response(
                success=True, message="Job submitted.", job_id=job_id.hex())
        else:
            logger.info("Failed to submit job %s: %s", job_id.hex(),
                        reply.status.message)
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to submit job: {}".format(
                    reply.status.message),
                job_id=job_id.hex(),
                conflict_job_id=binary_to_hex(reply.conflict_job_id))

    @routes.delete("/jobs/{job_id}")
    @dashboard_utils.check_body_signature()
    async def drop_job(self, req) -> aiohttp.web.Response:
        job_id = req.match_info.get("job_id")
        request = gcs_service_pb2.DropJobRequest(job_id=hex_to_binary(job_id))
        reply = await self._gcs_job_info_stub.DropJob(request)
        if reply.status.code == 0:
            logger.info("Succeeded to drop job %s", job_id)
            return dashboard_utils.rest_response(
                success=True, message="Job dropped.", job_id=job_id)
        else:
            logger.info("Failed to drop job %s", job_id)
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to drop job: {}".format(reply.status.message),
                job_id=job_id)

    async def _flush_get_all_jobs_cache(self, change):
        if change.new:
            job_id, new_job_table_data = change.new
            if change.old:
                job_id, old_job_table_data = change.old
                if old_job_table_data["state"] == new_job_table_data["state"]:
                    return
            logger.info("_flush_get_all_jobs_cache %s %s", job_id,
                        len(self.get_all_jobs.cache))
            # Clear all jobs cache.
            self.get_all_jobs.cache.clear()

    @routes.get("/jobs")
    @dashboard_utils.aiohttp_cache
    async def get_all_jobs(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        state = req.query.get("state")
        exclude_inactive = req.query.get("exclude-inactive") == "true"
        job_data_source = DataSource.jobs.values()
        if state is not None:
            available_states = gcs_pb2.JobTableData.JobState.keys()
            if state not in available_states:
                raise ValueError(f"Invalid state {state}, "
                                 f"available values: {available_states}")
            if state in INACTIVE_JOB_STATES:
                job_data_source = DataSource.inactive_jobs.values()
        elif not exclude_inactive:
            job_data_source = chain(DataSource.jobs.values(),
                                    DataSource.inactive_jobs.values())

        def get_job_summary_view(job_info):
            summary_keys = ("state", "jobId", "name", "owner", "language",
                            "driverEntry", "timestamp", "namespaceId",
                            "driverIpAddress", "driverHostname", "jobName",
                            "isDead", "driverPid", "startTime", "nodegroupId")
            summary_job = {k: job_info[k] for k in summary_keys}
            summary_job["config"] = \
                {"longRunning": job_info["config"]["longRunning"]}
            return summary_job

        def get_job_state_view(job_info):
            view_keys = ("jobId", "name", "state", "timestamp", "startTime",
                         "endTime")
            return {k: job_info[k] for k in view_keys}

        if view == "summary":
            return dashboard_utils.rest_response(
                success=True,
                message="All job summary fetched.",
                summary=[
                    get_job_summary_view(v) for v in job_data_source
                    if state is None or v["state"] == state
                ])
        elif view == "state":
            all_job_state = [
                get_job_state_view(v) for v in job_data_source
                if state is None or v["state"] == state
            ]
            return dashboard_utils.rest_response(
                success=True,
                message="All job states fetched.",
                states=all_job_state)
        else:
            return dashboard_utils.rest_response(
                success=False, message="Unknown view {}".format(view))

    async def _flush_get_job_cache(self, change):
        if change.new:
            job_id, new_job_table_data = change.new
            if change.old:
                job_id, old_job_table_data = change.old
                if old_job_table_data["state"] == new_job_table_data["state"]:
                    return
            logger.info("_flush_get_job_cache %s", job_id)
            token = dashboard_utils.ClusterTokenManager.cluster_unique_token()
            query_token = ["", f"clusterToken={token}"] if token else [""]
            query = ["", "view=state"]
            # Clear cache by key.
            keys = set()
            for q in query:
                for t in query_token:
                    keys.add(f"/jobs/{job_id}?{q}&{t}".strip("&?"))
                    keys.add(f"/jobs/{job_id}?{t}&{q}".strip("&?"))
            for key in sorted(keys):
                value = self.get_job.cache.pop(key, None)
                if value is not None:
                    logger.info("_flush_get_job_cache %s %s", key, value)

    @routes.get("/jobs/{job_id}")
    @dashboard_utils.aiohttp_cache(
        exclude_filter=lambda req: req.query.get("view") == "result")
    async def get_job(self, req) -> aiohttp.web.Response:
        job_id = req.match_info.get("job_id")
        view = req.query.get("view")
        if view is None:
            job_detail = {
                "jobInfo": DataSource.jobs.get(
                    job_id, DataSource.inactive_jobs.get(job_id, {})),
                "jobActors": DataSource.job_actors.get(job_id, {}),
                "jobWorkers": DataSource.job_workers.get(job_id, []),
            }
            await GlobalSignals.job_info_fetched.send(job_detail)
            return dashboard_utils.rest_response(
                success=True, message="Job detail fetched.", detail=job_detail)
        elif view == "summary":
            job_info = {
                "jobInfo": DataSource.jobs.get(
                    job_id, DataSource.inactive_jobs.get(job_id, {})),
            }
            await GlobalSignals.job_info_fetched.send(job_info)
            return dashboard_utils.rest_response(
                success=True, message="Job summary fetched.", summary=job_info)
        elif view == "state":
            job_info = DataSource.jobs.get(
                job_id, DataSource.inactive_jobs.get(job_id, {}))
            job_state = {
                k: job_info[k]
                for k in ("jobId", "name", "state", "startTime", "endTime")
                if k in job_info
            }
            return dashboard_utils.rest_response(
                success=True, message="Job state fetched.", **job_state)
        elif view == "result":
            assert ray.JobID(hex_to_binary(job_id))
            request = gcs_service_pb2.GetJobDataRequest(
                job_id=hex_to_binary(job_id),
                key=ray_constants.JOB_DATA_KEY_RESULT)
            reply = await self._gcs_job_info_stub.GetJobData(request)
            if reply.status.code == 0:
                job_result = reply.data.decode()
                try:
                    job_result = json.loads(job_result)
                except Exception:
                    pass
                return dashboard_utils.rest_response(
                    success=True,
                    message="Job result fetched",
                    result=job_result)
            else:
                raise Exception(
                    f"Failed to GetJobData: {reply.status.message}")
        else:
            return dashboard_utils.rest_response(
                success=False, message="Unknown view {}".format(view))

    async def debug(self):
        import socket
        from aiohttp import hdrs

        nodegroup_id = "default namespace"

        class MockRequest:
            query = {}

            async def json(self):
                return {
                    "namespaceId": nodegroup_id,
                    "hostNameList": [socket.gethostname()]
                }

        resp = await self.new_nodegroup(MockRequest())
        logger.info(resp.body)

        class MockRequest:
            method = hdrs.METH_GET
            path_qs = ""
            query = {}

        resp = await self.get_all_nodegroups(MockRequest())
        logger.info(resp.body)

        class MockRequest:
            query = {}

            async def json(self):
                return {
                    "name": "rayag_darknet",
                    "owner": "abc.xyz",
                    "language": "PYTHON",
                    "namespaceId": nodegroup_id,
                    "metricTemplate": "123",
                    "url": "http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/po/simple_job.zip",  # noqa: E501
                    "driverEntry": "simple_job",
                    "driverArgs": [],
                    "customConfig": {
                        "k1": "v1",
                        "k2": "v2"
                    },
                    "virtualenvCache": True,
                    "jvmOptions": ["-Dabc=123", "-Daaa=xxx"],
                    "dependencies": {
                        "python": [
                            "aiohttp", "click", "colorama", "filelock",
                            "google", "grpcio", "jsonschema",
                            "msgpack >= 0.6.0, < 1.0.0", "numpy >= 1.16",
                            "protobuf >= 3.8.0", "py-spy >= 0.2.0", "pyyaml",
                            "redis >= 3.3.2"
                        ],
                        "java": [{
                            "name": "rayag",
                            "version": "2.1",
                            "url": "http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/po/commons-io-2.5.jar",  # noqa: E501
                            "md5": ""
                        }]
                    }
                }

        resp = await self.new_job(MockRequest())
        logger.info(resp.body)

        class MockRequest:
            query = {}

            async def json(self):
                return {
                    "name": "rayag_darknet",
                    "owner": "abc.xyz",
                    "language": "JAVA",
                    "namespaceId": nodegroup_id,
                    "url": "http://alipay-kepler-resource.cn-hangzhou.alipay.aliyun-inc.com/ALIPW3CN_dev/antc/68002409/ICs6O0FM/helloworldtest.zip",  # noqa: E501
                    "driverEntry": "helloworldtestOnRay",
                    "driverArgs": [],
                    "customConfig": {
                        "k1": "v1",
                        "k2": "v2"
                    },
                    "jvmOptions": ["-Dabc=123", "-Daaa=xxx"],
                    "dependencies": {
                        "python": [
                            "aiohttp", "click", "colorama", "filelock",
                            "google", "grpcio", "jsonschema",
                            "msgpack >= 0.6.0, < 1.0.0", "numpy >= 1.16",
                            "protobuf >= 3.8.0", "py-spy >= 0.2.0", "pyyaml",
                            "redis >= 3.3.2"
                        ],
                        "java": [{
                            "name": "rayag",
                            "version": "2.1",
                            "url": "http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/po/commons-io-2.5.jar",  # noqa: E501
                            "md5": ""
                        }]
                    }
                }

        resp = await self.new_job(MockRequest())
        logger.info(resp.body)

    async def run(self, server):
        self._gcs_nodegroup_stub = gcs_service_pb2_grpc.\
            NodegroupInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_node_resource_info_stub = gcs_service_pb2_grpc.\
            NodeResourceInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)

        # This is used for test only. We disabled automatically submit system
        # jobs to test if it works well about the system jobs related APIs.
        if os.environ.get("DISABLE_AUTO_SUBMIT_SYSTEM_JOBS") != "1":
            logger.info("DISABLE_AUTO_SUBMIT_SYSTEM_JOBS is true")
            await self.__internal_submit_system_jobs_if_confied_v2(False)

        if DEBUG:
            await self.debug()

    # ANT-INTERNAL
    # Public HTTP accessing API to submit the system jobs from users end
    # manually.
    @routes.post("/system_jobs/submit_system_jobs_v2")
    async def submit_system_jobs_v2(self, req) -> aiohttp.web.Response:
        return await self.__internal_submit_system_jobs_if_confied_v2(True)

    async def __internal_submit_system_jobs_if_confied_v2(
            self, is_resubmitted_from_http) -> aiohttp.web.Response:
        # The steps of submitting system jobs are:
        #
        # 1. Parsing the system jobs config from ray_config_file.
        # 2. Try to creating SYSTEM_NAMESPACE node group. If the group exists,
        #    update it.
        # 3. If failed to create SYSTEM_NAMESPACE, reply a error with message.
        # 4. If succeeded to create SYSTEM_NAMESPACE, try to submit all system
        #    jobs.
        # 5. Reply the result of submitting system jobs to end users.
        #
        # Note that this HTTP service doesn't gurantee anything, including both
        # creating namespace and the number of successful system jobs.
        CLUSTER_CONFIG_URL = os.environ.get("ray_config_file")
        if CLUSTER_CONFIG_URL is None or len(CLUSTER_CONFIG_URL) == 0:
            logger.info("[SYSTEM_JOB_V2] `ray_config_file` is not found.")
            return dashboard_utils.rest_response(
                success=False, message="ray_config_file is not found.")

        assert CLUSTER_CONFIG_URL is not None
        cluster_conf = await load_cluster_config_from_url(CLUSTER_CONFIG_URL)
        if "system_jobs" not in cluster_conf:
            return dashboard_utils.rest_response(
                success=False,
                message="`system_jobs` config item is not found.")

        logger.info("[SYSTEM_JOB_V2] Parsing system jobs config file.")
        system_jobs_str = cluster_conf["system_jobs"]
        system_jobs_json = json.loads(system_jobs_str)
        logger.info("[SYSTEM_JOB_V2] All system jobs are %s", system_jobs_json)

        ok = await self.__create_nodegroup_for_system_job_v2(system_jobs_json)
        if not ok:
            # This could happen when the required resources are not ready
            # for these system jobs.
            return dashboard_utils.rest_response(
                success=False, message="Failed to create SYSTEM_NAMESPACE.")
        logger.info("[SYSTEM_JOB_V2] Succeeded to create SYSTEM_NAMESPACE.")

        ok = await self.__submit_system_jobs_v2(system_jobs_json,
                                                is_resubmitted_from_http)
        if not ok:
            logger.info("[SYSTEM_JOB_V2] Failed to submit all system jobs.")
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to submit system jobs. check it in dashboard. "
                + "Because we don't promise the number of succeeded jobs.")
        logger.info("[SYSTEM_JOB_V2] Succeeded to submit all system jobs.")
        return dashboard_utils.rest_response(
            success=True, message="Succeeded to submit all system jobs.")

    async def __create_nodegroup_for_system_job_v2(self,
                                                   system_jobs_json,
                                                   timeout_seconds=180):
        # Required node shapes for creating system namespace for system jobs.
        node_shape_list = system_jobs_json["system_namespace"]

        # Arguments Validation.
        assert isinstance(node_shape_list, list)
        assert len(node_shape_list) > 0
        logger.info(
            "[SYSTEM_JOB_V2] Creating nodegroup for system jobs with %s ",
            node_shape_list)
        for node_shape in node_shape_list:
            assert "group" in node_shape
            assert "nodeCount" in node_shape

        class CreateSystemNodegroupRequest:
            query = {}

            async def json(self):
                return {
                    "namespaceId": "SYSTEM_NAMESPACE",
                    "enableSubNamespaceIsolation": False,
                    "nodeShapeAndCountList": node_shape_list
                }

        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            resp = await self.create_or_update_nodegroup_v2(
                CreateSystemNodegroupRequest())
            result = dashboard_utils.response_to_dict(resp)
            if result["result"]:
                logger.info(
                    "[SYSTEM_JOB_V2] Succeeded to create SYSTEM_NAMESPACE")
                return True
            await asyncio.sleep(1)
        logger.info("[SYSTEM_JOB_V2] Failed to create SYSTEM_NAMESPACE : %s",
                    result)
        return False

    # Internally submit_system_job, which is not exported as HTTP accessing.
    async def __submit_system_jobs_v2(self,
                                      system_jobs_json,
                                      is_resubmitted_from_http,
                                      timeout_seconds=300):
        assert "jobs" in system_jobs_json
        jobs_conf_json = system_jobs_json["jobs"]
        job_id_to_reserve = 1
        logger.info("[SYSTEM_JOB_V2] Submitting %d system jobs.",
                    len(jobs_conf_json))
        for job_conf_name in jobs_conf_json:
            job_conf_json = jobs_conf_json[job_conf_name]
            system_job_conf = SystemJobConf.create(
                job_id=job_id_to_reserve,
                job_url=job_conf_json["url"],
                job_name=job_conf_name,
                driver_entry=job_conf_json["driver_entry"],
                total_memory_mb=get_field_from_json(job_conf_json,
                                                    "total_memory_mb", 1800),
            )
            ok = await self.__do_submit_one_system_job_v2(
                system_job_conf, is_resubmitted_from_http)
            if not ok:
                logger.info("[SYSTEM_JOB_V2] Failed to submit job %s",
                            system_job_conf)
                return False
            job_id_to_reserve += 1
        return True

    async def __do_submit_one_system_job_v2(self,
                                            system_job_conf,
                                            is_resubmitted_from_http,
                                            timeout_seconds=60):
        class SubmitSystemJobV2Request:
            query = {}

            async def json(self):
                return {
                    "name": "SYSTEM_JOB_" + system_job_conf.job_name,
                    "owner": "SystemJobs Team",
                    "language": "JAVA",
                    "namespaceId": "SYSTEM_NAMESPACE",
                    "metricTemplate": "system-actors",
                    "url": system_job_conf.job_url,
                    "driverEntry": system_job_conf.driver_entry,
                    "driverArgs": [],
                    "virtualenvCache": True,
                    "totalMemoryMb": system_job_conf.total_memory_mb
                }

        logger.info("Submitting system job: %s", system_job_conf)
        # If this system job is been submitting from HTTP, we'll use an
        # auto-increasing job id.
        if is_resubmitted_from_http:
            system_job_id = await self._next_job_id()
        else:
            system_job_id = await self._reserve_job_id(system_job_conf.job_id)
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            resp = await self._internal_new_job(
                SubmitSystemJobV2Request(), job_id=system_job_id)
            result = dashboard_utils.response_to_dict(resp)
            if result["result"]:
                logger.info(
                    "[SYSTEM_JOB_V2] Succeeded to submit system job %s.",
                    system_job_conf.job_name)
                return True
            elif "job id is conflict" in result["msg"]:
                # TODO: This should be refined as conflict exception.
                logger.info("Skip because it has been submitted before.")
                return True
            logger.info("[SYSTEM_JOB_V2] Failed to submit system job (%s): %s",
                        system_job_conf.job_name, result)
            await asyncio.sleep(3)
        return False
