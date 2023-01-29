import asyncio
import logging
import operator
import time
import heapq

import aiohttp.web
import grpc.aio as aiogrpc
import ray.gcs_utils
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import (
    DataSource,
    DataOrganizer,
    GlobalSignals,
)
from ray.new_dashboard.modules.actor import actor_consts
from ray.new_dashboard.modules.actor import actor_utils
from ray.new_dashboard.modules.actor.actor_utils import (
    actor_classname_from_function_descriptor, )

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class ActorGroupBy:
    @staticmethod
    async def _do(actor_list, op):
        tmp_job_actors = {}
        tmp_node_actors = {}

        for idx, actor_table_data in enumerate(actor_list):
            if idx % actor_consts.PROCESS_BATCH_COUNT == 0:
                await asyncio.sleep(0)
            try:
                actor_id = actor_table_data["actorId"]
                job_id = actor_table_data["jobId"]
                node_id = actor_table_data["address"]["rayletId"]
                tmp_job_actors.setdefault(job_id,
                                          {})[actor_id] = actor_table_data
                tmp_node_actors.setdefault(node_id,
                                           {})[actor_id] = actor_table_data
            except Exception:
                logger.exception("Update actor groupby failed.")

        if tmp_job_actors:
            await asyncio.sleep(0)
            for job_id, actors in tmp_job_actors.items():
                DataSource.job_actors.setdefault(job_id, {})
                job_actors = DataSource.job_actors.get(job_id)
                op(job_actors, actors)

        if tmp_node_actors:
            await asyncio.sleep(0)
            for node_id, actors in tmp_node_actors.items():
                DataSource.node_actors.setdefault(node_id, {})
                node_actors = DataSource.node_actors.get(node_id)
                op(node_actors, actors)

    @classmethod
    async def update(cls, actor_list):
        await cls._do(actor_list,
                      lambda target, actors: target.mutable().update(actors))

    @classmethod
    async def delete(cls, actor_list):
        def _delete(target, actors):
            mutable_target = target.mutable()
            for actor_id in actors.keys():
                mutable_target.pop(actor_id, None)

        await cls._do(actor_list, _delete)

    @classmethod
    def clear(cls):
        DataSource.job_actors.reset({})
        DataSource.node_actors.reset({})


class ActorUpdater(dashboard_utils.PSubscribeUpdater):
    @staticmethod
    def _actor_table_data_to_dict(message):
        data = dashboard_utils.message_to_dict(
            message, {
                "actorId", "parentId", "jobId", "workerId", "rayletId",
                "actorCreationDummyObjectId", "callerId", "taskId",
                "parentTaskId", "sourceActorId", "placementGroupId"
            },
            including_default_value_fields=True)
        actor_class = actor_classname_from_function_descriptor(
            data.get("functionDescriptor", {}))
        data["actorClass"] = actor_class
        return data

    @staticmethod
    async def _filter_actor_messages(actor_messages):
        actors = []
        dead_actors = []
        for idx, message in enumerate(actor_messages):
            if idx % actor_consts.PROCESS_BATCH_COUNT == 0:
                await asyncio.sleep(0)
            if message.state == gcs_pb2.ActorTableData.ActorState.DEAD:
                dead_actors.append(message)
            else:
                actors.append(message)
        await asyncio.sleep(0)
        dead_actors = heapq.nlargest(
            actor_consts.TOTAL_DEAD_ACTOR_COUNT_LIMIT,
            dead_actors,
            key=operator.attrgetter("timestamp"))
        return actors, dead_actors

    @classmethod
    async def _messages_to_dict(cls, actor_messages):
        actors = {}
        for idx, message in enumerate(actor_messages):
            if idx % actor_consts.PROCESS_BATCH_COUNT == 0:
                await asyncio.sleep(0)
            actor_table_data = cls._actor_table_data_to_dict(message)
            actors[actor_table_data["actorId"]] = actor_table_data
        return actors

    @classmethod
    async def _update_all(cls, gcs_channel):
        gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            gcs_channel)
        # Get all actor info.
        while True:
            try:
                logger.info("Getting all actor info from GCS.")
                request = gcs_service_pb2.GetAllActorInfoRequest()
                reply = await gcs_actor_info_stub.GetAllActorInfo(
                    request, timeout=120)
                if reply.status.code == 0:
                    logger.info("Received %s actor info from GCS.",
                                len(reply.actor_table_data))
                    await asyncio.sleep(0)
                    start_time = time.time()
                    actors, dead_actors = await cls._filter_actor_messages(
                        reply.actor_table_data)
                    actors = await cls._messages_to_dict(actors)
                    dead_actors = await cls._messages_to_dict(dead_actors)
                    for dead_actor in dead_actors.values():
                        dead_actor["deathCause"] = cls._digest_death_cause(
                            dead_actor.get("deathCause", {}))
                    # Update actors.
                    DataSource.actors.reset(actors)
                    DataSource.dead_actors.reset(dead_actors)
                    logger.info("Updated %d actor info from GCS cost %ss.",
                                len(actors) + len(dead_actors),
                                time.time() - start_time)
                    ActorGroupBy.clear()
                    await ActorGroupBy.update(actors.values())
                    await ActorGroupBy.update(dead_actors.values())
                    logger.info("Update actor groupby finished.")
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllActorInfo: {reply.status.message}")
            except Exception:
                logger.exception("Error Getting all actor info from GCS.")
                await asyncio.sleep(
                    actor_consts.RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS)

    @staticmethod
    def _filter(actor_table_data, current_timestamp_ms, ttl_ms):
        # The DEAD actor will not be restarted.
        if actor_table_data["state"] != "DEAD":
            return False

        # Check ttl
        delta = current_timestamp_ms - actor_table_data["timestamp"]
        if delta < ttl_ms:
            return False
        return True

    @classmethod
    async def _filter_actors(cls, actor_filter_timestamp):
        current_timestamp = time.time()
        if actor_filter_timestamp and \
           current_timestamp - actor_filter_timestamp < \
                actor_consts.ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS:
            return actor_filter_timestamp

        current_timestamp_ms = current_timestamp * 1000
        ttl_ms = actor_consts.DEAD_ACTOR_TTL_SECONDS * 1000
        filtered_actors = []
        for actor_id, actor_table_data in \
                DataSource.dead_actors.mutable().items():
            if cls._filter(actor_table_data, current_timestamp_ms, ttl_ms):
                filtered_actors.append(actor_id)

        removed_actors = []
        for actor_id in filtered_actors:
            removed_actor = DataSource.dead_actors.pop(actor_id, None)
            if removed_actor is not None:
                removed_actors.append(removed_actor)
        await ActorGroupBy.delete(removed_actors)

        if (len(DataSource.dead_actors) >
                actor_consts.TOTAL_DEAD_ACTOR_COUNT_LIMIT):
            purge_count = (len(DataSource.dead_actors) -
                           actor_consts.TOTAL_DEAD_ACTOR_COUNT_LIMIT)
            logger.warning(
                "Too many actor table data in the dashboard, "
                "purging %s actors to keep the count up to %s", purge_count,
                actor_consts.TOTAL_DEAD_ACTOR_COUNT_LIMIT)
            purge_actors = heapq.nsmallest(
                purge_count,
                DataSource.dead_actors.mutable().values(),
                operator.itemgetter("timestamp"))
            removed_actors = []
            for actor in purge_actors:
                removed_actor = DataSource.dead_actors.pop(
                    actor["actorId"], None)
                if removed_actor is not None:
                    removed_actors.append(removed_actor)
            await ActorGroupBy.delete(removed_actors)

        return current_timestamp

    @classmethod
    def _digest_death_cause(cls, death_cause: dict):

        # Actor Scheduling or creation failutre
        if "creationTaskFailureContext" in death_cause:
            death_cause["keyword"] = "CREATION EXCEPTION"
            death_cause["message"] = death_cause["creationTaskFailureContext"][
                "formattedExceptionString"]
            return death_cause
        if "runtimeEnvFailedContext" in death_cause:
            death_cause["keyword"] = "RUNTIME ENV"
            death_cause["message"] = death_cause["runtimeEnvFailedContext"][
                "errorMessage"]
            return death_cause
        if "actorUnschedulableContext" in death_cause:
            error_message = death_cause["actorUnschedulableContext"][
                "errorMessage"]
            if "The bundle" in error_message:
                death_cause["keyword"] = "BUNDLE REMOVED"
            elif "placement group" in error_message:
                death_cause["keyword"] = "PG REMOVED"
            else:
                death_cause["keyword"] = "SCHEDULING FAILED"
            death_cause["message"] = error_message
            return death_cause

        # Actor runtime failure
        keyword = "UNKOWN"
        if "actorDiedErrorContext" not in death_cause:
            death_cause["keyword"] = keyword
            return death_cause
        error_message = death_cause["actorDiedErrorContext"].get(
            "errorMessage", "")
        if "all references to the actor were removed" in error_message:
            keyword = "NORMAL"
        elif "owner has died" in error_message:
            keyword = "OWNER DIED"
        elif "worker process has died" in error_message:
            keyword = "WORKER DIED"
        elif "node has died" in error_message:
            keyword = "NODE DIED"
        elif "killed by `ray.kill`" in error_message:
            keyword = "RAY_KILLED"
        elif "job has dead" in error_message:
            keyword = "JOB DIED"
        death_cause["keyword"] = keyword
        death_cause["message"] = error_message
        return death_cause

    @classmethod
    async def _update_one(cls, actor_id, actor_table_data, state_keys):
        if actor_table_data["state"] == "DEPENDENCIES_UNREADY":
            actor_id = actor_table_data["actorId"]
            DataSource.actors[actor_id] = actor_table_data
            await ActorGroupBy.update([actor_table_data])
        else:
            # If actor is not new registered but updated,
            # we only update states related fields.
            actor_id = actor_id.decode("UTF-8")[len(
                ray.gcs_utils.TablePrefix_ACTOR_string + ":"):]
            # Get existing actor table data.
            existing_actor_table_data = (DataSource.actors.get(actor_id) or
                                         DataSource.dead_actors.get(actor_id))
            if existing_actor_table_data is None:
                logger.warning("Discard pub-sub partial actor table data: %s",
                               actor_table_data)
                return
            # Update partial actor table data to existing data.
            actor_table_data_copy = dict(existing_actor_table_data)
            for k in state_keys:
                actor_table_data_copy[k] = actor_table_data[k]
            actor_table_data = actor_table_data_copy
            # Set data according to state.
            if actor_table_data["state"] == "DEAD":
                DataSource.actors.pop(actor_id, None)
                DataSource.dead_actors[actor_id] = actor_table_data
                # Modify death cause for frontend display
                actor_table_data["deathCause"] = cls._digest_death_cause(
                    actor_table_data.get("deathCause", {}))
                await ActorGroupBy.update([actor_table_data])
            # TODO(fyrestone): Fix it by adding a timestamp to
            # ActorTableData.
            elif actor_id not in DataSource.dead_actors:
                DataSource.actors[actor_id] = actor_table_data
                await ActorGroupBy.update([actor_table_data])

    @classmethod
    async def _update_from_psubscribe_channel(cls, receiver):
        # Receive actors from channel.
        actor_filter_timestamp = None
        state_keys = ("state", "address", "numRestarts", "timestamp", "pid",
                      "deathCause")
        async for data in receiver.iter():
            try:
                # data["channel"] is b'ACTOR:00d03a7a9944aa35478559f201000000'
                actor_id = data["channel"]
                actor_table_data = data["data"]
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(
                    actor_table_data)
                message = ray.gcs_utils.ActorTableData.FromString(
                    pubsub_message.data)
                actor_table_data = cls._actor_table_data_to_dict(message)
                await cls._update_one(actor_id, actor_table_data, state_keys)
                actor_filter_timestamp = cls._filter_actors(
                    actor_filter_timestamp)
            except Exception:
                logger.exception("Error receiving actor info.")


class ActorHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        logger.info(
            "The actor module config: total actors count limit %s, "
            "dead actor ttl seconds %s, "
            "actor filter minimum interval seconds %s.",
            actor_consts.TOTAL_DEAD_ACTOR_COUNT_LIMIT,
            actor_consts.DEAD_ACTOR_TTL_SECONDS,
            actor_consts.ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS)
        GlobalSignals.gcs_reconnected.append(
            ActorUpdater.create(
                f"{actor_consts.ACTOR_CHANNEL}:*",
                self._dashboard_head.aioredis_client,
                self._dashboard_head.aiogrpc_gcs_channel).restart)

    @routes.get("/logical/actor_groups")
    async def get_actor_groups(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_all_actors()
        actor_creation_tasks = await DataOrganizer.get_actor_creation_tasks()
        # actor_creation_tasks have some common interface with actors,
        # and they get processed and shown in tandem in the logical view
        # hence we merge them together before constructing actor groups.
        actors.update(actor_creation_tasks)
        actor_groups = actor_utils.construct_actor_groups(actors)
        return dashboard_utils.rest_response(
            success=True,
            message="Fetched actor groups.",
            actor_groups=actor_groups)

    @routes.get("/actors")
    @dashboard_utils.aiohttp_cache
    async def get_all_actors2(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="All actors fetched.",
            actors=DataSource.actors.mutable())

    @routes.get("/logical/actors")
    @dashboard_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="All actors fetched.",
            actors=dict(DataSource.actors.mutable(),
                        **DataSource.dead_actors.mutable()))

    @routes.get("/logical/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        try:
            actor_id = req.query["actorId"]
            ip_address = req.query["ipAddress"]
            port = req.query["port"]
        except KeyError:
            return dashboard_utils.rest_response(
                success=False, message="Bad Request")
        try:
            channel = aiogrpc.insecure_channel(
                f"{ip_address}:{port}",
                options=dashboard_consts.GLOBAL_GRPC_OPTIONS)
            stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

            await stub.KillActor(
                core_worker_pb2.KillActorRequest(
                    intended_actor_id=ray._private.utils.hex_to_binary(
                        actor_id)))

        except aiogrpc.AioRpcError:
            # This always throws an exception because the worker
            # is killed and the channel is closed on the worker side
            # before this handler, however it deletes the actor correctly.
            pass

        return dashboard_utils.rest_response(
            success=True, message=f"Killed actor with id {actor_id}")

    async def run(self, server):
        pass
