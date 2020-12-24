import os
import json
import asyncio
import logging
from collections import OrderedDict, defaultdict

import aiohttp.web
import grpc.aio as aiogrpc

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.modules.event_collector.event_collector_consts as event_collector_consts
from ray.new_dashboard.modules.event_collector.event_collector_utils import (
    setup_persistence_event_logger,
    cleanup_persistence_event_logger,
    parse_event_strings,
    get_event_strings,
    monitor_events2,
)
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc
from ray.new_dashboard.datacenter import DataSource

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)


class EventCollectorHead(dashboard_utils.DashboardHeadModule,
                         event_pb2_grpc.ReportEventServiceServicer):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._event_dir = os.path.join(self._dashboard_head.log_dir, "event")
        os.makedirs(self._event_dir, exist_ok=True)
        self._stubs = {}
        self._logger_map = OrderedDict()
        self._monitor = None
        DataSource.agents.signal.append(self._update_stubs)

    def _get_logger(self, job_id):
        job_logger = self._logger_map.get(job_id)
        if job_logger is None:
            source_type = event_collector_consts.EVENT_SOURCE_PERSISTENCE
            if (len(self._logger_map) >=
                    event_collector_consts.EVENT_HEAD_PERSISTENT_LOGGER_COUNT):
                remove_job_id, _ = self._logger_map.popitem()
                logger.info(
                    "Remove event persistence logger %s "
                    "to create new logger %s.", remove_job_id, job_id)
                logger_name = f"{__name__}{source_type}_{remove_job_id}"
                cleanup_persistence_event_logger(logger_name)
            log_file = os.path.join(self._event_dir,
                                    f"event_{source_type}_{job_id}.log")
            job_logger = setup_persistence_event_logger(
                f"{__name__}{source_type}_{job_id}", log_file)
            self._logger_map[job_id] = job_logger
        else:
            self._logger_map.move_to_end(job_id, last=False)
        return job_logger

    def _update_events(self, event_list):
        all_job_events = defaultdict(JobEvents)
        for event in event_list:
            event_id = event["eventId"]
            job_id = event.get("jobId", "global") or "global"
            job_logger = self._get_logger(job_id)
            job_logger.info("%s", json.dumps(event))
            all_job_events[job_id][event_id] = event
        for job_id, new_job_events in all_job_events.items():
            job_events = DataSource.events.get(job_id, JobEvents())
            job_events.update(new_job_events)
            DataSource.events[job_id] = job_events
            new_job_pipeline_events = JobEvents(
                (event["eventId"], event) for event in new_job_events.values()
                if event.get("label", "").startswith("JOB_PIPELINE_"))
            if len(new_job_pipeline_events) > 0:
                job_pipeline_events = DataSource.pipeline_events.get(
                    job_id, JobEvents())
                job_pipeline_events.update(new_job_pipeline_events)
                DataSource.pipeline_events[job_id] = job_pipeline_events

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, agent_data = change.new
            ip = DataSource.node_id_to_ip[node_id]
            port = agent_data[dashboard_consts.DASHBOARD_AGENT_GRPC_PORT]
            channel = aiogrpc.insecure_channel(f"{ip}:{port}")
            stub = event_pb2_grpc.EventServiceStub(channel)
            self._stubs[ip] = stub
            await self._sync_events_from_agent(ip, stub)

    async def _sync_events_from_agent(self, ip, stub):
        address = f"{self._dashboard_head.ip}:{self._dashboard_head.grpc_port}"
        request = event_pb2.SyncEventsRequest(
            report_address=address,
            last_nbytes=event_collector_consts.EVENT_AGENT_RECOVER_BYTES)
        await stub.SyncEvents(request)

    async def ReportEvents(self, request, context):
        received_events = []
        # if request.event_objects:
        #     for event in request.event_objects:
        #         received_events.append(
        #             dashboard_utils.message_to_dict(
        #                 event, including_default_value_fields=True))
        if request.event_strings:
            received_events.extend(parse_event_strings(request.event_strings))
        logger.info("Received %d events", len(received_events))
        self._update_events(received_events)
        return event_pb2.ReportEventsReply(send_sucess=True)

    @routes.get("/events")
    @dashboard_utils.aiohttp_cache(5)
    async def get_event(self, req) -> aiohttp.web.Response:
        job_id = req.query.get("job_id")
        if job_id is None:
            all_events = {
                job_id: list(job_events.values())
                for job_id, job_events in DataSource.events.items()
            }
            return dashboard_utils.rest_response(
                success=True, message="All events fetched.", events=all_events)
        view = req.query.get("view")
        if view is not None and view != "pipeline":
            return dashboard_utils.rest_response(
                success=False, message="Unknown view: {}".format(view))
        if view == "pipeline":
            job_events = DataSource.pipeline_events.get(job_id, {})
        else:
            job_events = DataSource.events.get(job_id, {})
        return dashboard_utils.rest_response(
            success=True,
            message="Job events fetched.",
            job_id=job_id,
            events=list(job_events.values()))

    @routes.post("/events")
    async def new_events(self, req) -> aiohttp.web.Response:
        event_list = await req.json()
        self._update_events(event_list)
        return dashboard_utils.rest_response(
            success=True,
            message=f"Received {len(event_list)} external events.")

    async def run(self, server):
        event_pb2_grpc.add_ReportEventServiceServicer_to_server(self, server)
        persistence_source = event_pb2.Event.SourceType.Name(
            event_pb2.Event.PERSISTENCE)
        event_strings = get_event_strings(self._event_dir,
                                          [persistence_source])
        event_list = parse_event_strings(event_strings)
        self._update_events(event_list)
        self._monitor = monitor_events2(
            self._event_dir,
            lambda l: self._update_events(parse_event_strings(l)),
            source_types=event_collector_consts.EVENT_SOURCE_NON_PERSISTENCE)
