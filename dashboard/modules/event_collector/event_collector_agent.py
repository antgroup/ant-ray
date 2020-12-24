import os
import json
import asyncio
import logging

import aiohttp.web
import ray.new_dashboard.modules.event_collector.event_collector_consts as event_collector_consts
from ray.new_dashboard.modules.event_collector.event_collector_utils import (
    monitor_events2, )
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.utils import async_loop_forever
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class EventCollectorAgent(dashboard_utils.DashboardAgentModule,
                          event_pb2_grpc.EventServiceServicer):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._event_dir = os.path.join(self._dashboard_agent.log_dir, "event")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor = None
        self._stub = None
        self._cached_events = asyncio.Queue(
            event_collector_consts.EVENT_AGENT_CACHE_SIZE)
        logger.info("Event collector cache buffer size: %s",
                    self._cached_events.maxsize)

    @routes.post("/agent_events")
    async def new_events(self, req) -> aiohttp.web.Response:
        event_list = await req.json()
        self._cache_events([json.dumps(event) for event in event_list])
        return dashboard_utils.rest_response(
            success=True,
            message=f"Received {len(event_list)} external events.")

    async def SyncEvents(self, request, context):
        logger.info("Sync last %s bytes of events to %s", request.last_nbytes,
                    request.report_address)
        channel = dashboard_utils.insecure_channel(request.report_address)
        self._stub = event_pb2_grpc.ReportEventServiceStub(channel)
        if self._monitor:
            try:
                self._monitor.cancel()
            except Exception:
                logger.exception("Cancel monitor task failed.")

        self._monitor = monitor_events2(
            self._event_dir,
            self._cache_events,
            source_types=event_collector_consts.EVENT_SOURCE_NON_PERSISTENCE)
        return event_pb2.SyncEventsReply()

    def _cache_events(self, data):
        if data:
            create_task(self._cached_events.put(data))

    @async_loop_forever(
        event_collector_consts.EVENT_AGENT_REPORT_INTERVAL_SECONDS)
    async def report_events(self):
        if self._stub is None:
            if not self._cached_events.empty():
                logger.info(
                    "Dashboard has not sync events yet. Report events later.")
            return
        data = await self._cached_events.get()
        while True:
            try:
                logger.info("report events: %s", data)
                request = event_pb2.ReportEventsRequest(event_strings=data)
                await self._stub.ReportEvents(request)
                break
            except Exception:
                logger.exception("Report event failed, retry later.")
            await asyncio.sleep(
                event_collector_consts.EVENT_AGENT_RETRY_INTERVAL_SECONDS)

    async def run(self, server):
        event_pb2_grpc.add_EventServiceServicer_to_server(self, server)
        await self.report_events()
