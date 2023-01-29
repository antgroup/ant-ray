import os
import json
import asyncio
import logging

import aiohttp.web
from ray.new_dashboard.modules.event import event_consts
from ray.new_dashboard.modules.event.event_utils import (
    monitor_events,
    format_event,
    get_external_logger,
    new_event,
)
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.consts as dashboard_consts
from ray.new_dashboard.utils import async_loop_forever, create_task
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class EventCollectorAgent(dashboard_utils.DashboardAgentModule,
                          event_pb2_grpc.EventServiceServicer):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._event_dir = os.path.join(self._dashboard_agent.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._external_logger = get_external_logger(self._event_dir)
        self._monitor = None
        self._stub = None
        self._cached_events = asyncio.Queue(
            event_consts.EVENT_AGENT_CACHE_SIZE)
        logger.info("Event collector cache buffer size: %s",
                    self._cached_events.maxsize)

    @routes.post("/agent_events")
    async def new_events(self, req) -> aiohttp.web.Response:
        event_list = await req.json()
        for event in event_list:
            event = format_event(event)
            self._external_logger.info("%s", json.dumps(event))
        # Writes the external events to log file for external log
        # collection system. These events will be reported by event
        # monitor, so we don't report here.
        return dashboard_utils.rest_response(
            success=True,
            message=f"Received {len(event_list)} external events.")

    async def _cache_events(self, event_list):
        if event_list:
            length = len(event_list)
            batch_size = min(length, event_consts.EVENT_AGENT_BATCH_CACHE_SIZE)
            for i in range(0, length, batch_size):
                events = event_list[i:i + batch_size]
                await self._cached_events.put(events)
                logger.info("Cached one item which had %d events", len(events))

    async def SyncEvents(self, request, context):
        logger.info("Sync events to %s", request.report_address)
        channel = dashboard_utils.insecure_channel(
            request.report_address,
            options=dashboard_consts.GLOBAL_GRPC_OPTIONS)
        self._stub = event_pb2_grpc.ReportEventServiceStub(channel)
        if self._monitor:
            try:
                self._monitor.cancel()
            except Exception:
                logger.exception("Cancel monitor task failed.")

        self._monitor = monitor_events(
            self._event_dir,
            lambda data: create_task(self._cache_events(data)),
            condition_func=lambda: not self._cached_events.full(),
            source_types=event_consts.EVENT_SOURCE_ALL)
        return event_pb2.SyncEventsReply()

    async def CheckJavaHsErrLog(self, request, context):
        def _read_lines(log_path):
            lines = []
            with open(log_path, "rb") as f:
                for _ in range(dashboard_consts.
                               EVENT_AGENT_CHECK_JAVA_HS_ERR_LOG_LINE_NUMBER):
                    line = f.readline().decode("utf-8")
                    if not line:
                        break
                    lines.append(line)
            return "".join(lines)

        logger.info("Check Java hs err log on the path %s", request.log_path)
        # We delay check hs err log because maybe the log file will be
        # generated later than the disconnecting callback.
        await asyncio.sleep(
            dashboard_consts.EVENT_AGENT_CHECK_JAVA_HS_ERR_LOG_DELAY_SECONDS)
        if os.path.exists(request.log_path):
            logger.info(
                "Hs err log exists on the path %s, "
                "a new event will be created.", request.log_path)
            try:
                loop = asyncio.get_event_loop()
                log = await loop.run_in_executor(None, _read_lines,
                                                 request.log_path)
                custom_fields = {
                    "job_id": request.job_id,
                    "job_name": request.job_name,
                    "worker_id": request.worker_id,
                    "actor_id": request.actor_id,
                    "pid": str(request.pid),
                    "ip": request.ip,
                    "is_driver": str(request.is_driver)
                }
                msg = f"JVM crash at {request.ip}, the pid is {request.pid}," \
                    f"the hs err log path {request.log_path}, abstract:\n{log}"

                event = new_event("WorkerFailureWithHsErrLog", custom_fields,
                                  msg, "ERROR")
                self._external_logger.info("%s", json.dumps(event))
            except Exception:
                logger.exception("Make hs err log event failed.")

        return event_pb2.CheckJavaHsErrLogReply()

    @async_loop_forever(event_consts.EVENT_AGENT_REPORT_INTERVAL_SECONDS)
    async def report_events(self):
        if self._stub is None:
            if not self._cached_events.empty():
                logger.info(
                    "Dashboard has not sync events yet. Report events later.")
            return
        batch_size = self._cached_events.qsize()
        data = []
        for _ in range(batch_size):
            events = await self._cached_events.get()
            data.extend(events)
            if len(data) >= event_consts.EVENT_AGENT_BATCH_REPORT_SIZE:
                break
        if not data:
            return
        while True:
            try:
                logger.info("Report %s events.", len(data))
                request = event_pb2.ReportEventsRequest(event_strings=data)
                await self._stub.ReportEvents(request)
                break
            except Exception:
                logger.exception("Report event failed, retry later.")
            await asyncio.sleep(
                event_consts.EVENT_AGENT_RETRY_INTERVAL_SECONDS)

    async def run(self, server):
        event_pb2_grpc.add_EventServiceServicer_to_server(self, server)
        await self.report_events()
