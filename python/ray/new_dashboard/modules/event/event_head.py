import os
import json
import asyncio
import logging
import datetime
from collections import OrderedDict, defaultdict

import aiohttp.web
import grpc.aio as aiogrpc

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.consts as dashboard_consts
from ray.new_dashboard.modules.event import event_consts
from ray.new_dashboard.modules.event.event_utils import (
    get_external_logger,
    parse_event_strings,
    monitor_events,
    format_event,
    new_event,
)
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc
from ray.new_dashboard.datacenter import DataSource
from ray.new_dashboard.datacenter import GlobalSignals
from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.utils import create_task

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)

OrderedEvents = OrderedDict
dashboard_utils._json_compatible_types.add(OrderedEvents)


class EventCollectorHead(dashboard_utils.DashboardHeadModule,
                         event_pb2_grpc.ReportEventServiceServicer):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._event_dir = os.path.join(self._dashboard_head.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._external_logger = get_external_logger(self._event_dir)
        self._stubs = {}
        self._monitor = None
        self._init_actor_worker_sls_url()
        self._cached_events = asyncio.Queue(event_consts.EVENT_HEAD_CACHE_SIZE)
        self._discarded_events_num = 0
        self._display_severity = self._get_display_severity()
        logger.info("Event cache buffer size: %s", self._cached_events.maxsize)
        DataSource.agents.signal.append(self._update_stubs)
        GlobalSignals.dashboard_restarted.append(
            self._dashboard_restarted_handler)

    def _init_actor_worker_sls_url(self):
        self._log_url = os.environ.get("LOG_URL",
                                       dashboard_consts.DEFAULT_LOG_URL)
        self._cluster_name = os.environ.get("CLUSTER_NAME", "empty_host")
        self._worker_sls_pattern = {
            "PYTHON": "python-worker-*.log",
            "JAVA": "java-worker*.log",
        }
        self._worker_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/{sls_pattern}&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and "
        self._worker_filename = {
            "PYTHON": "python-worker-{}-{}",
            "JAVA": "java-worker-{}-{}",
        }
        self._tags_hostname_path = \
            "( __tag__:__hostname__: {hostname} and " \
            "__tag__:__path__: /home/admin/logs/ray-logs/logs/{filename}.log )"

    async def _cache_events(self, event_list):
        if event_list:
            length = len(event_list)
            if self._cached_events.full():
                self._discarded_events_num += length
                return
            batch_size = min(length, event_consts.EVENT_HEAD_BATCH_CACHE_SIZE)
            for i in range(0, length, batch_size):
                events = event_list[i:i + batch_size]
                await self._cached_events.put(events)
                logger.info("Cached %d events", len(events))

    @async_loop_forever(event_consts.EVENT_HEAD_UPDATE_INTERVAL_SECONDS)
    async def _update_events(self):
        batch_size = self._cached_events.qsize()
        batch_events = []
        for _ in range(batch_size):
            events = await self._cached_events.get()
            batch_events.extend(events)
            if len(batch_events) >= event_consts.EVENT_HEAD_BATCH_UPDATE_SIZE:
                break
        if batch_events:
            self._batch_update_events(batch_events)

    def _batch_update_events(self, event_list):
        logger.info("Updating %d events, discarded %d events.",
                    len(event_list), self._discarded_events_num)
        self._discarded_events_num = 0
        # {job_id: {event_id: event}}
        all_job_events = defaultdict(JobEvents)
        # {worker_id: {event_id: event}}
        all_worker_events = defaultdict(JobEvents)
        # {actor_id: {event_id: event}}
        all_actor_events = defaultdict(JobEvents)
        for event in event_list:
            event_id = event["event_id"]
            custom_fields = event.get("custom_fields")
            system_event = False
            if custom_fields:
                job_id = custom_fields.get("job_id", "global") or "global"
                worker_id = custom_fields.get("worker_id", "")
                actor_id = custom_fields.get("actor_id", "")
                label_name = event.get("label", "")
                if label_name.startswith("WORKER_") and worker_id:
                    all_worker_events[worker_id][event_id] = event
                    system_event = True
                elif label_name == "WorkerFailureWithHsErrLog" and worker_id:
                    all_worker_events[worker_id][event_id] = event
                elif label_name.startswith("ACTOR_") and actor_id:
                    all_actor_events[actor_id][event_id] = event
                    system_event = True
            else:
                job_id = "global"
            if system_event is False:
                all_job_events[job_id][event_id] = event

        # update DataSource.job_pipeline_events
        self._update_job_pipeline_events(all_job_events)
        # update DataSource.worker_pipeline_events
        self._update_worker_pipeline_events(all_worker_events)
        # update DataSource.actor_pipeline_events
        self._update_actor_pipeline_events(all_actor_events)

    def _update_job_pipeline_events(self, all_job_events):
        for job_id, new_job_events in all_job_events.items():
            job_events = DataSource.events.get(job_id, JobEvents())
            job_events.update(new_job_events)
            # Remove job events if the count exceeded the limitation.
            limit = event_consts.PER_JOB_EVENTS_COUNT_LIMIT
            if len(job_events) > limit:
                pop_count = len(job_events) - limit
                for _ in range(pop_count):
                    job_events.popitem(last=False)
            DataSource.events[job_id] = job_events
            new_job_pipeline_events = JobEvents(
                (event["event_id"], event)
                for event in new_job_events.values()
                if event.get("label", "").startswith("JOB_PIPELINE_"))
            if len(new_job_pipeline_events) > 0:
                job_pipeline_events = DataSource.job_pipeline_events.get(
                    job_id, JobEvents())
                job_pipeline_events.update(new_job_pipeline_events)
                DataSource.job_pipeline_events[job_id] = job_pipeline_events

    def _update_worker_pipeline_events(self, all_worker_events):
        job_ids = set()
        for worker_id, new_worker_events in all_worker_events.items():
            worker_pipeline_events = DataSource.worker_pipeline_events.get(
                worker_id, OrderedEvents())
            for event in new_worker_events.values():
                job_id = event.get("custom_fields", {}).get("job_id", "")
                job_ids.add(job_id)
                # Note: i does not express the number of worker fo,
                # it's just an order.
                i = next(reversed(
                    worker_pipeline_events)) if worker_pipeline_events else 0
                if event.get("label", "") == "WORKER_START":
                    worker_pipeline_events[i + 1] = [event]
                else:
                    if i > 0:
                        last_events = next(
                            reversed(worker_pipeline_events.values()))
                        last_events.append(event)
                        self._pop_worker_events(last_events)
                    else:
                        worker_pipeline_events[i + 1] = [event]
            worker_events = DataSource.job_worker_pipeline_events.setdefault(
                job_id, OrderedEvents())
            worker_events[worker_id] = worker_pipeline_events
            DataSource.worker_pipeline_events[
                worker_id] = worker_pipeline_events
        self._pop_job_worker_pipeline_events(job_ids)

    def _update_actor_pipeline_events(self, all_actor_events):
        job_ids = set()
        actor_ids = set()
        for actor_id, new_actor_events in all_actor_events.items():
            actor_ids.add(actor_id)
            actor_pipeline_events = DataSource.actor_pipeline_events.get(
                actor_id, OrderedEvents())
            for event in new_actor_events.values():
                job_id = event.get("custom_fields", {}).get("job_id", "")
                job_ids.add(job_id)
                # Note: i does not express the number of actor fo,
                # it's just an order.
                i = next(reversed(
                    actor_pipeline_events)) if actor_pipeline_events else 0
                if event.get("label", "") == "ACTOR_RECONSTRUCTION_START":
                    actor_pipeline_events[i + 1] = [event]
                else:
                    if i > 0:
                        last_events = next(
                            reversed(actor_pipeline_events.values()))
                        last_events.append(event)
                        self._pop_actor_events(last_events)
                    else:
                        actor_pipeline_events[i + 1] = [event]
            actor_events = DataSource.job_actor_pipeline_events.setdefault(
                job_id, OrderedEvents())
            actor_events[actor_id] = actor_pipeline_events
            DataSource.actor_pipeline_events[actor_id] = actor_pipeline_events
        self._pop_actor_pipeline_events(actor_ids)
        self._pop_job_actor_pipeline_events(job_ids)

    def _pop_job_worker_pipeline_events(self, job_ids):
        for job_id in job_ids:
            worker_ids = DataSource.job_worker_pipeline_events.get(
                job_id, OrderedEvents())
            pop_count = len(
                worker_ids) - event_consts.PER_JOB_WORKER_PIPELINES_COUNT_LIMIT
            self.__pop_worker_pipeline_events(worker_ids, pop_count)

    def __pop_worker_pipeline_events(self, worker_ids, pop_count):
        for _ in range(pop_count):
            (worker_id, _) = worker_ids.popitem(last=False)
            if worker_id in DataSource.worker_pipeline_events:
                DataSource.worker_pipeline_events.pop(worker_id)

    def _pop_worker_events(self, events_list):
        pop_count = len(events_list) \
            - event_consts.PER_WORKER_EVENTS_COUNT_LIMIT
        for _ in range(pop_count):
            events_list.pop(0)

    def _pop_job_actor_pipeline_events(self, job_ids):
        for job_id in job_ids:
            actor_ids = DataSource.job_actor_pipeline_events.get(
                job_id, OrderedEvents())
            pop_count = len(
                actor_ids) - event_consts.PER_JOB_ACTOR_PIPELINES_COUNT_LIMIT
            for _ in range(pop_count):
                (actor_id, _) = actor_ids.popitem(last=False)
                DataSource.actor_pipeline_events.pop(actor_id)

    def _pop_actor_pipeline_events(self, actor_ids):
        for actor_id in actor_ids:
            actor_pipeline_events = DataSource.actor_pipeline_events.get(
                actor_id, OrderedEvents())
            i = len(actor_pipeline_events)
            pop_count = i - event_consts.PER_ACTOR_PIPELINES_COUNT_LIMIT
            for _ in range(pop_count):
                actor_pipeline_events.popitem(last=False)

    def _pop_actor_events(self, events_list):
        pop_count = len(events_list) \
            - event_consts.PER_ACTOR_EVENTS_COUNT_LIMIT
        for _ in range(pop_count):
            events_list.pop(0)

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, agent_data = change.new
            ip = DataSource.node_id_to_ip[node_id]
            port = agent_data[dashboard_consts.DASHBOARD_AGENT_GRPC_PORT]
            channel = aiogrpc.insecure_channel(
                f"{ip}:{port}", options=dashboard_consts.GLOBAL_GRPC_OPTIONS)
            stub = event_pb2_grpc.EventServiceStub(channel)
            self._stubs[ip] = stub
            await self._sync_events_from_agent(ip, stub)

    async def _sync_events_from_agent(self, ip, stub):
        address = f"{self._dashboard_head.ip}:{self._dashboard_head.grpc_port}"
        request = event_pb2.SyncEventsRequest(report_address=address)
        await stub.SyncEvents(request)

    def _get_display_severity(self):
        display_level = os.environ.get(
            event_consts.EVENT_DISPLAY_LEVEL_ENV_NAME,
            event_consts.DEFAULT_EVENT_DISPLAY_LEVEL).upper()
        level_to_list = {
            "INFO": "INFO,WARNING,ERROR,FATAL",
            "WARNING": "WARNING,ERROR,FATAL",
            "ERROR": "ERROR,FATAL",
            "FATAL": "FATAL",
        }
        if display_level in level_to_list.keys():
            return level_to_list[display_level]
        else:
            logger.warning("Unavailable event display level %s.",
                           display_level)
            return level_to_list["WARNING"]

    async def ReportEvents(self, request, context):
        received_events = []
        if request.event_strings:
            received_events.extend(parse_event_strings(request.event_strings))
        logger.info("Received %d events", len(received_events))
        await self._cache_events(received_events)
        return event_pb2.ReportEventsReply(send_sucess=True)

    # just for test
    @routes.get("/events/cache_size")
    @dashboard_utils.aiohttp_cache(5)
    async def get_event_cache_size(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="Event head cache size fetched.",
            cache_size=self._cached_events.qsize())

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
                success=True,
                message="All events fetched.",
                events=all_events,
                display_severity=self._display_severity)
        view = req.query.get("view")
        if view is not None and view != "pipeline":
            return dashboard_utils.rest_response(
                success=False, message="Unknown view: {}".format(view))
        if view == "pipeline":
            job_events = DataSource.job_pipeline_events.get(job_id, {})
        else:
            job_events = DataSource.events.get(job_id, {})
        return dashboard_utils.rest_response(
            success=True,
            message="Job events fetched.",
            job_id=job_id,
            events=list(job_events.values()),
            display_severity=self._display_severity)

    @routes.post("/events")
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

    @routes.get("/events/pipeline")
    @dashboard_utils.aiohttp_cache(5)
    async def get_pipeline_events(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view is None or view not in ["job", "worker", "actor"]:
            return dashboard_utils.rest_response(
                success=False,
                message="Parameter view error, should be [job|worker|actor]",
                events=[])
        query_id = req.query.get("query_id")
        if view == "job":
            return self._get_job_pipeline_events(query_id)
        if view == "worker":
            query_type = req.query.get("query_type")
            return self._get_worker_pipeline_events(query_type, query_id)
        if view == "actor":
            return self._get_actor_pipeline_events(query_id)

    @staticmethod
    def _get_job_pipeline_events(query_id):
        job_events = DataSource.job_pipeline_events.get(query_id, {})
        return dashboard_utils.rest_response(
            success=True,
            message="Job events fetched.",
            job_id=query_id,
            events=list(job_events.values()))

    @staticmethod
    def _get_worker_pipeline_events(query_type, query_id):
        if query_type is None or query_type not in ["job", "worker"]:
            return dashboard_utils.rest_response(
                success=False,
                message=  # noqa: E251
                "Parameter query_type error, should be [job|worker]",
                events=[])
        if query_type == "job":
            worker_events = DataSource.job_worker_pipeline_events.get(
                query_id, {})
            return dashboard_utils.rest_response(
                success=True,
                message="Worker events fetched.",
                job_id=query_id,
                events=worker_events)
        if query_type == "worker":
            worker_events = DataSource.worker_pipeline_events.get(query_id, {})
            return dashboard_utils.rest_response(
                success=True,
                message="Worker events fetched.",
                worker_id=query_id,
                events=worker_events)

    def _get_actor_pipeline_events(self, query_id):
        actor_pipe_events = DataSource.actor_pipeline_events.get(query_id, {})
        actor_events = {}
        language = ""
        tags = ""
        for i, events in actor_pipe_events.items():
            worker_info = {}
            for event in events:
                if event.get("label",
                             "") == "ACTOR_LEASED_FINISHED" and event.get(
                                 "severity", "") == "INFO":
                    worker_id = event.get("custom_fields", {}).get(
                        "worker_id", "")
                    pipeline_events = DataSource.worker_pipeline_events.get(
                        worker_id, {})
                    worker_events = next(iter(
                        pipeline_events.values())) if pipeline_events else []
                    worker_info = worker_events[0].get(
                        "custom_fields", {}) if worker_events else {}
                    if not language and worker_info.get("language", ""):
                        language = worker_info.get("language")
                    # find the Worker WorkerFailureWithHsErrLog or
                    # WORKER_EXIT event in reversed order of worker events
                    # and add time stamp and message
                    for worker_event in reversed(worker_events):
                        label = worker_event.get("label", "")
                        if label == "WorkerFailureWithHsErrLog":
                            worker_info["time_stamp"] = worker_event.get(
                                "time_stamp", "")
                            worker_info["message"] = worker_event.get(
                                "message", "")
                            break
                        if label == "WORKER_EXIT":
                            worker_info["time_stamp"] = worker_event.get(
                                "time_stamp", "")
                            worker_info["message"] = worker_event.get(
                                "message", "")
                            break
                    break
            if not worker_info and events:
                custom_fields = events[0].get("custom_fields", {})
                worker_info = {
                    "job_name": custom_fields.get("job_name", ""),
                    "job_id": custom_fields.get("job_id", "")
                }
            actor_events[i] = {"events": events, "info": worker_info}
            tags = self._generate_sls_url_tags_hostname_path(tags, worker_info)
        sls_url = self._generate_sls_url_for_all_workers(tags, language)
        return dashboard_utils.rest_response(
            success=True,
            message="Actor events fetched.",
            actor_id=query_id,
            sls_url=sls_url,
            events=actor_events)

    def _generate_sls_url_for_all_workers(self, tags_hostname_path, language):
        if language not in self._worker_sls_pattern.keys(
        ) or not tags_hostname_path:
            return ""
        sls_pattern = self._worker_sls_pattern.get(language)
        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        sls_url = self._worker_log.format(
            sls_pattern=sls_pattern,
            start_time=start_time,
            cluster_name=self._cluster_name)
        sls_url += tags_hostname_path
        return sls_url

    def _generate_sls_url_tags_hostname_path(self, tags, worker_info):
        job_id = worker_info.get("job_id", "")
        pid = worker_info.get("pid", "")
        if not job_id or not pid:
            hostname_path = ""
        else:
            hostname = worker_info.get("host_name", "")
            language = worker_info.get("language", "")
            filename = self._worker_filename.get(language, "")
            filename = filename.format(job_id, pid)
            hostname_path = self._tags_hostname_path.format(
                hostname=hostname, filename=filename)
        if hostname_path:
            tags = tags + " or " + hostname_path if tags else hostname_path
        return tags

    async def run(self, server):
        event_pb2_grpc.add_ReportEventServiceServicer_to_server(self, server)
        disable_event_head_monitor = os.environ.get(
            "DISABLE_EVENT_HEAD_MONITOR")
        if disable_event_head_monitor:
            logger.info("Disable event head monitor.")
        else:
            self._monitor = monitor_events(
                    self._event_dir,
                    lambda l: create_task(
                        self._cache_events(parse_event_strings(l))),
                    condition_func=lambda: not self._cached_events.full(),
                    source_types=event_consts.EVENT_SOURCE_ALL)
        await asyncio.gather(self._update_events())

    # ANT-INTERNAL
    async def _dashboard_restarted_handler(self, old_address, new_address):
        # Add a new event to indicate that the dashboard has been restarted.
        message = "The Ray dashboard has been restarted, the old address is " \
            f"{old_address} and the new address is {new_address}."
        logger.error(message)
        event = new_event("RAY_DASHBOARD_RESTARTED", {}, message, "FATAL")
        self._external_logger.info("%s", json.dumps(event))
