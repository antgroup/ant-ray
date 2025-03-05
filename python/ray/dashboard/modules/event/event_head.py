import asyncio
import logging
import os
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import islice
from typing import Dict, Union

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import env_integer
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray._private.utils import get_or_create_event_loop
from ray.core.generated import event_pb2, event_pb2_grpc
from ray.dashboard.consts import (
    RAY_STATE_SERVER_MAX_HTTP_REQUEST,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME,
)
from ray.dashboard.modules.event.event_utils import monitor_events, parse_event_strings
from ray.dashboard.state_api_utils import do_filter, handle_list_api
from ray.util.state.common import ClusterEventState, ListApiOptions, ListApiResponse

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)

MAX_EVENTS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_EVENTS_TO_CACHE", 10000))

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS", 1
)


async def _list_cluster_events_impl(
    *, all_events, executor: ThreadPoolExecutor, option: ListApiOptions
) -> ListApiResponse:
    """
    List all cluster events from the cluster. Made a free function to allow unit tests.

    Returns:
        A list of cluster events in the cluster.
        The schema of returned "dict" is equivalent to the
        `ClusterEventState` protobuf message.
    """

    def transform(all_events) -> ListApiResponse:
        result = []
        for _, events in all_events.items():
            for _, event in events.items():
                event["time"] = str(datetime.fromtimestamp(int(event["timestamp"])))
                result.append(event)

        num_after_truncation = len(result)
        result.sort(key=lambda entry: entry["timestamp"])
        total = len(result)
        result = do_filter(result, option.filters, ClusterEventState, option.detail)
        num_filtered = len(result)
        # Sort to make the output deterministic.
        result = list(islice(result, option.limit))
        return ListApiResponse(
            result=result,
            total=total,
            num_after_truncation=num_after_truncation,
            num_filtered=num_filtered,
        )

    return await get_or_create_event_loop().run_in_executor(
        executor, transform, all_events
    )


class EventHead(
    dashboard_utils.DashboardHeadModule,
    dashboard_utils.RateLimitedModule,
    event_pb2_grpc.ReportEventServiceServicer,
):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        dashboard_utils.DashboardHeadModule.__init__(self, config)
        dashboard_utils.RateLimitedModule.__init__(
            self,
            min(
                RAY_STATE_SERVER_MAX_HTTP_REQUEST,
                RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
            ),
        )
        self._event_dir = os.path.join(self.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        self.total_report_events_count = 0
        self.total_events_received = 0
        self.module_started = time.monotonic()
        # {job_id hex(str): {event_id (str): event (dict)}}
        self.events: Dict[str, JobEvents] = defaultdict(JobEvents)
        
        # New data structure for call graph
        # {job_id: {caller_class.caller_func -> callee_class.callee_func: count}}
        self.call_graph = defaultdict(lambda: defaultdict(int))
        # Maps to track unique actors and methods per job
        self.actors = defaultdict(set)
        self.actor_id_map = defaultdict(dict)  # {job_id: {actor_class: actor_id}}
        self.methods = defaultdict(dict)  # {job_id: {class.method: {id: unique_id, actorId: actor_id}}}
        self.functions = defaultdict(set)
        self.function_id_map = defaultdict(dict)  # {job_id: {function_name: function_id}}
        self.actor_counter = defaultdict(int)
        self.method_counter = defaultdict(int)
        self.function_counter = defaultdict(int)
        self.remote_call_uids = defaultdict(set)  # {job_id: {uid}}

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="event_head_executor",
        )

    async def limit_handler_(self):
        return dashboard_optional_utils.rest_response(
            success=False,
            error_message=(
                "Max number of in-progress requests="
                f"{self.max_num_call_} reached. "
                "To set a higher limit, set environment variable: "
                f"export {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME}='xxx'. "
                f"Max allowed = {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED}"
            ),
            result=None,
        )

    def _update_events(self, event_list):
        # {job_id: {event_id: event}}
        all_job_events = defaultdict(JobEvents)
        for event in event_list:
            event_id = event["event_id"]
            custom_fields = event.get("custom_fields")
            system_event = False
            if custom_fields:
                job_id = custom_fields.get("job_id", "global") or "global"
            else:
                job_id = "global"
            if system_event is False:
                all_job_events[job_id][event_id] = event

        for job_id, new_job_events in all_job_events.items():
            job_events = self.events[job_id]
            job_events.update(new_job_events)

            # Limit the # of events cached if it exceeds the threshold.
            if len(job_events) > MAX_EVENTS_TO_CACHE * 1.1:
                while len(job_events) > MAX_EVENTS_TO_CACHE:
                    job_events.popitem(last=False)

    async def ReportEvents(self, request, context):
        received_events = []
        if request.event_strings:
            received_events.extend(parse_event_strings(request.event_strings))
        logger.debug("Received %d events", len(received_events))
        self._update_events(received_events)
        self.total_report_events_count += 1
        self.total_events_received += len(received_events)
        return event_pb2.ReportEventsReply(send_success=True)

    async def _periodic_state_print(self):
        if self.total_events_received <= 0 or self.total_report_events_count <= 0:
            return

        elapsed = time.monotonic() - self.module_started
        return {
            "total_events_received": self.total_events_received,
            "Total_requests_received": self.total_report_events_count,
            "total_uptime": elapsed,
        }

    @routes.get("/events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_event(self, req) -> aiohttp.web.Response:
        job_id = req.query.get("job_id")
        if job_id is None:
            all_events = {
                job_id: list(job_events.values())
                for job_id, job_events in self.events.items()
            }
            return dashboard_optional_utils.rest_response(
                success=True, message="All events fetched.", events=all_events
            )

        job_events = self.events[job_id]
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Job events fetched.",
            job_id=job_id,
            events=list(job_events.values()),
        )

    @routes.get("/api/v0/cluster_events")
    @dashboard_utils.RateLimitedModule.enforce_max_concurrent_calls
    async def list_cluster_events(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_CLUSTER_EVENTS, "1")

        async def list_api_fn(option: ListApiOptions):
            return await _list_cluster_events_impl(
                all_events=self.events, executor=self._executor, option=option
            )

        return await handle_list_api(list_api_fn, req)

    @routes.post("/record_call")
    async def record_function_call(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Handle function call records sent from the record_calls decorator.
        
        This endpoint receives call records with information about function calls
        including caller/callee class and function names, and call counts.
        
        The records are stored in a call graph data structure instead of events.
        """
        try:
            # Parse the request data
            data = await req.json()
            call_record = data.get("call_record", {})
            job_id = call_record.get("job_id", "default_job")

            uid = data.get("uid", "")
            if uid in self.remote_call_uids[job_id]:
                return dashboard_optional_utils.rest_response(
                    success=True,
                    message="Function call record already received."
                )
            self.remote_call_uids[job_id].add(uid)
            
            # Extract call record information
            caller_class = call_record.get("caller_class", "")
            caller_func = call_record.get("caller_func", "")
            callee_class = call_record.get("callee_class", "")
            callee_func = call_record.get("callee_func", "")
            call_times = call_record.get("call_times", 1)
            
            # Create caller and callee identifiers
            caller_id = f"{caller_class}.{caller_func}" if caller_class else caller_func
            callee_id = f"{callee_class}.{callee_func}" if callee_class else callee_func
            
            # Update call graph
            self.call_graph[job_id][f"{caller_id}->{callee_id}"] += call_times
            
            # Track actors and methods
            if caller_class is not None:
                self.actors[job_id].add(caller_class)
                # Get or create actor ID using the map
                actor_id = self._get_actor_id(job_id, caller_class)
                
                if caller_id not in self.methods[job_id]:
                    self.method_counter[job_id] += 1
                    self.methods[job_id][caller_id] = {
                        "id": f"method{self.method_counter[job_id]}",
                        "actorId": actor_id,
                        "name": caller_func,
                        "class": caller_class
                    }
            else:
                self.functions[job_id].add(caller_func)
                # Get or create function ID using the map
                self._get_function_id(job_id, caller_func)
            
            if callee_class is not None:
                self.actors[job_id].add(callee_class)
                # Get or create actor ID using the map
                actor_id = self._get_actor_id(job_id, callee_class)
                
                if callee_id not in self.methods[job_id]:
                    self.method_counter[job_id] += 1
                    self.methods[job_id][callee_id] = {
                        "id": f"method{self.method_counter[job_id]}",
                        "actorId": actor_id,
                        "name": callee_func,
                        "class": callee_class
                    }
            else:
                self.functions[job_id].add(callee_func)
                # Get or create function ID using the map
                self._get_function_id(job_id, callee_func)

            if "main" not in self.functions[job_id]:
                self.functions[job_id].add("main")
            
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Function call record received and stored in call graph.",
            )
        except Exception as e:
            logger.error(f"Error processing function call record: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False,
                message=f"Error processing function call record: {str(e)}"
            )
    
    def _get_actor_id(self, job_id, actor_class):
        self.actor_id_map[job_id][actor_class] = actor_class.split(":")[1]
        return actor_class.split(":")[1]
    
    def _get_function_id(self, job_id, func_name):
        """Helper method to get or create function ID using a map for efficient lookup"""
        if func_name == "main":
            self.function_id_map[job_id]["main"] = "main"
            return "main"
        if func_name in self.function_id_map[job_id]:
            return self.function_id_map[job_id][func_name]
        
        self.function_counter[job_id] += 1
        function_id = f"function{self.function_counter[job_id]}"
        self.function_id_map[job_id][func_name] = function_id
        return function_id
        
    @routes.get("/call_graph")
    async def get_call_graph(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return the call graph data in the requested format for visualization."""
        try:
            # Get job_id from query parameters, default to "default_job" if not provided
            job_id = req.query.get("job_id", "default_job")
            
            # Build the graph data structure
            graph_data = {
                "actors": [],
                "methods": [],
                "functions": [],
                "callFlows": [],
                "dataFlows": []
            }
            
            # Add actors
            for actor_class, actor_id in self.actor_id_map[job_id].items():
                graph_data["actors"].append({
                    "id": actor_id,
                    "name": actor_class.split(":")[0],
                    "language": "python"
                })
            
            # Add methods
            for method_id, method_info in self.methods[job_id].items():
                graph_data["methods"].append({
                    "id": method_info["id"],
                    "actorId": method_info["actorId"],
                    "name": method_info["name"],
                    "language": "python"
                })
            
            # Add functions
            for func_name, function_id in self.function_id_map[job_id].items():
                if "." not in func_name:  # Ensure it's not a method
                    graph_data["functions"].append({
                        "id": function_id,
                        "name": func_name,
                        "language": "python"
                    })
            
            # Add call flows
            for call_edge, count in self.call_graph[job_id].items():
                caller, callee = call_edge.split("->")
                
                # Get source ID
                source_id = None
                if caller in self.methods[job_id]:
                    source_id = self.methods[job_id][caller]["id"]
                else:
                    # Check if it's a function
                    if caller in self.function_id_map[job_id]:
                        source_id = self.function_id_map[job_id][caller]
                
                # Get target ID
                target_id = None
                if callee in self.methods[job_id]:
                    target_id = self.methods[job_id][callee]["id"]
                else:
                    # Check if it's a function
                    if callee in self.function_id_map[job_id]:
                        target_id = self.function_id_map[job_id][callee]
                
                if source_id and target_id:
                    graph_data["callFlows"].append({
                        "source": source_id,
                        "target": target_id,
                        "count": count
                    })
                    
            
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Call graph data retrieved successfully.",
                graph_data=graph_data,
                job_id=job_id
            )
        except Exception as e:
            logger.error(f"Error retrieving call graph data: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False,
                message=f"Error retrieving call graph data: {str(e)}"
            )

    @routes.get("/call_graph_jobs")
    async def list_call_graph_jobs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return a list of job_ids for which we have call graph data."""
        try:
            # Get all job_ids that have call graph data
            job_ids = list(self.call_graph.keys())
            
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Call graph job_ids retrieved successfully.",
                job_ids=job_ids
            )
        except Exception as e:
            logger.error(f"Error retrieving call graph job_ids: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False,
                message=f"Error retrieving call graph job_ids: {str(e)}"
            )

    async def run(self, server):
        event_pb2_grpc.add_ReportEventServiceServicer_to_server(self, server)
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: self._update_events(parse_event_strings(data)),
            self._executor,
        )

    @staticmethod
    def is_minimal_module():
        return False
