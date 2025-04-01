import logging
import time
import json
import asyncio
from ray._private import ray_constants
import aiohttp.web
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.datacenter import DataSource
from ray._private.test_utils import get_resource_usage

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


class AsyncTelnetClient:
    """A simple asynchronous telnet client implementation using asyncio."""

    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.reader = None
        self.writer = None
        self._read_lock = asyncio.Lock()

    async def connect(self):
        """Connect to the telnet server."""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        logger.info(f"Connected to telnet server at {self.host}:{self.port}")
        return self

    def check_connection(self):
        """Check if the connection is still active."""
        if self.reader is None or self.writer is None:
            raise ConnectionError("Not connected to telnet server")
        if self.writer.is_closing():
            raise ConnectionError("Connection is closing or closed")
        return True

    async def read_very_eager(self):
        """Read all available data without blocking."""
        self.check_connection()

        # Use a lock to prevent multiple concurrent reads
        async with self._read_lock:
            # Set a very short timeout to avoid blocking
            try:
                data = await asyncio.wait_for(self.reader.read(4096), timeout=0.1)
            except asyncio.TimeoutError:
                return None

            return data

    async def write(self, data):
        """Write data to the telnet connection."""
        self.check_connection()
        self.writer.write(data)
        await self.writer.drain()

    async def close(self):
        """Close the telnet connection."""
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception as e:
                logger.warning(f"Error waiting for writer to close: {str(e)}")
            self.writer = None
            self.reader = None


class InsightHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self.debug_sessions = {}
        self.breakpoints = {}

    async def fetch_breakpoint(self, job_id, task_id):
        """Fetch the breakpoint from the InsightMonitor."""
        active_sessions = await self.gcs_aio_client.internal_kv_keys(
            b"RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
        )
        for session in active_sessions:
            data = json.loads(
                await self.gcs_aio_client.internal_kv_get(
                    session, namespace=ray_constants.KV_NAMESPACE_PDB
                )
            )
            if data.get("job_id") == job_id and data.get("task_id") == task_id:
                return data
        return None

    async def _deactivate_breakpoint(self, task_id):
        """Deactivate the breakpoint for a given task."""
        if task_id in self.breakpoints:
            breakpoint = self.breakpoints[task_id]
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )
            if address:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"http://{address.decode()}/deactivate_breakpoint",
                        json={
                            "task_id": task_id,
                            "actor_id": breakpoint.get("actor_id"),
                            "job_id": breakpoint.get("job_id"),
                            "actor_cls": breakpoint.get("actor_cls"),
                            "actor_name": breakpoint.get("actor_name"),
                            "method_name": breakpoint.get("method_name"),
                            "func_name": breakpoint.get("func_name"),
                        },
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"Error deactivating breakpoint: {await response.text()}"
                            )
                        elif response.status == 200:
                            del self.breakpoints[task_id]
                            return True
                return False

    async def _cleanup_debug_session(self, task_id):
        """Helper method to clean up a debug session."""
        try:
            # Only proceed if the session still exists
            if task_id in self.debug_sessions:
                tn = self.debug_sessions[task_id]
                await tn.write("c\n".encode("utf-8"))
                await asyncio.sleep(1)
                # Close the telnet connection
                try:
                    await self.debug_sessions[task_id].close()
                except Exception as e:
                    logger.warning(
                        f"Error closing telnet connection for task {task_id}: {str(e)}"
                    )

                # Remove from sessions dict
                del self.debug_sessions[task_id]

                await self._deactivate_breakpoint(task_id)

                logger.info(f"Debug session for task {task_id} cleaned up successfully")
        except Exception as e:
            logger.error(
                f"Error cleaning up debug session for task {task_id}: {str(e)}"
            )

    @routes.get("/get_debug_state")
    async def get_debug_state(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Get the debug state for a given job."""
        job_id = req.query.get("job_id", None)
        address = await self.gcs_aio_client.internal_kv_get(
            "insight_monitor_address",
            namespace="flowinsight",
            timeout=5,
        )
        if not address:
            return dashboard_optional_utils.rest_response(
                success=False,
                message="InsightMonitor address not found in KV store",
            )
        host, port = address.decode().split(":")
        try:
            debug_state = False
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://{host}:{port}/get_debug_state?job_id={job_id}",
                ) as response:
                    if response.status != 200:
                        logger.warning(
                            f"Error getting debug state: {await response.text()}"
                        )
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error getting debug state: {await response.text()}",
                        )
                    body = await response.json()
                    debug_state = body.get("debug_state", False)

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Debug state retrieved successfully.",
                debug_state=debug_state,
            )

        except Exception as e:
            logger.error(f"Error getting debug state: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error getting debug state: {str(e)}"
            )

    @routes.get("/switch_debug_mode")
    async def switch_debug_mode(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Switch the debug mode for a given task."""
        job_id = req.query.get("job_id", None)
        mode = req.query.get("mode", "false")
        mode = True if mode == "true" else False
        address = await self.gcs_aio_client.internal_kv_get(
            "insight_monitor_address",
            namespace="flowinsight",
            timeout=5,
        )
        if not address:
            return dashboard_optional_utils.rest_response(
                success=False,
                message="InsightMonitor address not found in KV store",
            )
        host, port = address.decode().split(":")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://{host}:{port}/switch_debug_mode",
                    json={"job_id": job_id, "mode": mode},
                ) as response:
                    if response.status != 200:
                        logger.warning(
                            f"Error switching debug mode: {await response.text()}"
                        )
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error switching debug mode: {await response.text()}",
                        )

            return dashboard_optional_utils.rest_response(
                success=True, message="Debug mode switched successfully."
            )

        except Exception as e:
            logger.error(f"Error switching debug mode: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error switching debug mode: {str(e)}"
            )

    @routes.get("/insight_debug")
    async def insight_debug(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Send a command to a debug session and retrieve output"""
        job_id = req.query.get("job_id", None)
        task_id = req.query.get("task_id", None)
        cmd = req.query.get("cmd", None)

        if job_id is None:
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Job ID is required",
            )

        # Initialize debug session if it doesn't exist
        if task_id not in self.debug_sessions:
            breakpoint = await self.fetch_breakpoint(job_id, task_id)
            if breakpoint is None:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="Breakpoint not found",
                )

            pdb_address = breakpoint.get("pdb_address")
            host, port = pdb_address.split(":")

            # Create and connect the async telnet client
            tn = AsyncTelnetClient(host, port)
            await tn.connect()
            self.debug_sessions[task_id] = tn

            initial_data = b""
            start_time = time.time()
            while b"(Pdb)" not in initial_data and time.time() - start_time < 5:
                data = await tn.read_very_eager()
                if data:
                    initial_data += data
                else:
                    await asyncio.sleep(0.1)

        # Send command if provided
        tn = self.debug_sessions[task_id]
        if cmd is not None and cmd != "":
            cmd_with_newline = cmd + "\n" if not cmd.endswith("\n") else cmd
            await tn.write(cmd_with_newline.encode("utf-8"))

        full_response = b""
        start_time = time.time()
        timeout_seconds = 10

        # Collect output with a timeout
        while True:
            # Check if we've timed out
            if time.time() - start_time > timeout_seconds:
                break

            # Check if we've received the prompt, indicating command completion
            if b"(Pdb)" in full_response:
                break

            # Read available data
            data = await tn.read_very_eager()
            if data:
                full_response += data
            else:
                # Short sleep to avoid busy waiting
                await asyncio.sleep(0.1)

        return dashboard_optional_utils.rest_response(
            success=True,
            message="Command sent successfully.",
            output=full_response.decode("utf-8"),
        )

    @routes.get("/close_debug_session")
    async def close_debug_session(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """Close a specific debug session and clean up resources."""
        try:
            task_id = req.query.get("task_id", None)
            job_id = req.query.get("job_id", None)
            if task_id is None:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="Task ID is required",
                )
            await self.do_close_debug_session(task_id, job_id)
            return dashboard_optional_utils.rest_response(
                success=True,
                message=f"Debug session for task {task_id} closed successfully.",
            )
        except Exception as e:
            logger.error(f"Error closing debug session: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error closing debug session: {str(e)}"
            )

    async def do_close_debug_session(self, task_id, job_id):
        if task_id in self.debug_sessions:
            # Use the helper method to clean up the debug session
            await self._cleanup_debug_session(task_id)
        else:
            breakpoint = await self.fetch_breakpoint(job_id, task_id)
            if breakpoint is None:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="Breakpoint not found",
                )

            pdb_address = breakpoint.get("pdb_address")
            host, port = pdb_address.split(":")

            # Create and connect the async telnet client
            tn = AsyncTelnetClient(host, port)
            try:
                await tn.connect()
                # Send continue command with a newline character
                await tn.write("c\n".encode("utf-8"))
                # Small delay to allow pdb to process the command
                await asyncio.sleep(1)
                await tn.close()
            except ConnectionError as e:
                logger.warning(f"Connection error with debugger: {str(e)}")
                # Debugger connection may already be closed, which is fine
            except Exception as e:
                logger.warning(f"Error when continuing debugger: {str(e)}")
                await tn.close()

            # Consider the session closed regardless of outcome
            # Deactivate breakpoint if it exists for this task
            await self._deactivate_breakpoint(task_id)

    @routes.get("/set_breakpoint")
    async def set_breakpoint(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Set a breakpoint for a given function or actor method."""
        try:
            job_id = req.query.get("job_id", None)
            actor_id = req.query.get("actor_id", None)
            actor_cls = req.query.get("actor_cls", None)
            actor_name = req.query.get("actor_name", None)
            method_name = req.query.get("method_name", None)
            func_name = req.query.get("func_name", None)
            flag = req.query.get("flag", "true")
            flag = True if flag == "true" else False
            # Get insight monitor address from KV store
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )
            if not address:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="InsightMonitor address not found in KV store",
                )

            if not flag:
                to_close = []
                for task_id, breakpoint in self.breakpoints.items():
                    if (
                        breakpoint.get("job_id") == job_id
                        and breakpoint.get("actor_id") == actor_id
                        and breakpoint.get("actor_cls") == actor_cls
                        and breakpoint.get("actor_name") == actor_name
                        and breakpoint.get("method_name") == method_name
                        and breakpoint.get("func_name") == func_name
                    ):
                        to_close.append(task_id)
                for task_id in to_close:
                    await self.do_close_debug_session(task_id, job_id)

            host, port = address.decode().split(":")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://{host}:{port}/set_breakpoint",
                    json={
                        "job_id": job_id,
                        "actor_id": actor_id,
                        "actor_cls": actor_cls,
                        "actor_name": actor_name,
                        "method_name": method_name,
                        "func_name": func_name,
                        "flag": flag,
                    },
                ) as response:
                    if response.status != 200:
                        logger.warning(
                            f"Error setting breakpoint: {await response.text()}"
                        )
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error setting breakpoint: {await response.text()}",
                        )
            return dashboard_optional_utils.rest_response(
                success=True, message="Breakpoint set successfully."
            )
        except Exception as e:
            logger.error(f"Error setting breakpoint: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error setting breakpoint: {str(e)}"
            )

    @routes.get("/get_breakpoints")
    async def get_breakpoints(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Get all breakpoints for a given job."""
        try:
            job_id = req.query.get("job_id", "default_job")
            # Get insight monitor address from KV store
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )
            if not address:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="InsightMonitor address not found in KV store",
                )
            host, port = address.decode().split(":")
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://{host}:{port}/get_breakpoints?job_id={job_id}"
                ) as response:
                    if response.status != 200:
                        logger.warning(
                            f"Error fetching breakpoints: {await response.text()}"
                        )
                        breakpoints = {}
                    else:
                        breakpoints = await response.json()

            for breakpoint in breakpoints:
                if breakpoint.get("task_id", None) is not None:
                    self.breakpoints[breakpoint.get("task_id")] = {
                        **breakpoint,
                        "job_id": job_id,
                    }

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Breakpoints retrieved successfully.",
                breakpoints=breakpoints,
            )

        except Exception as e:
            logger.error(f"Error retrieving breakpoints: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error retrieving breakpoints: {str(e)}"
            )

    @routes.get("/physical_view")
    async def get_physical_view(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return a physical view of resource usage and actor placement across nodes.

        Query Parameters:
            job_id: Filter actors by job ID

        Returns:
            JSON with node-based view of resources and actors
        """
        try:
            job_id = req.query.get("job_id", "default_job")

            # Get resource usage data from GCS
            resources_data = get_resource_usage(self.gcs_address)

            # Get insight monitor address from KV store
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )

            if not address:
                logger.warning("InsightMonitor address not found in KV store")
                context_info = {}
                resource_usage = {}
            else:
                host, port = address.decode().split(":")
                # Fetch context info from insight monitor
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"http://{host}:{port}/get_context_info",
                        params={"job_id": job_id},
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"Error fetching context info: {await response.text()}"
                            )
                            context_info = {}
                        else:
                            context_info = await response.json()

                    async with session.get(
                        f"http://{host}:{port}/get_resource_usage",
                        params={"job_id": job_id},
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"Error fetching resource usage: {await response.text()}"
                            )
                            resource_usage = {}
                        else:
                            resource_usage = await response.json()

            # Build node-based view
            physical_view = {}

            # Process each node's resource usage
            for node_data in resources_data.batch:
                node_id = node_data.node_id.hex()
                if node_id not in physical_view:
                    physical_view[node_id] = {
                        "resources": {},
                        "actors": {},
                        "gpus": [],
                    }

                for resource_name, total in node_data.resources_total.items():
                    available = node_data.resources_available.get(resource_name, 0.0)
                    physical_view[node_id]["resources"][resource_name] = {
                        "total": total,
                        "available": available,
                    }

                # Add GPU information from node physical stats
                node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
                if "gpus" in node_physical_stats:
                    physical_view[node_id]["gpus"] = node_physical_stats["gpus"]

            # Add actor information
            for actor_id, actor_info in DataSource.actors.items():
                if actor_info.get("jobId") == job_id:
                    # Skip the internal insight monitor actor
                    if actor_info.get("name", "") != "":
                        actor_name = actor_info.get("name")
                    else:
                        actor_name = actor_info.get("className", "Unknown")

                    if actor_name == "_ray_internal_insight_monitor":
                        continue

                    node_id = actor_info["address"]["rayletId"]
                    if node_id in physical_view:
                        actor_pid = actor_info.get("pid")
                        actor_view = {
                            "actorId": actor_id,
                            "name": actor_name,
                            "state": actor_info.get("state", "UNKNOWN"),
                            "pid": actor_pid,
                            "nodeId": node_id,  # Add node ID to actor information
                            "requiredResources": actor_info.get(
                                "requiredResources", {}
                            ),
                            "gpuDevices": [],  # Add GPU devices used by this actor
                        }

                        # Add placement group info if available
                        pg_id = actor_info.get("placementGroupId")
                        if pg_id:
                            actor_view["placementGroup"] = {
                                "id": pg_id,
                            }

                        # Initialize process stats with default values
                        actor_view["processStats"] = {
                            "cpuPercent": 0,
                            "memoryInfo": {"rss": 0},
                        }

                        # Get CPU and memory information from node_physical_stats by matching PID
                        node_physical_stats = DataSource.node_physical_stats.get(
                            node_id, {}
                        )
                        if actor_pid and "workers" in node_physical_stats:
                            for worker in node_physical_stats["workers"]:
                                if worker.get("pid") == actor_pid:
                                    # Update process stats with data from node_physical_stats
                                    actor_view["processStats"] = {
                                        "cpuPercent": worker.get("cpuPercent", 0),
                                        "memoryInfo": worker.get(
                                            "memoryInfo", {"rss": 0}
                                        ),
                                    }

                        actor_view["nodeCpuPercent"] = node_physical_stats.get("cpu", 0)

                        # Add node memory info from the dictionary if available
                        actor_view["nodeMem"] = node_physical_stats.get("mem", 0)

                        # Add GPU information for this actor based on its PID
                        if actor_pid:
                            node_gpus = physical_view[node_id].get("gpus", [])
                            for gpu in node_gpus:
                                for process in gpu.get("processesPids", []):
                                    if process["pid"] == actor_pid:
                                        actor_view["gpuDevices"].append(
                                            {
                                                "index": gpu["index"],
                                                "name": gpu["name"],
                                                "uuid": gpu["uuid"],
                                                "memoryUsed": process["gpuMemoryUsage"],
                                                "memoryTotal": gpu["memoryTotal"],
                                                "utilization": gpu.get(
                                                    "utilizationGpu", 0
                                                ),
                                            }
                                        )

                        # If actor has no GPU devices but the node has GPUs, add node GPU info
                        if not actor_view["gpuDevices"]:
                            for gpu in node_physical_stats["gpus"]:
                                actor_view["gpuDevices"].append(
                                    {
                                        "index": gpu["index"],
                                        "name": gpu["name"],
                                        "uuid": gpu["uuid"],
                                        "memoryTotal": gpu["memoryTotal"],
                                        "memoryUsed": 0,  # No specific usage for this actor
                                        "utilization": gpu.get("utilizationGpu", 0),
                                        "nodeGpuOnly": True,
                                    }
                                )

                        actor_view["contextInfo"] = context_info.get(actor_id, {})
                        actor_view["resourceUsage"] = resource_usage.get(actor_id, {})

                        physical_view[node_id]["actors"][actor_id] = actor_view

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Physical view data retrieved successfully.",
                physical_view=physical_view,
            )

        except Exception as e:
            logger.error(f"Error retrieving physical view data: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error retrieving physical view data: {str(e)}"
            )

    @routes.get("/call_graph")
    async def get_call_graph(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return the call graph data by reading from the InsightMonitor HTTP endpoint."""
        try:
            job_id = req.query.get("job_id", "default_job")
            stack_mode = req.query.get("stack_mode", "0")

            # Get insight monitor address from KV store using gcs_aio_client
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )

            if not address:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="InsightMonitor address not found in KV store",
                )

            host, port = address.decode().split(":")

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://{host}:{port}/get_call_graph_data",
                    params={"job_id": job_id, "stack_mode": stack_mode},
                ) as response:
                    if response.status != 200:
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error from insight monitor: {await response.text()}",
                        )

                    graph_data = await response.json()

                    # Add GPU information and actor names to each node in the graph data
                    for node in graph_data.get("actors", []):
                        actor_id = node.get("id")
                        # Check if we have this actor in DataSource.actors
                        if actor_id in DataSource.actors:
                            actor_info = DataSource.actors[actor_id]
                            # Use actor name if available, otherwise keep original name
                            if "name" in actor_info and actor_info["name"] != "":
                                node["name"] = actor_info["name"]
                                node["is_actor_name_set"] = True
                            else:
                                node["is_actor_name_set"] = False

                        # Add GPU information for this actor
                        if actor_id in DataSource.actors:
                            actor_info = DataSource.actors[actor_id]
                            node_id = actor_info["address"]["rayletId"]
                            actor_pid = actor_info.get("pid")

                            # Add node ID to actor information
                            node["nodeId"] = node_id

                            # Initialize GPU info for this actor
                            node["gpuDevices"] = []

                            node_physical_stats = DataSource.node_physical_stats.get(
                                node_id, {}
                            )

                            # Add node CPU info from the dictionary if available
                            node["nodeCpuPercent"] = node_physical_stats.get("cpu")

                            # Add node memory info from the dictionary if available
                            node["nodeMem"] = node_physical_stats.get("mem", [])

                            if actor_pid:
                                for gpu in node_physical_stats.get("gpus", []):
                                    for process in gpu.get("processesPids", []):
                                        if process["pid"] == actor_pid:
                                            node["gpuDevices"].append(
                                                {
                                                    "index": gpu["index"],
                                                    "name": gpu["name"],
                                                    "uuid": gpu["uuid"],
                                                    "memoryUsed": process[
                                                        "gpuMemoryUsage"
                                                    ],
                                                    "memoryTotal": gpu["memoryTotal"],
                                                    "utilization": gpu.get(
                                                        "utilizationGpu", 0
                                                    ),
                                                }
                                            )

                            # If actor has no GPU devices but the node has GPUs, add node GPU info
                            if not node["gpuDevices"]:
                                # Add node-level GPU information without usage details
                                for gpu in node_physical_stats.get("gpus", []):
                                    node["gpuDevices"].append(
                                        {
                                            "index": gpu["index"],
                                            "name": gpu["name"],
                                            "uuid": gpu["uuid"],
                                            "memoryTotal": gpu["memoryTotal"],
                                            "memoryUsed": 0,  # No specific usage for this actor
                                            "utilization": gpu.get("utilizationGpu", 0),
                                            "nodeGpuOnly": True,  # Flag to indicate this is node-level info only
                                        }
                                    )

                    return dashboard_optional_utils.rest_response(
                        success=True,
                        message="Call graph data retrieved successfully.",
                        graph_data=graph_data,
                        job_id=job_id,
                    )

        except Exception as e:
            logger.error(f"Error retrieving call graph data: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error retrieving call graph data: {str(e)}"
            )

    @routes.get("/flame_graph")
    async def get_flame_graph(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return the flame graph data by reading from the InsightMonitor HTTP endpoint."""
        try:
            job_id = req.query.get("job_id", "default_job")

            # Get insight monitor address from KV store using gcs_aio_client
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )

            if not address:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="InsightMonitor address not found in KV store",
                )

            host, port = address.decode().split(":")

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://{host}:{port}/get_flame_graph_data",
                    params={"job_id": job_id},
                ) as response:
                    if response.status != 200:
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error from insight monitor: {await response.text()}",
                        )

                    flame_data = await response.json()

                    return dashboard_optional_utils.rest_response(
                        success=True,
                        message="Flame graph data retrieved successfully.",
                        flame_data=flame_data,
                        job_id=job_id,
                    )

        except Exception as e:
            logger.error(f"Error retrieving flame graph data: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error retrieving flame graph data: {str(e)}"
            )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
