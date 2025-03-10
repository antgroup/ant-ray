import logging
import aiohttp.web
import ray
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.datacenter import DataSource
from ray._private.test_utils import get_resource_usage

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


class InsightHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)

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
            else:
                host, port = address.decode().split(":")
                # Fetch context info from insight monitor
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"http://{host}:{port}/get_context_info",
                        params={"job_id": job_id},
                    ) as response:
                        if response.status != 200:
                            logger.warning(f"Error fetching context info: {await response.text()}")
                            context_info = {}
                        else:
                            context_info = await response.json()
            
            # Build node-based view
            physical_view = {}
            
            # Process each node's resource usage
            for node_data in resources_data.batch:
                node_id = node_data.node_id.hex()
                if node_id not in physical_view:
                    physical_view[node_id] = {
                        "resources": {},
                        "actors": {},
                    }
                
                # Add resource usage - using the correct map fields from protobuf
                for resource_name, total in node_data.resources_total.items():
                    available = node_data.resources_available.get(resource_name, 0.0)
                    physical_view[node_id]["resources"][resource_name] = {
                        "total": total,
                        "available": available
                    }
            
            # Add actor information
            for actor_id, actor_info in DataSource.actors.items():
                if actor_info.get("jobId") == job_id:
                    node_id = actor_info["address"]["rayletId"]
                    if node_id in physical_view:
                        actor_view = {
                            "actor_id": actor_id,
                            "name": actor_info.get("actorConstructor", "Unknown"),
                            "state": actor_info.get("state", "UNKNOWN"),
                            "pid": actor_info.get("pid"),
                            "required_resources": actor_info.get("requiredResources", {}),
                        }
                        
                        # Add placement group info if available
                        pg_id = actor_info.get("placementGroupId")
                        if pg_id:
                            actor_view["placement_group"] = {
                                "id": pg_id,
                            }
                        
                        actor_view["context_info"] = {}
                        
                        for context_entry in context_info.get(actor_id, []):
                            key = context_entry.get("key")
                            if key:
                                actor_view["context_info"][key] = context_entry.get("value")
                        
                        physical_view[node_id]["actors"][actor_id] = actor_view

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Physical view data retrieved successfully.",
                physical_view=physical_view,
            )

        except Exception as e:
            logger.error(f"Error retrieving physical view data: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, 
                message=f"Error retrieving physical view data: {str(e)}"
            )

    @routes.get("/call_graph")
    async def get_call_graph(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return the call graph data by reading from the InsightMonitor HTTP endpoint."""
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
                    f"http://{host}:{port}/get_call_graph_data",
                    params={"job_id": job_id},
                ) as response:
                    if response.status != 200:
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error from insight monitor: {await response.text()}",
                        )

                    graph_data = await response.json()

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

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
