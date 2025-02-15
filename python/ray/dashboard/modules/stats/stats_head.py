import asyncio
import logging
import time
from typing import Dict, List, Optional

import aiohttp.web
from ray.dashboard.modules.stats.stats_utils import (
    calculate_resource_utilization,
    calculate_node_metrics,
    calculate_actor_stats,
    calculate_job_stats,
    calculate_advanced_metrics,
)
from ray.dashboard.utils import rest_response

logger = logging.getLogger(__name__)
routes = aiohttp.web.RouteTableDef()

@routes.get("/api/cluster/info")
async def get_cluster_info(req) -> aiohttp.web.Response:
    """Get basic cluster information."""
    dashboard_head = req.app["dashboard_head"]
    nodes = await dashboard_head.get_all_node_info()
    
    alive_nodes = sum(1 for node in nodes if node["alive"])
    dead_nodes = len(nodes) - alive_nodes
    
    resource_usage = calculate_resource_utilization(nodes)
    
    return rest_response(
        {
            "aliveNodes": alive_nodes,
            "deadNodes": dead_nodes,
            "totalCpuUsage": resource_usage["cpu"],
            "totalMemoryUsage": resource_usage["memory"],
            "totalGpuUsage": resource_usage.get("gpu"),
        }
    )

@routes.get("/api/cluster/metrics")
async def get_cluster_metrics(req) -> aiohttp.web.Response:
    """Get detailed cluster metrics over time."""
    dashboard_head = req.app["dashboard_head"]
    metrics = await dashboard_head.get_cluster_metrics()
    
    return rest_response(metrics)

@routes.get("/api/nodes/metrics")
async def get_node_metrics(req) -> aiohttp.web.Response:
    """Get detailed node metrics."""
    dashboard_head = req.app["dashboard_head"]
    nodes = await dashboard_head.get_all_node_info()
    
    metrics = calculate_node_metrics(nodes)
    return rest_response(metrics)

@routes.get("/api/actors/groups")
async def get_actor_groups(req) -> aiohttp.web.Response:
    """Get actor statistics grouped by class."""
    dashboard_head = req.app["dashboard_head"]
    actors = await dashboard_head.get_all_actor_info()
    
    stats = calculate_actor_stats(actors)
    return rest_response(stats)

@routes.get("/api/jobs/stats")
async def get_job_stats(req) -> aiohttp.web.Response:
    """Get job statistics and metrics."""
    dashboard_head = req.app["dashboard_head"]
    jobs = await dashboard_head.get_all_job_info()
    
    stats = calculate_job_stats(jobs)
    return rest_response(stats)

@routes.get("/api/analytics/metrics")
async def get_advanced_metrics(req) -> aiohttp.web.Response:
    """Get advanced analytics and correlations."""
    dashboard_head = req.app["dashboard_head"]
    
    # Gather all necessary data
    nodes = await dashboard_head.get_all_node_info()
    actors = await dashboard_head.get_all_actor_info()
    jobs = await dashboard_head.get_all_job_info()
    
    metrics = calculate_advanced_metrics(nodes, actors, jobs)
    return rest_response(metrics)

async def init_stats_head(dashboard_head):
    """Initialize the stats module."""
    dashboard_head.app.add_routes(routes) 