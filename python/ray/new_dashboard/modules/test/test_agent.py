import logging
import time

import aiohttp.web

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.test.test_utils as test_utils
import ray.new_dashboard.modules.test.test_consts as test_consts
from ray.ray_constants import env_bool
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.dashboard_module(
    enable=env_bool(test_consts.TEST_MODULE_ENVIRONMENT_KEY, False))
class TestAgent(dashboard_utils.DashboardAgentModule,
                gcs_service_pb2_grpc.HeartbeatInfoGcsServiceServicer):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    async def CheckAlive(self, request, context):
        return gcs_service_pb2.CheckAliveReply(
            status=gcs_service_pb2.GcsStatus(code=1, message="OK"))

    @routes.get("/test/http_get_from_agent")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self._dashboard_agent.http_session,
                                           url)
        return aiohttp.web.json_response(result)

    @routes.head("/test/route_head")
    async def route_head(self, req) -> aiohttp.web.Response:
        pass

    @routes.post("/test/route_post")
    async def route_post(self, req) -> aiohttp.web.Response:
        pass

    @routes.patch("/test/route_patch")
    async def route_patch(self, req) -> aiohttp.web.Response:
        pass

    @routes.get("/test/agent_hang")
    async def hang(self, req) -> aiohttp.web.Response:
        time.sleep(1000)
        return aiohttp.web.json_response()

    async def run(self, server):
        gcs_service_pb2_grpc.add_HeartbeatInfoGcsServiceServicer_to_server(
            self, server)
