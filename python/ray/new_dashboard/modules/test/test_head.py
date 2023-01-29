import os
import time
import uuid
import tempfile
import logging
import asyncio
import threading

import aiohttp.web
import grpc.aio as aiogrpc

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.modules.test.test_utils as test_utils
import ray.new_dashboard.modules.test.test_consts as test_consts
from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.datacenter import DataSource
from ray.ray_constants import env_bool
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.dashboard_module(
    enable=env_bool(test_consts.TEST_MODULE_ENVIRONMENT_KEY, False))
class TestHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._notified_agents = {}
        self._test_reconnect_channel = None
        self._test_reconnect_agent_stub = None
        self._test_reconnect_result = []
        self._gcs_heartbeat_info_stub = None
        DataSource.agents.signal.append(self._update_notified_agents)

    async def _update_notified_agents(self, change):
        if change.old:
            ip, port = change.old
            self._notified_agents.pop(ip)
        if change.new:
            ip, agent_data = change.new
            port = agent_data[dashboard_consts.DASHBOARD_AGENT_GRPC_PORT]
            self._notified_agents[ip] = agent_data
            if self._test_reconnect_channel is None:
                target = f"{self._dashboard_head.ip}:{port}"
                self._test_reconnect_channel = \
                    dashboard_utils.insecure_channel(
                        target, options=dashboard_consts.GLOBAL_GRPC_OPTIONS)
                self._test_reconnect_agent_stub = \
                    gcs_service_pb2_grpc.HeartbeatInfoGcsServiceStub(
                        self._test_reconnect_channel)
                self._test_reconnect_result.append("Connect")
            else:
                target = f"{self._dashboard_head.ip}:{port}"
                await self._test_reconnect_channel.reconnect(target)
                self._test_reconnect_result.append("Reconnect")

    @async_loop_forever(1)
    async def _test_reconnect_rpc(self):
        if not self._test_reconnect_agent_stub:
            return
        try:
            request = gcs_service_pb2.CheckAliveRequest()
            reply = await self._test_reconnect_agent_stub.CheckAlive(request)
            self._test_reconnect_result.append(reply.status.message)
            logger.info("Test auto reconnect rpc: %s", reply.status.message)
        except aiogrpc.AioRpcError as ex:
            self._test_reconnect_result.append(str(ex.code()))
            logger.error(ex)

    @routes.get("/test/reconnect_result")
    async def get_reconnect_result(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="Success",
            reconnect_sequence=self._test_reconnect_result)

    @routes.get("/test/check_gcs_alive")
    async def check_gcs_alive(self, req) -> aiohttp.web.Response:
        request = gcs_service_pb2.CheckAliveRequest()
        reply = await self._gcs_heartbeat_info_stub.CheckAlive(
            request, timeout=2)
        if reply.status.code != 0:
            raise Exception(f"Failed to CheckAlive: {reply.status.message}")
        return dashboard_utils.rest_response(success=True, message="Success")

    @routes.get("/test/route_get")
    async def route_get(self, req) -> aiohttp.web.Response:
        pass

    @routes.put("/test/route_put")
    async def route_put(self, req) -> aiohttp.web.Response:
        pass

    @routes.delete("/test/route_delete")
    async def route_delete(self, req) -> aiohttp.web.Response:
        pass

    @routes.view("/test/route_view")
    async def route_view(self, req) -> aiohttp.web.Response:
        pass

    @routes.post("/test/clear_datasource")
    async def clear_datasource(self, req) -> aiohttp.web.Response:
        for k, v in DataSource.__dict__.items():
            if not k.startswith("_"):
                logger.info(f"clear DataSource.{k}")
                v.reset({})
        return dashboard_utils.rest_response(
            success=True, message="clear datasource success.")

    @routes.get("/test/dump")
    async def dump(self, req) -> aiohttp.web.Response:
        key = req.query.get("key")
        if key is None:
            all_data = {
                k: dict(v)
                for k, v in DataSource.__dict__.items()
                if not k.startswith("_")
            }
            return dashboard_utils.rest_response(
                success=True,
                message="Fetch all data from datacenter success.",
                **all_data)
        else:
            data = dict(DataSource.__dict__.get(key))
            return dashboard_utils.rest_response(
                success=True,
                message=f"Fetch {key} from datacenter success.",
                **{key: data})

    @routes.get("/test/notified_agents")
    async def get_notified_agents(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="Fetch notified agents success.",
            **self._notified_agents)

    @routes.get("/test/http_get")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self._dashboard_head.http_session,
                                           url)
        return aiohttp.web.json_response(result)

    @routes.get("/test/aiohttp_cache/{sub_path}")
    @dashboard_utils.aiohttp_cache(
        ttl_seconds=1,
        include_filter=lambda req: int(req.query.get("value", 0)) <= 10,
        exclude_filter=lambda req: int(req.query.get("value", 0)) in [10, 11])
    async def test_aiohttp_cache(self, req) -> aiohttp.web.Response:
        value = req.query["value"]
        return dashboard_utils.rest_response(
            success=True, message="OK", value=value, timestamp=time.time())

    @routes.get("/test/aiohttp_cache_lru/{sub_path}")
    @dashboard_utils.aiohttp_cache(ttl_seconds=60, maxsize=5)
    async def test_aiohttp_cache_lru(self, req) -> aiohttp.web.Response:
        value = req.query.get("value")
        return dashboard_utils.rest_response(
            success=True, message="OK", value=value, timestamp=time.time())

    @routes.get("/test/aiohttp_cache_exception")
    @dashboard_utils.aiohttp_cache(cache_exception=True)
    async def test_aiohttp_cache_exception(self, req) -> aiohttp.web.Response:
        raise Exception("test exception")

    @routes.get("/test/aiohttp_not_cache_exception")
    @dashboard_utils.aiohttp_cache(cache_exception=False)
    async def test_aiohttp_not_cache_exception(self,
                                               req) -> aiohttp.web.Response:
        raise Exception("test exception")

    @routes.get("/test/file")
    async def test_file(self, req) -> aiohttp.web.FileResponse:
        file_path = req.query.get("path")
        logger.info("test file: %s", file_path)
        return aiohttp.web.FileResponse(file_path)

    @routes.get("/test/frontend_url")
    async def get_frontend_url(self, req) -> aiohttp.web.FileResponse:
        temp_file = tempfile.mktemp(".zip")
        temp_dir = tempfile.mkdtemp()
        temp_build_static = os.path.join(temp_dir, "build/static")
        os.makedirs(temp_build_static)
        logger.info("Temp build static dir: %s", temp_build_static)
        logger.info("Temp file: %s", temp_file)
        r = os.system(f"cp {__file__} {temp_build_static}")
        if r != 0:
            raise Exception("Create test frontend url failed.")
        os.chdir(temp_dir)
        r = os.system(f"zip -r {temp_file} build")
        if r != 0:
            raise Exception("Create test frontend url failed.")
        return aiohttp.web.FileResponse(temp_file)

    @routes.get("/test/url_with_same_etag")
    async def get_url_with_same_etag(self, req) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            headers={
                "Etag": '"6B9B95BB5F79811E16D5BD4B4DA56EF8-1"',
                "Last-Modified": "Thu, 28 Jan 2021 14:42:35 GMT"
            })

    @routes.get("/test/url_with_unique_etag")
    async def get_url_with_unique_etag(self, req) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            headers={
                "Etag": f'"{uuid.uuid4()}"',
                "Last-Modified": "Thu, 28 Jan 2021 14:42:35 GMT"
            })

    @routes.get("/test/url_without_etag")
    async def get_url_without_etag(self, req) -> aiohttp.web.Response:
        return aiohttp.web.Response()

    @routes.get("/test/pubsub_thread_number")
    async def get_pubsub_thread_number(self, req) -> aiohttp.web.Response:
        count = 0
        for thread in threading.enumerate():
            if thread.name == "RedisClientPubsubThread":
                count += 1
        return dashboard_utils.rest_response(
            success=True, message="OK", value=count, timestamp=time.time())

    async def run(self, server):
        self._gcs_heartbeat_info_stub = \
            gcs_service_pb2_grpc.HeartbeatInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
        await asyncio.gather(self._test_reconnect_rpc())
