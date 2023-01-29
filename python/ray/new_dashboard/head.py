import os
import sys
import socket
import json
import queue
import struct
import asyncio
import logging
import ipaddress
import collections

import aiohttp
import aiohttp.web
from aiohttp import hdrs
import grpc.aio as aiogrpc

import ray.gcs_utils
import ray._private.services
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
from ray import ray_constants
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import (
    DataSource,
    DataOrganizer,
    GlobalSignals,
)
from ray.new_dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()


class NodeUpdater(dashboard_utils.PSubscribeUpdater):
    # {hostname(str): dead node id queue, the end is the latest}
    _reserved_dead_nodes = collections.defaultdict(
        lambda: queue.Queue(dashboard_consts.RESERVED_DEAD_NODES_COUNT_PER_NODE)  # noqa: E501
    )

    @staticmethod
    def _gcs_node_info_to_dict(message):
        return dashboard_utils.message_to_dict(
            message, {"nodeId"}, including_default_value_fields=True)

    @classmethod
    async def _update_one_node(cls, gcs_node_info):
        node_id = gcs_node_info["nodeId"]
        state = gcs_node_info["state"]
        ip = gcs_node_info["nodeManagerAddress"]
        hostname = gcs_node_info["nodeManagerHostname"]
        nodename = gcs_node_info["nodeName"]
        assert state in ("ALIVE", "DEAD")
        if state == "ALIVE":
            # These fields never change in the life cycle of node.
            DataSource.node_id_to_ip[node_id] = ip
            DataSource.node_id_to_hostname[node_id] = hostname
            DataSource.nodes[node_id] = gcs_node_info
            # The number of nodename and hostname will not be very large,
            # and they will be reused, so they could not be gced.
            DataSource.nodename_to_hostname[nodename] = hostname
            DataSource.hostname_to_nodename[hostname] = nodename
        else:
            DataSource.nodes.pop(node_id, None)
            DataSource.dead_nodes[node_id] = gcs_node_info
            # Reserve dead nodes.
            dead_nodes = cls._reserved_dead_nodes[hostname]
            if dead_nodes.full():
                remove_node_id = dead_nodes.get()
                DataSource.node_id_to_ip.pop(remove_node_id, None)
                DataSource.node_id_to_hostname.pop(remove_node_id, None)
                DataSource.nodes.pop(remove_node_id, None)
            dead_nodes.put(node_id)
            if nodename not in DataSource.nodename_to_hostname:
                DataSource.nodename_to_hostname[nodename] = hostname
            if hostname not in DataSource.hostname_to_nodename:
                DataSource.hostname_to_nodename[hostname] = nodename

    @classmethod
    def _update_all_nodes(cls, node_info_list):
        """
        Update all nodes from GcsNodeInfo object list.
        :param node_info_list: A list of GcsNodeInfo messages.
        :return:
        """

        hostname_to_dead_nodes = collections.defaultdict(list)
        alive_nodes = {}
        node_id_to_ip = {}
        node_id_to_hostname = {}
        nodename_to_hostname = {}
        hostname_to_nodename = {}

        def move_basic_node_info(node_info_dict: dict):
            for k, v in node_info_dict["basicGcsNodeInfo"].items():
                node_info_dict[k] = v
            del node_info_dict["basicGcsNodeInfo"]

        for info in node_info_list:
            if info.basic_gcs_node_info.state == gcs_pb2.BasicGcsNodeInfo.DEAD:
                hostname_to_dead_nodes[
                    info.basic_gcs_node_info.node_manager_address].append(info)
            else:
                node_info_dict = cls._gcs_node_info_to_dict(info)
                move_basic_node_info(node_info_dict)
                node_id = node_info_dict["nodeId"]
                ip = node_info_dict["nodeManagerAddress"]
                hostname = node_info_dict["nodeManagerHostname"]
                nodename = node_info_dict["nodeName"]
                alive_nodes[node_id] = node_info_dict
                node_id_to_ip[node_id] = ip
                node_id_to_hostname[node_id] = hostname
                nodename_to_hostname[nodename] = hostname
                hostname_to_nodename[hostname] = nodename

        dead_nodes = {}
        for node_address, node_list in hostname_to_dead_nodes.items():
            nodename = None
            hostname = None
            node_list.sort(key=lambda node: node.basic_gcs_node_info.timestamp)
            node_list = node_list[
                -dashboard_consts.RESERVED_DEAD_NODES_COUNT_PER_NODE:]
            # The _reserved_dead_nodes may not be empty if GCS is restarted.
            cls._reserved_dead_nodes.pop(node_address, None)
            for info in node_list:
                node_info_dict = cls._gcs_node_info_to_dict(info)
                move_basic_node_info(node_info_dict)
                node_id = node_info_dict["nodeId"]
                nodename = node_info_dict["nodeName"]
                hostname = node_info_dict["nodeManagerHostname"]
                dead_nodes[node_id] = node_info_dict
                cls._reserved_dead_nodes[node_address].put_nowait(node_id)
            if nodename is not None and hostname is not None:
                if nodename not in nodename_to_hostname:
                    nodename_to_hostname[nodename] = hostname
                if hostname not in hostname_to_nodename:
                    hostname_to_nodename[hostname] = nodename

        DataSource.dead_nodes.reset(dead_nodes)
        DataSource.nodes.reset(alive_nodes)
        DataSource.node_id_to_ip.reset(node_id_to_ip)
        DataSource.node_id_to_hostname.reset(node_id_to_hostname)
        DataSource.nodename_to_hostname.reset(nodename_to_hostname)
        DataSource.hostname_to_nodename.reset(hostname_to_nodename)

    @classmethod
    async def _update_all(cls, gcs_channel):
        gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            gcs_channel)
        # Get all node info.
        while True:
            try:
                logger.info("Getting all node info from GCS.")
                request = gcs_service_pb2.GetAllFullNodeInfoRequest()
                reply = await gcs_node_info_stub.GetAllFullNodeInfo(
                    request, timeout=120)
                if reply.status.code == 0:
                    cls._update_all_nodes(reply.node_info_list)
                    logger.info("Received %d node info from GCS.",
                                len(reply.node_info_list))
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllNodeInfo: {reply.status.message}")
            except Exception:
                logger.exception("Error Getting all node info from GCS.")
                await asyncio.sleep(
                    dashboard_consts.RETRY_GET_ALL_NODE_INFO_INTERVAL_SECONDS)

    @classmethod
    async def _update_from_psubscribe_channel(cls, receiver):
        # Receive nodes from channel.
        def move_basic_node_info(node_info_dict: dict):
            for k, v in node_info_dict["basicGcsNodeInfo"].items():
                node_info_dict[k] = v
            del node_info_dict["basicGcsNodeInfo"]

        async for data in receiver.iter():
            try:
                data = data["data"]
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(data)
                node_info = ray.gcs_utils.GcsNodeInfo.FromString(
                    pubsub_message.data)
                node_info_dict = cls._gcs_node_info_to_dict(node_info)
                move_basic_node_info(node_info_dict)
                await cls._update_one_node(node_info_dict)
            except Exception:
                logger.exception("Error receiving node info.")


class DashboardHead:
    def __init__(
            self,
            http_host,
            http_port,
            http_port_retries,
            redis_address,
            redis_password,
            temp_dir,
            log_dir,
    ):
        # NodeInfoGcsService
        self._gcs_node_info_stub = None
        self._gcs_heartbeat_info_stub = None
        self._gcs_check_alive_seq = 0
        self._gcs_rpc_error_counter = 0
        # Redis error counter
        self._redis_error_counter = 0
        # Public attributes are accessible for all head modules.
        # Walkaround for issue: https://github.com/ray-project/ray/issues/7084
        self.http_host = "127.0.0.1" if http_host == "localhost" else http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries
        self.redis_address = dashboard_utils.address_tuple(redis_address)
        self.redis_password = redis_password
        self.temp_dir = temp_dir
        self.log_dir = log_dir
        self.aioredis_client = None
        self.aiogrpc_gcs_channel = None
        self.http_session = None
        self.ip = ray.util.get_node_ip_address()
        self.server = aiogrpc.server(
            options=(("grpc.so_reuseport", 0), ) +
            dashboard_consts.GLOBAL_GRPC_OPTIONS)
        listen_address = dashboard_utils.grpc_force_ipv4()
        self.grpc_port = self.server.add_insecure_port(listen_address)
        self._init_stats()
        logger.info("Dashboard head grpc address: %s:%s", self.ip,
                    self.grpc_port)
        logger.info("Dashboard head http address: %s:%s", self.http_host,
                    self.http_port)

    def _init_stats(self):
        global_tags = {
            "ClusterName": os.environ.get("cluster_name", ""),
            "Component": "dashboard",
            "Ip": self.ip,
            "Host": socket.gethostname(),
            "Pid": str(os.getpid()),
        }
        dashboard_utils.init_stats(global_tags, app_name="dashboard_stats")

    @async_loop_forever(dashboard_consts.UPDATE_AGENTS_INTERVAL_SECONDS)
    async def _update_agents(self):
        all_node_ids = DataSource.agents.keys()
        alive_node_ids = set()
        query_keys = []
        for node_id, node in DataSource.nodes.items():
            if node["state"] == "ALIVE":
                alive_node_ids.add(node_id)
                k = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{node_id}"
                query_keys.append(k)
        # Remove old values first.
        for node_id in all_node_ids - alive_node_ids:
            DataSource.agents.pop(node_id, None)
        # Then, add new values.
        if query_keys:
            agent_data = await self.aioredis_client.mget(
                query_keys[0], *query_keys[1:])
            for key, data in zip(query_keys, agent_data):
                if data:
                    node_id = key[len(
                        dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX):]
                    DataSource.agents[node_id] = json.loads(data)

    async def _check_redis_alive(self):
        aioredis_client = self.aioredis_client
        seq = [0, 0]  # [sender, receiver]
        max_unreceived = \
            dashboard_consts.REDIS_CHECK_ALIVE_MAX_NUM_OF_UNRECEIVED_MESSAGES

        async def _sender():
            while True:
                try:
                    seq[0] += 1
                    if seq[0] > seq[1] + max_unreceived:
                        logger.error(
                            "Dashboard suicide, the redis check alive "
                            "sender %d, receiver %d, more than %d messages "
                            "are not received.", seq[0], seq[1],
                            max_unreceived)
                        os._exit(-1)
                    await aioredis_client.publish(
                        dashboard_consts.REDIS_CHECK_ALIVE_CHANNEL,
                        struct.pack("=Q", seq[0]))
                except Exception:
                    logger.exception("Error publishing check alive messages.")
                await asyncio.sleep(
                    dashboard_consts.REDIS_CHECK_ALIVE_INTERVAL_SECONDS)

        async def _receiver():
            check_alive_key = f"{dashboard_consts.REDIS_CHECK_ALIVE_CHANNEL}*"
            pubsub = aioredis_client.pubsub()
            pubsub.psubscribe(check_alive_key)
            logger.info("Subscribed to %s.", check_alive_key)

            async for data in pubsub.iter():
                try:
                    data = data["data"]
                    received = struct.unpack("=Q", data)[0]
                    seq[1] = received
                except Exception:
                    logger.exception("Error receiving check alive messages.")

            logger.error("Dashboard suicide due to redis connection lost.")
            os._exit(-1)

        return await asyncio.gather(_sender(), _receiver())

    @async_loop_forever(dashboard_consts.GCS_CHECK_ALIVE_INTERVAL_SECONDS)
    async def _auto_reconnect_gcs(self):
        try:
            self._gcs_check_alive_seq += 1
            request = gcs_service_pb2.CheckAliveRequest(
                seq=self._gcs_check_alive_seq)
            logger.info("[CheckAlive][%d] request", self._gcs_check_alive_seq)
            reply = await self._gcs_heartbeat_info_stub.CheckAlive(
                request, timeout=2)
            if reply.status.code != 0:
                raise Exception(
                    f"Failed to CheckAlive: {reply.status.message}")
            logger.info("[CheckAlive][%d] reply", self._gcs_check_alive_seq)
            self._gcs_rpc_error_counter = 0
        except aiogrpc.AioRpcError:
            logger.exception(
                "[CheckAlive][%d] Got AioRpcError when checking GCS is alive.",
                self._gcs_check_alive_seq)
            self._gcs_rpc_error_counter += 1
            if self._gcs_rpc_error_counter > \
                    dashboard_consts.GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR:
                logger.error(
                    "[CheckAlive][%d] The GCS RPC error count %s > %s, "
                    "reconnecting...", self._gcs_check_alive_seq,
                    self._gcs_rpc_error_counter,
                    dashboard_consts.GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR)
                kwargs = await self._get_gcs_channel_kwargs()
                await self.aiogrpc_gcs_channel.reconnect(**kwargs)
                await GlobalSignals.gcs_reconnected.send()
        except aiogrpc.UsageError:
            logger.exception(
                "[CheckAlive][%d] Got UsageError when checking GCS is alive.",
                self._gcs_check_alive_seq)
            kwargs = await self._get_gcs_channel_kwargs()
            await self.aiogrpc_gcs_channel.reconnect(**kwargs)
            await GlobalSignals.gcs_reconnected.send()
        except Exception:
            logger.exception("[CheckAlive][%d] Error checking GCS is alive.",
                             self._gcs_check_alive_seq)

    async def _get_gcs_channel_kwargs(self):
        gcs_address = await self.aioredis_client.get(
            dashboard_consts.REDIS_KEY_GCS_SERVER_ADDRESS)
        if not gcs_address:
            raise Exception("GCS RPC address is not found.")
        logger.info("Found GCS RPC address: %s", gcs_address)
        return {
            "target": gcs_address,
            "options": dashboard_consts.GLOBAL_GRPC_OPTIONS,
        }

    def _load_modules(self):
        """Load dashboard head modules."""
        modules = []
        head_cls_list = dashboard_utils.get_all_modules(
            dashboard_utils.DashboardHeadModule)
        for cls in head_cls_list:
            logger.info("Loading %s: %s",
                        dashboard_utils.DashboardHeadModule.__name__, cls)
            c = cls(self)
            dashboard_utils.ClassMethodRouteTable.bind(c)
            modules.append(c)
        logger.info("Loaded %d modules.", len(modules))
        return modules

    async def run(self):
        # Create an aioredis client for all modules.
        try:
            host, port = self.redis_address
            self.aioredis_client = dashboard_utils.RedisClientThread(
                host, port, self.redis_password)
            self.aioredis_client.start()
        except (socket.gaierror, ConnectionError):
            logger.error(
                "Dashboard head exiting: "
                "Failed to connect to redis at %s", self.redis_address)
            sys.exit(-1)

        # Create a http session for all modules.
        self.http_session = aiohttp.ClientSession(
            loop=asyncio.get_event_loop())

        # Waiting for GCS is ready.
        while True:
            try:
                kwargs = await self._get_gcs_channel_kwargs()
                channel = dashboard_utils.insecure_channel(**kwargs)
            except Exception as ex:
                logger.exception("Connect to GCS failed: %s, retry...", ex)
                await asyncio.sleep(
                    dashboard_consts.GCS_RETRY_CONNECT_INTERVAL_SECONDS)
            else:
                self.aiogrpc_gcs_channel = channel
                break

        # Create a NodeInfoGcsServiceStub.
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            self.aiogrpc_gcs_channel)

        # Create a NamespaceInfoGcsServiceStub.
        self._gcs_heartbeat_info_stub = \
            gcs_service_pb2_grpc.HeartbeatInfoGcsServiceStub(
                self.aiogrpc_gcs_channel)

        # Start a grpc asyncio server.
        await self.server.start()

        # Create or get cluster unique token.
        await dashboard_utils.ClusterTokenManager.create_or_get(
            self.aioredis_client)

        async def _async_notify():
            """Notify signals from queue."""
            while True:
                co = await dashboard_utils.NotifyQueue.get()
                try:
                    await co
                except Exception:
                    logger.exception(f"Error notifying coroutine {co}")

        modules = self._load_modules()

        # Http server should be initialized after all modules loaded.
        app = aiohttp.web.Application()
        app.add_routes(routes=routes.bound_routes())

        runner = aiohttp.web.AppRunner(
            app, access_log_format=dashboard_consts.WEB_ACCESS_LOG_FORMAT)
        await runner.setup()
        last_ex = None
        for _ in range(1 + self.http_port_retries):
            try:
                site = aiohttp.web.TCPSite(runner, self.http_host,
                                           self.http_port)
                await site.start()
                break
            except OSError as e:
                last_ex = e
                self.http_port += 1
                logger.warning("Try to use port %s: %s", self.http_port, e)
        else:
            raise Exception(f"Failed to find a valid port for dashboard after "
                            f"{self.http_port_retries} retries: {last_ex}")
        http_host, http_port, *_ = site._server.sockets[0].getsockname()
        http_host = self.ip if ipaddress.ip_address_original(
            http_host).is_unspecified else http_host
        logger.info("Dashboard head http address: %s:%s", http_host, http_port)

        # ANT-INTERNAL: Callbacks for dashboard restarted.
        old_address = await self.aioredis_client.get(
            ray_constants.REDIS_KEY_DASHBOARD)

        # Write the dashboard head port to redis.
        await self.aioredis_client.set(ray_constants.REDIS_KEY_DASHBOARD,
                                       f"{http_host}:{http_port}")
        await self.aioredis_client.set(
            dashboard_consts.REDIS_KEY_DASHBOARD_RPC,
            f"{self.ip}:{self.grpc_port}")

        # Dump registered http routes.
        dump_routes = [
            r for r in app.router.routes() if r.method != hdrs.METH_HEAD
        ]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))

        GlobalSignals.gcs_reconnected.append(
            NodeUpdater.create(f"{dashboard_consts.FULL_NODE_INFO_CHANNEL}:*",
                               self.aioredis_client,
                               self.aiogrpc_gcs_channel).restart)

        # Freeze signal after all modules loaded.
        dashboard_utils.SignalManager.freeze()

        # ANT-INTERNAL: Callbacks for dashboard restarted.
        if old_address:
            await GlobalSignals.dashboard_restarted.send(
                old_address, f"{http_host}:{http_port}")

        concurrent_tasks = [
            self._update_agents(),
            self._auto_reconnect_gcs(),
            self._check_redis_alive(),
            _async_notify(),
            DataOrganizer.organize(),
            DataOrganizer.log_info(),
        ]

        await asyncio.gather(*concurrent_tasks,
                             *(m.run(self.server) for m in modules))
        await self.server.wait_for_termination()
