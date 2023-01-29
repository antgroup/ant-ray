import logging
import os
import shutil
import socket
import datetime
import asyncio

import aiohttp.web
import async_timeout
import yaml
import json

import ray
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.internal.internal_consts as internal_consts
from ray.new_dashboard.datacenter import GlobalSignals, DataSource
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from google.protobuf import json_format

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class InternalHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # NodegroupInfoGcsServiceStub
        self._gcs_nodegroup_stub = None

        self._update_frontend_counter = 0
        self._data_source = os.environ.get("METRIC_DATA_SOURCE", "")
        self._cluster_name = os.environ.get("CLUSTER_NAME", "empty_host")
        self._log_url = os.environ.get("LOG_URL",
                                       dashboard_consts.DEFAULT_LOG_URL)
        self._event_url = os.environ.get("EVENT_URL",
                                         internal_consts.DEFAULT_EVENT_URL)
        self._metric_url = os.environ.get("METRIC_URL",
                                          internal_consts.DEFAULT_METRIC_URL)
        self._ray_config = None
        self._gcs_actor_info_stub = None
        self._gcs_frozen_node_stub = None
        GlobalSignals.node_info_fetched.append(
            self._generate_sls_url_for_node_info)
        GlobalSignals.node_summary_fetched.append(
            self._generate_sls_url_for_node_info)
        GlobalSignals.job_info_fetched.extend([
            self._generate_sls_url_for_job_info,
            self._generate_event_url_for_job_info,
            self._generate_metric_url_for_job_info,
        ])
        GlobalSignals.worker_info_fetched.append(
            self._generate_sls_url_for_worker_info)

    @routes.get("/ray_features")
    @dashboard_utils.aiohttp_cache(60 * 30, cache_exception=False)
    async def get_features(self, req) -> aiohttp.web.Response:
        with open(internal_consts.RAY_FEATURES_FILE, "rb") as f:
            features = list(set(yaml.load(f, Loader=yaml.FullLoader)))
        return dashboard_utils.rest_response(
            success=True, message="Ray features fetched.", features=features)

    @routes.get("/ray_config")
    @dashboard_utils.aiohttp_cache(60 * 30, cache_exception=False)
    async def get_ray_config(self, req) -> aiohttp.web.Response:
        url = os.environ.get("ray_config_file")
        if not url:
            return dashboard_utils.rest_response(
                success=False,
                message="Failed to fetch ray config, "
                "the ray_config_file env var not exists")

        dashboard_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/dashboard.*&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname}"

        cluster_event = self._event_url + "&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name}"

        gcs_server_out = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/gcs_server.*out&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname}"

        gcs_server_err = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/gcs_server.*err&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname}"

        hostname = socket.gethostname()
        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        start_time = int(start_time.timestamp() * 1000)
        sls_url = {
            "dashboard": dashboard_log.format(
                start_time=start_time,
                hostname=hostname,
                cluster_name=self._cluster_name),
            "event": cluster_event.format(
                start_time=start_time, cluster_name=self._cluster_name),
            "gcs_server.out": gcs_server_out.format(
                start_time=start_time,
                hostname=hostname,
                cluster_name=self._cluster_name),
            "gcs_server.err": gcs_server_err.format(
                start_time=start_time,
                hostname=hostname,
                cluster_name=self._cluster_name)
        }
        metric_url = f"{self._metric_url}/d/ray_cluster_metrics?" \
                     f"var-clusterName={self._cluster_name}"
        if self._data_source:
            metric_url = f"{metric_url}&var-DATA_SOURCE={self._data_source}"

        if self._ray_config is None:
            with async_timeout.timeout(
                    internal_consts.DOWNLOAD_CONFIG_TIMEOUT_SECONDS):
                async with self._dashboard_head.http_session.get(
                        url) as response:
                    logger.info("Get ray config from %s", url)
                    config_yaml = await response.read()
                    self._ray_config = yaml.load(
                        config_yaml, Loader=yaml.FullLoader)
        self._ray_config["slsUrl"] = sls_url
        self._ray_config["metricUrl"] = metric_url
        self._ray_config["headImageUrl"] = os.environ.get(
            "pouch_container_image")
        return dashboard_utils.rest_raw_response(
            success=True,
            message="Ray config fetched.",
            config=self._ray_config)

    async def _generate_sls_url_for_worker_info(self, node_id, worker):
        hostname = DataSource.node_id_to_hostname.get(node_id)
        if hostname is None:
            return

        worker_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/{sls_pattern}&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname} and " \
            "__tag__:__path__: /home/admin/logs/ray-logs/logs/{filename}.log"

        worker_filename = {
            "PYTHON": "python-worker-{}-{}",
            "JAVA": "java-worker-{}-{}",
            "CPP": "cpp-core-worker-{}-{}"
        }
        worker_sls_pattern = {
            "PYTHON": "python-worker-*.log",
            "JAVA": "java-worker*.log",
            "CPP": "cpp-core-worker*.log",
        }

        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        language = worker["language"]
        filename = worker_filename.get(language, "")
        filename = filename.format(worker["jobId"], worker["pid"])
        sls_pattern = worker_sls_pattern.get(language, "")
        worker["slsUrl"] = worker_log.format(
            sls_pattern=sls_pattern,
            start_time=start_time,
            hostname=hostname,
            cluster_name=self._cluster_name,
            filename=filename)

    async def _generate_sls_url_for_node_info(self, node_info):
        hostname = node_info.get("raylet", {}).get("nodeManagerHostname")
        if not hostname:
            logger.error("Can't find hostname in node info: %s", node_info)
            return

        node_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/raylet.*out&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname}"

        dashboard_agent_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/dashboard_agent.*&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname}"

        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        start_time = int(start_time.timestamp() * 1000)
        node_info["slsUrl"] = {
            "raylet": node_log.format(
                start_time=start_time,
                hostname=hostname,
                cluster_name=self._cluster_name),
            "dashboardAgent": dashboard_agent_log.format(
                start_time=start_time,
                hostname=hostname,
                cluster_name=self._cluster_name)
        }

    async def _generate_sls_url_for_job_info(self, job_detail):
        driver_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/driver-*&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__hostname__: {hostname} and " \
            "__tag__:__path__: /home/admin/logs/ray-logs/logs/driver-{job_id}"

        job_log = self._log_url + "&" \
            "logFullPath=/home/admin/logs/ray-logs/logs/{worker_filename}&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "__tag__:__job_id__: {job_id}"

        worker_filename = {
            "PYTHON": "python-worker-*.log",
            "JAVA": "java-worker*.log",
            "CPP": "cpp-core-worker*.log",
        }

        job_info = dict(job_detail["jobInfo"])
        hostname = job_info["driverHostname"]
        job_id = job_info["jobId"]
        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        start_time = int(start_time.timestamp() * 1000)
        driver_log = driver_log.format(
            start_time=start_time,
            hostname=hostname,
            cluster_name=self._cluster_name,
            job_id=job_id)
        sls_url = [driver_log + ".err", driver_log + ".out"]
        sls_url = {os.path.basename(url): url for url in sls_url}
        for key, value in worker_filename.items():
            sls_url["jobLog." + key] = job_log.format(
                start_time=start_time,
                cluster_name=self._cluster_name,
                job_id=job_id,
                worker_filename=value)
        job_info["slsUrl"] = sls_url
        job_detail["jobInfo"] = job_info

    async def _generate_event_url_for_job_info(self, job_detail):
        job_event = self._event_url + "&" \
            "startTime={start_time}&" \
            "query=* and " \
            "__tag__:APPSOURCE: {cluster_name} and " \
            "job_id: {job_id}"

        job_info = dict(job_detail["jobInfo"])
        job_id = job_info["jobId"]
        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        start_time = int(start_time.timestamp() * 1000)
        job_info["eventUrl"] = job_event.format(
            start_time=start_time,
            cluster_name=self._cluster_name,
            job_id=job_id)
        job_detail["jobInfo"] = job_info

    async def _generate_metric_url_for_job_info(self, job_detail):
        if "metricTemplate" not in job_detail["jobInfo"]:
            logger.info(
                "Not generate metric url for job %s, "
                "not found 'metricTemplate' in job info.",
                job_detail["jobInfo"]["jobId"])
            return
        job_metric = self._metric_url + "/d/{uid}?var-JobName={job_name}"
        if self._data_source:
            job_metric = f"{job_metric}&var-DATA_SOURCE={self._data_source}"

        job_info = dict(job_detail["jobInfo"])
        job_name = job_info["name"]
        metric_template = job_info["metricTemplate"]
        job_info["metricUrl"] = job_metric.format(
            uid=metric_template, job_name=job_name)
        job_detail["jobInfo"] = job_info

    @routes.get("/dump")
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

    @routes.put("/frontend")
    async def update_frontend(self, req) -> aiohttp.web.Response:
        update_body = dict(await req.json())
        url = update_body["url"]
        self._update_frontend_counter += 1
        index = self._update_frontend_counter
        logger.info("Downloading front-end[%d] from url %s", index, url)
        filename = os.path.join(self._dashboard_head.temp_dir,
                                internal_consts.UPDATE_FRONTEND_FILENAME)
        with async_timeout.timeout(
                internal_consts.DOWNLOAD_CONFIG_TIMEOUT_SECONDS):
            async with self._dashboard_head.http_session.get(url) as response:
                with open(filename, "wb") as f:
                    while True:
                        chunk = await response.content.read(
                            internal_consts.DOWNLOAD_BUFFER_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)
        new_dashboard_path = ray.new_dashboard.__path__[0]
        client_build_dir = os.path.join(new_dashboard_path, "client/build")
        client_build_bak = os.path.join(new_dashboard_path, "client/build_bak")
        client_build_tmp = os.path.join(new_dashboard_path, "client/build_tmp")
        logger.info("Remove temp front-end[%d] %s", index, client_build_tmp)
        shutil.rmtree(client_build_tmp, ignore_errors=True)
        unzip_cmd = f"unzip -o -d {client_build_tmp} {filename}"
        proc = await asyncio.create_subprocess_shell(
            unzip_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        logger.info("Unzip front-end[%d] cmd: %s", index, repr(unzip_cmd))
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0 and stderr:
            raise Exception(stderr.decode("utf-8"))
        logger.info("Unzip front-end[%d] output: %s", index,
                    stdout.decode("utf-8"))
        shutil.rmtree(client_build_bak, ignore_errors=True)
        assert os.path.exists(os.path.join(client_build_tmp, "build/static"))
        logger.info("Backup front-end[%d] %s to %s", index, client_build_dir,
                    client_build_bak)
        shutil.move(client_build_dir, client_build_bak)
        client_build = os.path.join(client_build_tmp, "build")
        logger.info("Update front-end[%d] %s to %s", index, client_build,
                    client_build_dir)
        shutil.move(client_build, client_build_dir)
        logger.info("Update front-end[%d] success.", index)
        return dashboard_utils.rest_response(
            success=True, message="Updated front-end.", url=url, index=index)

    @routes.get("/clusterToken")
    async def get_cluster_token(self, req) -> aiohttp.web.Response:
        token = await dashboard_utils.ClusterTokenManager.create_or_get(
            self._dashboard_head.aioredis_client)
        return dashboard_utils.rest_response(
            success=True, message="Cluster token fetched", token=token)

    @routes.get("/scheduler/job_distribution")
    @dashboard_utils.aiohttp_cache(10)
    async def job_distribution(self, req) -> aiohttp.web.Response:
        reply = await self._gcs_actor_info_stub.GetJobDistribution(
            gcs_service_pb2.GetJobDistributionRequest())
        data = dashboard_utils.message_to_dict(
            reply, {"nodeId", "jobId", "workerProcessId"},
            including_default_value_fields=True)
        return dashboard_utils.rest_response(
            success=True, message="Get job distribution success.", **data)

    @routes.get("/scheduler/cluster_resources")
    @dashboard_utils.aiohttp_cache(10)
    async def cluster_resources(self, req) -> aiohttp.web.Response:
        reply = await self._gcs_node_resource_info_stub.GetClusterResources(
            gcs_service_pb2.GetClusterResourcesRequest())
        data = dashboard_utils.message_to_dict(
            reply, {"nodeId"}, including_default_value_fields=True)
        for node in data["nodeResources"]:
            for key in node["totalResources"]:
                if key.startswith("memory"):
                    node["totalResources"][key] /= (1024**2)
            for key in node["availableResources"]:
                if key.startswith("memory"):
                    node["availableResources"][key] /= (1024**2)
        return dashboard_utils.rest_response(
            success=True, message="Get cluster resources success.", **data)

    def _difference(self, input_elem_list, all_elem_dict):
        diff = []
        for elem in input_elem_list:
            if elem not in all_elem_dict:
                diff.append(elem)
        return diff

    @routes.post("/scheduler/freeze_nodes")
    async def freeze_nodes(self, req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /scheduler/freeze_nodes %s", request_json)

        body = dict(request_json)
        node_name_set = body.get("nodeNameSet", [])
        if len(node_name_set) == 0:
            node_name_set = body.get("node_name_set", [])

        if len(node_name_set) == 0:
            host_name_list = body.get("hostNameList", [])
            diff = self._difference(host_name_list,
                                    DataSource.hostname_to_nodename)
            if len(diff) > 0:
                code = 4  # Invalid
                message = f"Invalid nodes: {diff}"
                logger.info("%s, code = %s", message, code)
                return dashboard_utils.rest_response(
                    success=False, code=code, message=message)

            node_name_set = [
                DataSource.hostname_to_nodename[host_name]
                for host_name in host_name_list
            ]

        if len(node_name_set) == 0:
            code = 0
            message = "Succeed in freezing nodes."
            logger.info("%s, code = %s", message, code)
            return dashboard_utils.rest_response(
                success=True, code=code, message=message)

        diff = self._difference(node_name_set, DataSource.nodename_to_hostname)
        if len(diff) > 0:
            code = 4  # Invalid
            message = f"Invalid nodes: {diff}"
            logger.info("%s, code = %s", message, code)
            return dashboard_utils.rest_response(
                success=False, code=code, message=message)

        request_pb = gcs_service_pb2.FreezeNodesRequest(
            node_name_set=node_name_set)
        reply = await self._gcs_frozen_node_stub.FreezeNodes(request_pb)

        message = reply.status.message
        if reply.status.code == 0:
            message = "Succeed in freezing nodes."
        logger.info("%s, code = %s", message, reply.status.code)

        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message)

    @routes.post("/scheduler/unfreeze_nodes")
    async def unfreeze_nodes(self, req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /scheduler/unfreeze_nodes %s", request_json)

        body = dict(request_json)
        node_name_set = body.get("nodeNameSet", [])
        if len(node_name_set) == 0:
            node_name_set = body.get("node_name_set", [])

        if len(node_name_set) == 0:
            host_name_list = body.get("hostNameList", [])
            diff = self._difference(host_name_list,
                                    DataSource.hostname_to_nodename)
            if len(diff) > 0:
                code = 4  # Invalid
                message = f"Invalid nodes: {diff}"
                logger.info("%s, code = %s", message, code)
                return dashboard_utils.rest_response(
                    success=False, code=code, message=message)

            node_name_set = [
                DataSource.hostname_to_nodename[host_name]
                for host_name in host_name_list
            ]

        if len(node_name_set) == 0:
            code = 0
            message = f"Succeed in unfreezing nodes {node_name_set}."
            logger.info("%s, code = %s", message, code)
            return dashboard_utils.rest_response(
                success=True, code=code, message=message)

        diff = self._difference(node_name_set, DataSource.nodename_to_hostname)
        if len(diff) > 0:
            code = 4  # Invalid
            message = f"Invalid nodes: {diff}"
            logger.info("%s, code = %s", message, code)
            return dashboard_utils.rest_response(
                success=False, code=code, message=message)

        code, message = await self._unfreeze_nodes_by_node_names(node_name_set)
        return dashboard_utils.rest_response(
            success=(code == 0), code=code, message=message)

    @routes.get("/scheduler/frozen_nodes")
    async def frozen_nodes(self, req) -> aiohttp.web.Response:
        logger.info("GET /scheduler/frozen_nodes")

        request_pb = gcs_service_pb2.GetAllFrozenNodesRequest()
        reply = await self._gcs_frozen_node_stub.GetAllFrozenNodes(request_pb)

        message = reply.status.message
        if reply.status.code == 0:
            message = "Succeed in getting all frozen nodes."
        logger.info("%s, code = %s", message, reply.status.code)

        data = dashboard_utils.message_to_dict(
            reply, including_default_value_fields=True)
        host_name_list = [
            DataSource.nodename_to_hostname.get(node_name, "")
            for node_name in data["frozenNodeSet"]
        ]

        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message,
            frozen_node_set=data["frozenNodeSet"],
            host_name_list=host_name_list)

    @routes.post("/scheduler/reject_node")
    async def reject_node(self, req) -> aiohttp.web.Response:
        body = dict(await req.json())
        request_pb = gcs_service_pb2.RejectNodeRequest()
        request_pb.node_id = bytes.fromhex(body["node_id"])
        if body["reject_type"] == "EXIT_NODE":
            request_pb.reject_type = gcs_service_pb2.NodeRejectType.EXIT_NODE
        elif body["reject_type"] == "EXIT_AND_REPLACE_NODE":
            request_pb.reject_type = \
                gcs_service_pb2.NodeRejectType.EXIT_AND_REPLACE_NODE
        else:
            return dashboard_utils.rest_response(
                success=False,
                message="Invalid reject type, should either be "
                "EXIT_NODE or EXIT_AND_REPLACE_NODE.")

        await self._gcs_heartbeat_info_stub.RejectNode(request_pb)
        return dashboard_utils.rest_response(
            success=True, message="Reject node success.")

    @routes.post("/scheduler/migrate_node_actors")
    async def migrate_node_actors(self, req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /scheduler/migrate_node_actors %s", request_json)

        body = dict(request_json)
        request_pb = gcs_service_pb2.MigrateActorsInNodeRequest()
        if "nodeNameList" in body:
            node_name_list = body.get("nodeNameList", [])
            logger.info("nodeNameList detected, using node name.")
            for node_name in node_name_list:
                request_pb.node_name_list.append(node_name)
        else:
            host_name_list = body.get("hostNameList", [])
            diff = self._difference(host_name_list,
                                    DataSource.hostname_to_nodename)
            if len(diff) > 0:
                code = 4  # Invalid
                message = f"Invalid nodes: {diff}"
                logger.info("%s, code = %s", message, code)
                return dashboard_utils.rest_response(
                    success=False, code=code, message=message)

            for host_name in host_name_list:
                node_name = DataSource.hostname_to_nodename[host_name]
                request_pb.node_name_list.append(node_name)

        res_pb = await self._gcs_actor_migration_stub.MigrateActorsInNode(
            request_pb)

        migration_result = {}
        for node_name in res_pb.results:
            migration_result[DataSource.nodename_to_hostname[node_name]] = {
                "success": res_pb.results[node_name].success,
                "message": res_pb.results[node_name].message,
            }

        message = res_pb.status.message
        if res_pb.status.code == 0:
            message = "Succeed in executing migrate_node_actors command."
        logger.info("%s, code = %s, result = %s", message, res_pb.status.code,
                    migration_result)

        return dashboard_utils.rest_response(
            success=(res_pb.status.code == 0),
            code=res_pb.status.code,
            message=message,
            migration_result=migration_result)

    @routes.post("/update_internal_config")
    async def update_internal_config(self, req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /update_internal_config %s", request_json)

        request_pb = gcs_service_pb2.UpdateInternalConfigRequest()
        request_pb.config_json_str = json.dumps(request_json)
        res_pb = await self._gcs_node_info_stub.UpdateInternalConfig(request_pb
                                                                     )

        return dashboard_utils.rest_response(
            success=(res_pb.status.code == 0),
            code=res_pb.status.code,
            message=res_pb.status.message)

    @routes.get("/internal_config")
    async def get_internal_config(self, req) -> aiohttp.web.Response:
        logger.info("GET /internal_config")

        request_pb = gcs_service_pb2.GetInternalConfigRequest()
        res_pb = await self._gcs_node_info_stub.GetInternalConfig(request_pb)
        logger.info(f"Got config: {res_pb.config}, type={type(res_pb.config)}")
        return dashboard_utils.rest_response(
            success=(res_pb.status.code == 0),
            code=res_pb.status.code,
            message=res_pb.status.message,
            config=res_pb.config)

    @routes.post("/scheduler/check_if_migration_is_complete")
    async def check_if_migration_is_complete(self,
                                             req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /scheduler/check_if_migration_is_complete %s",
                    request_json)

        request_data = dict(request_json)
        host_name_list = request_data.get("hostNameList", [])
        diff = self._difference(host_name_list,
                                DataSource.hostname_to_nodename)
        if len(diff) > 0:
            code = 4  # Invalid
            message = f"Invalid nodes: {diff}"
            logger.info("%s, code = %s", message, code)
            return dashboard_utils.rest_response(
                success=False, code=code, message=message)

        node_name_list = [
            DataSource.hostname_to_nodename[host_name]
            for host_name in host_name_list
        ]

        request_pb = gcs_service_pb2.CheckIfMigrationIsCompleteRequest(
            node_name_list=node_name_list)
        reply = await self._gcs_actor_migration_stub.\
            CheckIfMigrationIsComplete(request_pb)
        data = dashboard_utils.message_to_dict(
            reply, including_default_value_fields=True)

        completed = [
            DataSource.nodename_to_hostname[node_name]
            for node_name in data.get("completedNodeNameList", [])
        ]
        uncompleted = [
            DataSource.nodename_to_hostname[node_name]
            for node_name in data.get("uncompletedNodeNameList", [])
        ]

        message = reply.status.message
        if reply.status.code == 0:
            if len(uncompleted) != 0:
                message = "There are still {} node(s) that have not completed"\
                          " the migration, please check if resources are"\
                          " reserved for PlacementGroup on these nodes or"\
                          " if actors/tasks/drivers are still running on "\
                          "these nodes.".format(len(uncompleted))
            else:
                message = "All nodes complete the migration."
        logger.info("%s, code = %s", message, reply.status.code)

        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message,
            completed=completed,
            uncompleted=uncompleted)

    @routes.post("/scheduler/mark_failure_machines")
    async def remove_machine(self, req) -> aiohttp.web.Response:
        body = dict(await req.json())
        request_pb = gcs_service_pb2.MarkFailureMachinesRequest()
        request_pb.machine_list.extend(body["machineList"])

        await self._gcs_node_info_stub.MarkFailureMachines(request_pb)
        return dashboard_utils.rest_response(
            success=True, message="Mark failure machines success.")

    @routes.post("/pin_nodes_for_cluster_scaling_down")
    async def pin_nodes_for_cluster_scaling_down(self,
                                                 req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /pin_nodes_for_cluster_scaling_down %s",
                    request_json)

        request_data = dict(request_json)
        final_node_shape_and_count_list = []
        for data in request_data["finalNodeShapeAndCountList"]:
            node_shape_pb = gcs_pb2.NodeShape(
                shape_group=data.get("group", ""),
                resource_shape=data.get("nodeShape", {}))
            shape_and_count = gcs_service_pb2.NodeShapeAndCount(
                node_count=data["nodeCount"], node_shape=node_shape_pb)
            final_node_shape_and_count_list.append(shape_and_count)
        alive_node_list = request_data.get("aliveNodeList", [])

        request_pb = gcs_service_pb2.PinNodesForClusterScalingDownRequest(
            final_node_shape_and_count_list=final_node_shape_and_count_list,
            alive_node_list=alive_node_list)
        reply = await self._gcs_nodegroup_stub.PinNodesForClusterScalingDown(
            request_pb)
        data = dashboard_utils.message_to_dict(
            reply, including_default_value_fields=True)

        message = reply.status.message
        if reply.status.code == 0:
            message = "Succeed in pinning nodes for cluster scaling down."
        logger.info("%s, code = %s", message, reply.status.code)

        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message,
            pinned_node_list=data["pinnedNodeList"])

    @routes.post("/namespaces_v3/add_alternate_nodes_for_migration")
    async def add_alternate_nodes_for_migration(self,
                                                req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /namespaces_v3/add_alternate_nodes_for_migration %s",
                    request_json)

        request_data = dict(request_json)

        alternate_node_pb_list = []
        alternate_nodes = request_data.get("alternateNodes", [])
        for alternate_node in alternate_nodes:
            alternate_host_name = alternate_node.get("alternateHostName", "")
            to_be_migrated_host_name = alternate_node.get(
                "toBeMigratedHostName", "")
            alternate_node_pb_list.append(
                gcs_service_pb2.AlternateNode(
                    alternate_host_name=alternate_host_name,
                    to_be_migrated_host_name=to_be_migrated_host_name))

        request = gcs_service_pb2.AddAlternateNodesForMigrationRequest(
            alternate_node_list=alternate_node_pb_list)
        reply = await self._gcs_nodegroup_stub.AddAlternateNodesForMigration(
            request)

        message = reply.status.message
        if reply.status.code == 0:
            message = "Succeed in adding alternate nodes for migration"
        logger.info("%s, code = %s", message, reply.status.code)

        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message)

    async def _parse_params(self, request_info):
        """ This method is used to parse `hostNameList`, `mode` and
        `unfreeze_nodes_if_needed` field from request_info for method
        `remove_nodes_for_migration` and
        `scale_down_nodes_from_nodegroup_v3`
        """
        code = 0
        message = ""
        params = {}

        host_name_list = request_info.get("hostNameList", [])
        if len(host_name_list) == 0:
            code = 4  # Invalid.
            message = "Param 'hostNameList' must not be empty."
            return code, message, params
        params["host_name_list"] = host_name_list

        mode = request_info.get("mode", "soft").lower()
        if mode != "soft" and mode != "hard":
            code = 4  # Invalid.
            message = "Param 'mode' is invalid."
            return code, message, params
        params["mode"] = gcs_service_pb2.RemoveNodesFromNodegroupRequest.SOFT \
            if mode == "soft" \
            else gcs_service_pb2.RemoveNodesFromNodegroupRequest.HARD

        unfreeze_nodes_if_needed = str(
            request_info.get("unfreeze_nodes_if_needed", "")).lower()
        if unfreeze_nodes_if_needed != "true" \
                and unfreeze_nodes_if_needed != "false":
            code = 4  # Invalid.
            message = "Param 'unfreeze_nodes_if_needed' is invalid."
            return code, message, params
        params["unfreeze_nodes_if_needed"] = (
            unfreeze_nodes_if_needed == "true")

        return code, message, params

    async def _remove_nodes_from_nodegroup(self, nodegroup_id, mode,
                                           host_name_list):
        request = gcs_service_pb2.RemoveNodesFromNodegroupRequest(
            nodegroup_id=nodegroup_id,
            mode=mode,
            host_name_list=host_name_list)
        reply = await self._gcs_nodegroup_stub.RemoveNodesFromNodegroup(request
                                                                        )
        code = reply.status.code
        message = f"Succeed in removing nodes from namespace {nodegroup_id}"
        node_id_list = reply.node_id_list
        if code != 0:
            message = reply.status.message
            logger.info("%s, code = %s", message, code)
            return code, message, node_id_list

        logger.info("%s, code = %s", message, code)
        return code, message, node_id_list

    async def _unfreeze_nodes_by_node_names(self, node_names):
        logger.info("Unfreezing nodes: %s", node_names)
        request_pb = gcs_service_pb2.UnfreezeNodesRequest(
            node_name_set=node_names)
        reply = await self._gcs_frozen_node_stub.UnfreezeNodes(request_pb)

        code = reply.status.code
        message = reply.status.message
        if code == 0:
            message = f"Succeed in unfreezing nodes {node_names}"
        logger.info("%s, code = %s", message, code)
        return code, message

    async def _unfreeze_nodes_by_node_ids(self, node_id_list):
        node_names = []
        for node_id_binary in node_id_list:
            node_id_hex = ray.NodeID(node_id_binary).hex()
            node_info = DataSource.nodes.get(node_id_hex, None)
            if node_info is None:
                node_info = DataSource.dead_nodes.get(node_id_hex, None)
            if node_info is not None:
                node_name = node_info.get("nodeName", None)
                if node_name is not None:
                    node_names.append(node_name)

        return await self._unfreeze_nodes_by_node_names(node_names)

    @routes.post("/namespaces_v3/remove_nodes_for_migration")
    async def remove_nodes_for_migration(self, req) -> aiohttp.web.Response:
        request_json = await req.json()
        logger.info("POST /namespaces_v3/remove_nodes_for_migration %s",
                    request_json)

        code, message, params = await self._parse_params(dict(request_json))
        if code != 0:
            return dashboard_utils.rest_response(
                success=False, code=code, message=message)

        host_name_list = params["host_name_list"]
        mode = params["mode"]
        unfreeze_nodes_if_needed = params["unfreeze_nodes_if_needed"]

        def get_hosts_in_each_nodegroup(host_name_list, nodegroup_data_list):
            unknown_hosts = []
            unassigned_hosts = []
            nodegroup_to_hosts = {}
            for nodegroup_data in nodegroup_data_list:
                nodegroup_id = nodegroup_data.nodegroup_id
                classified_host_name_list = []
                for host in nodegroup_data.host_to_node_spec.keys():
                    if host in host_name_list:
                        classified_host_name_list.append(host)
                if len(classified_host_name_list) > 0:
                    if nodegroup_id == "NAMESPACE_LABEL_RAY_DEFAULT":
                        unassigned_hosts = classified_host_name_list
                        unknown_hosts = self._difference(
                            host_name_list, classified_host_name_list)
                    else:
                        nodegroup_to_hosts[
                            nodegroup_id] = classified_host_name_list

            for hosts in nodegroup_to_hosts.values():
                for host in hosts:
                    if host in unassigned_hosts:
                        unassigned_hosts.remove(host)

            return nodegroup_to_hosts, unassigned_hosts, unknown_hosts

        request = gcs_service_pb2.GetAllNodegroupsRequest()
        reply = await self._gcs_nodegroup_stub.GetAllNodegroups(request)
        if reply.status.code != 0:
            message = reply.status.message
            logger.info("%s, code = %s", message, reply.status.code)
            return dashboard_utils.rest_response(
                success=(reply.status.code == 0),
                code=reply.status.code,
                message=message)

        nodegroup_to_hosts, unassigned_hosts, unknown_hosts = \
            get_hosts_in_each_nodegroup(
                host_name_list, reply.nodegroup_data_list)

        if len(unknown_hosts) > 0:
            return dashboard_utils.rest_response(
                success=False,
                code=4,  # Invalid
                message=f"Nodes {unknown_hosts} are not registered yet.")

        if len(nodegroup_to_hosts) == 0 and not unfreeze_nodes_if_needed:
            return dashboard_utils.rest_response(
                success=True,
                code=0,
                message="Succeed in removing all nodes for migration.")

        result_dict = {}
        success_count = 0
        for nodegroup_id, hosts in nodegroup_to_hosts.items():
            code, message, nodes = await self._remove_nodes_from_nodegroup(
                nodegroup_id, mode, hosts)
            if code == 0:
                if unfreeze_nodes_if_needed:
                    code, message = await self._unfreeze_nodes_by_node_ids(
                        nodes)
                if code == 0:
                    message = f"Succeed in removing nodes from {nodegroup_id}"
                    success_count += 1
            result_dict[nodegroup_id] = {
                "code": code,
                "message": message,
                "hostNameList": hosts
            }

        if unfreeze_nodes_if_needed and len(unassigned_hosts) > 0:
            node_names = [
                DataSource.hostname_to_nodename[host_name]
                for host_name in host_name_list
            ]
            code, message = await self._unfreeze_nodes_by_node_names(node_names
                                                                     )
            if code == 0:
                message = f"Succeed in unfreezing nodes of {nodegroup_id}"
                success_count += 1
            result_dict["unassigned"] = {
                "code": code,
                "message": message,
                "hostNameList": unassigned_hosts
            }

        if success_count == len(result_dict):
            return dashboard_utils.rest_response(
                success=True,
                code=0,
                message="Succeed in removing all nodes for migration.")

        # Not all success.
        message = "Failed to removing all nodes for migration. "
        for nodegroup_id, result in result_dict.items():
            code = result["code"]
            msg = result["message"]
            hosts = result["hostNameList"]
            if code != 0:
                message += f"{nodegroup_id}: {hosts}, reason: {msg};"

        return dashboard_utils.rest_response(
            success=False,
            code=4,  # Not all success
            message=message,
            data=result_dict)

    def _build_schedule_options_proto(self, schedule_options):
        resource_weights = schedule_options.get("resourceWeights", {})
        schedule_options_proto = gcs_service_pb2.ScheduleOptions(
            pack_step=float(
                schedule_options.get(
                    "packStep", os.getenv("RAY_GCS_SCHEDULE_PACK_STEP",
                                          "0.0"))),
            rare_resources_schedule_pack_step=float(
                schedule_options.get(
                    "rareResourcesSchedulePackStep",
                    os.getenv("RAY_rare_resources_schedule_pack_step",
                              "1.0"))),
            num_candidate_nodes_for_scheduling=int(
                schedule_options.get(
                    "numCandidateNodesForScheduling",
                    os.getenv("NUM_CANDIDATE_NODES_FOR_SCHEDULING", "3"))),
            resource_weights=dict(resource_weights),
            runtime_resource_scheduling_enabled=str(
                schedule_options.get(
                    "runtimeResourceSchedulingEnabled",
                    os.getenv("RAY_runtime_resource_scheduling_enabled",
                              "true"))).lower() == "true",
            runtime_resources_calculation_interval_s=int(
                schedule_options.get(
                    "runtimeResourcesCalculationIntervalS",
                    os.getenv("RAY_runtime_resources_calculation_interval_s",
                              "600"))),
            runtime_memory_tail_percentile=float(
                schedule_options.get(
                    "runtimeMemoryTailPercentile",
                    os.getenv("RAY_runtime_memory_tail_percentile", "1.0"))),
            runtime_cpu_tail_percentile=float(
                schedule_options.get(
                    "runtimeCPUTailPercentile",
                    os.getenv("RAY_runtime_cpu_tail_percentile", "0.95"))),
            runtime_resources_history_window_len_s=int(
                schedule_options.get(
                    "runtimeResourcesHistoryWindowLenS",
                    os.getenv("RAY_runtime_resources_history_window_len_s",
                              "86400"))),
            runtime_memory_history_window_tail=float(
                schedule_options.get(
                    "runtimeMemoryHistoryWindowTail",
                    os.getenv("RAY_runtime_memory_history_window_tail",
                              "0.99"))),
            runtime_cpu_history_window_tail=float(
                schedule_options.get(
                    "runtimeCPUHistoryWindowTail",
                    os.getenv("RAY_runtime_cpu_history_window_tail", "0.95"))),
            overcommit_ratio=float(
                schedule_options.get("overcommitRatio",
                                     os.getenv("RAY_overcommit_ratio",
                                               "1.5"))),
            node_overcommit_ratio=float(
                schedule_options.get(
                    "nodeOvercommitRatio",
                    os.getenv("RAY_node_overcommit_ratio", "1.5"))),
            job_resource_requirements_max_min_ratio_limit=float(
                schedule_options.get(
                    "jobResourceRequirementsMaxMinRatioLimit",
                    os.getenv(
                        "RAY_job_resource_requirements_max_min_ratio_limit",
                        "10.0"))))
        return schedule_options_proto

    @routes.post("/namespaces_v3")
    async def create_or_update_nodegroup_v3(self, req) -> aiohttp.web.Response:
        nodegroup_info_json = await req.json()
        logger.info("POST /namespaces_v3 %s", nodegroup_info_json)

        nodegroup_info = dict(nodegroup_info_json)
        nodegroup_id = nodegroup_info["namespaceId"]
        nodegroup_name = nodegroup_info.get("name", "")
        user_data = json.dumps(nodegroup_info_json)
        revision = nodegroup_info.get("revision", 0)
        schedule_options_proto = self._build_schedule_options_proto(
            nodegroup_info.get("scheduleOptions", {}))

        shape_and_count_list = []
        for data in nodegroup_info["nodeShapeAndCountList"]:
            node_shape_pb = gcs_pb2.NodeShape(
                shape_group=data.get("group", ""),
                resource_shape=data.get("nodeShape", {}))
            shape_and_count = gcs_service_pb2.NodeShapeAndCount(
                node_count=data["nodeCount"], node_shape=node_shape_pb)
            shape_and_count_list.append(shape_and_count)

        isolation = str(
            nodegroup_info.get("enableSubNamespaceIsolation",
                               False)).lower() == "true"
        enable_job_quota = str(
            nodegroup_info.get("enableJobQuota",
                               os.getenv("RAY_enable_job_quota",
                                         "true"))).lower() == "true"
        request = gcs_service_pb2.CreateOrUpdateNodegroupRequest(
            nodegroup_id=nodegroup_id,
            nodegroup_name=nodegroup_name,
            enable_sub_nodegroup_isolation=isolation,
            node_shape_and_count_list=shape_and_count_list,
            schedule_options=schedule_options_proto,
            enable_revision=True,
            revision=revision,
            user_data=user_data,
            enable_job_quota=enable_job_quota)
        reply = await self._gcs_nodegroup_stub.CreateOrUpdateNodegroup(request)

        message = reply.status.message
        if reply.status.code == 0:
            message = f"Succeed in creating or updating namespace "\
                f"{nodegroup_id}"
        logger.info("%s, code = %s", message, reply.status.code)

        data = dashboard_utils.message_to_dict(
            reply, including_default_value_fields=True)
        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message,
            namespace_id=nodegroup_id,
            nodegroup_name=nodegroup_name,
            revision=reply.revision,
            host_name_list=data["hostToNodeSpec"])

    @routes.post("/namespaces_v3/scale_down/{namespace_id}")
    async def scale_down_nodes_from_nodegroup_v3(self,
                                                 req) -> aiohttp.web.Response:
        nodegroup_id = req.match_info.get("namespace_id")
        request_json = await req.json()
        logger.info("POST /namespaces_v3/scale_down/%s %s", nodegroup_id,
                    request_json)

        code, message, params = await self._parse_params(dict(request_json))
        if code != 0:
            return dashboard_utils.rest_response(
                success=False, code=code, message=message)

        host_name_list = params["host_name_list"]
        mode = params["mode"]
        unfreeze_nodes_if_needed = params["unfreeze_nodes_if_needed"]

        code, message, nodes = await self._remove_nodes_from_nodegroup(
            nodegroup_id, mode, host_name_list)
        if code == 0 and unfreeze_nodes_if_needed:
            code, message = await self._unfreeze_nodes_by_node_ids(nodes)

        return dashboard_utils.rest_response(
            success=(code == 0), code=code, message=message)

    @routes.delete("/namespaces_v3/{namespace_id}")
    async def remove_nodegroup(self, req) -> aiohttp.web.Response:
        """ If the namespace_id contains a special string (e.g. /),
        the DELETE may be failed. This is to compatible with antc,
        antc can't create a DELETE request with body.
        """
        nodegroup_id = req.match_info.get("namespace_id")

        request = gcs_service_pb2.RemoveNodegroupRequest(
            nodegroup_id=nodegroup_id)
        reply = await self._gcs_nodegroup_stub.RemoveNodegroup(request)
        message = reply.status.message
        if reply.status.code == 0:
            message = f"Succeed in deleting namespace {nodegroup_id}"
        logger.info("%s, code = %s", message, reply.status.code)

        return dashboard_utils.rest_response(
            success=(reply.status.code == 0),
            code=reply.status.code,
            message=message)

    @routes.get("/namespaces_v3")
    @dashboard_utils.aiohttp_cache(30)
    async def get_all_nodegroups(self, req) -> aiohttp.web.Response:
        include_sub_nodegroups = (req.query.get("include_sub_namespaces",
                                                "0") == "1")
        show_node_spec = (req.query.get("show_node_spec", "0") == "1")
        show_user_data = (req.query.get("show_user_data", "0") == "1")
        request_nodegroup_id = req.query.get("namespace_id", None)
        request = gcs_service_pb2.GetAllNodegroupsRequest(
            include_sub_nodegroups=include_sub_nodegroups)
        reply = await self._gcs_nodegroup_stub.GetAllNodegroups(request)

        def string_weights_to_dict(string_weights):
            weight_dict = {}
            if len(string_weights) == 0:
                return weight_dict

            weight_list = string_weights.split("_")
            for weight in weight_list:
                pair = weight.split(":")
                weight_dict[pair[0]] = int(pair[1])
            return weight_dict

        all_nodegroups = []
        for nodegroup_data in reply.nodegroup_data_list:
            if request_nodegroup_id is not None and\
                 request_nodegroup_id != nodegroup_data.nodegroup_id:
                continue

            schedule_options = dict(nodegroup_data.schedule_options)
            schedule_options["resource_weights"] = string_weights_to_dict(
                schedule_options.get("resource_weights", ""))
            schedule_options["scorer_weights"] = string_weights_to_dict(
                schedule_options.get("scorer_weights", ""))

            nodegroup_info = dict(
                revision=nodegroup_data.revision,
                namespace_id=nodegroup_data.nodegroup_id,
                name=nodegroup_data.nodegroup_name,
                enable_sub_namespace_isolation=nodegroup_data.
                enable_sub_nodegroup_isolation,
                enable_revision=nodegroup_data.enable_revision,
                schedule_options=schedule_options)

            group_to_count = {}
            host_name_dict = {}
            for key, val in nodegroup_data.host_to_node_spec.items():
                if val.node_shape.shape_group not in group_to_count:
                    group_to_count[val.node_shape.shape_group] = 1
                else:
                    group_to_count[val.node_shape.shape_group] += 1
                node_spec = json.loads(json_format.MessageToJson(val))
                host_name_dict[key] = node_spec

            if show_node_spec:
                nodegroup_info["host_name_list"] = host_name_dict
            else:
                nodegroup_info["host_name_list"] = list(host_name_dict.keys())

            if show_user_data and len(nodegroup_data.user_data) > 0:
                user_data = dict(json.loads(nodegroup_data.user_data))
                node_shape_and_count_list = user_data.get(
                    "nodeShapeAndCountList", [])
                wildcard_index = -1
                remainning_count = len(host_name_dict)
                for i in range(len(node_shape_and_count_list)):
                    group = node_shape_and_count_list[i].get("group", "")
                    if len(group) == 0:
                        wildcard_index = i
                        continue
                    node_count = group_to_count.get(group, 0)
                    node_shape_and_count_list[i]["nodeCount"] = node_count
                    remainning_count -= node_count
                if wildcard_index >= 0:
                    if remainning_count > 0:
                        node_shape_and_count_list[wildcard_index][
                            "nodeCount"] = remainning_count
                    else:
                        del node_shape_and_count_list[wildcard_index]

                user_data["nodeShapeAndCountList"] = node_shape_and_count_list
                user_data["rayPodNumber"] = len(host_name_dict)
                nodegroup_info["user_data"] = user_data

            all_nodegroups.append(nodegroup_info)

        code = reply.status.code
        message = reply.status.message
        if code == 0:
            message = "Succeed in getting namespaces"

        if len(all_nodegroups) == 0:
            code = 102  # non-existent
            message = f"Failed to get namespace "\
                f"{request_nodegroup_id} as it does not exist."

        logger.info("%s, code = %s", message, reply.status.code)

        return dashboard_utils.rest_response(
            success=(code == 0),
            code=code,
            message=message,
            namespaces=all_nodegroups)

    @routes.get("/cluster_info")
    @dashboard_utils.aiohttp_cache(3, cache_exception=False)
    async def get_cluster_info(self, req) -> aiohttp.web.Response:
        value_bytes = await self._dashboard_head.aioredis_client.get(
            dashboard_consts.REDIS_KEY_GCS_SERVER_ADDRESS)
        gcs_server_address = str(
            value_bytes, encoding="utf-8") if value_bytes is not None else None
        return dashboard_utils.rest_response(
            success=True,
            message="Ray cluster info fetched.",
            gcs_server_address=gcs_server_address)

    async def run(self, server):
        self._gcs_actor_info_stub = gcs_service_pb2_grpc \
            .ActorInfoGcsServiceStub(self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_frozen_node_stub = gcs_service_pb2_grpc.\
            FrozenNodeGcsServiceStub(self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_actor_migration_stub = gcs_service_pb2_grpc.\
            ActorMigrationGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_node_resource_info_stub = gcs_service_pb2_grpc.\
            NodeResourceInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_heartbeat_info_stub = gcs_service_pb2_grpc.\
            HeartbeatInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_nodegroup_stub = gcs_service_pb2_grpc.\
            NodegroupInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
