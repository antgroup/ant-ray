import logging
import sys
import tempfile
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import pytest
import pytest_asyncio

import ray
from ray._private.gcs_utils import GcsChannel
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import AllocationMode
from ray.core.generated.gcs_service_pb2 import CreateOrUpdateVirtualClusterRequest
from ray.dashboard.modules.job.common import JobSubmitRequest, validate_request_type
from ray.dashboard.modules.job.job_head import JobAgentSubmissionClient
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.tests.conftest import get_default_fixture_ray_kwargs

TEMPLATE_ID_PREFIX = "template_id_"
NTEMPLATE = 5

logger = logging.getLogger(__name__)


def _check_job(
    client: JobSubmissionClient, job_id: str, status: JobStatus, timeout: int = 10
) -> bool:
    res_status = client.get_job_status(job_id)
    assert res_status == status
    return True


@contextmanager
def _ray_start_virtual_cluster(**kwargs):
    cluster_not_supported_ = kwargs.pop("skip_cluster", cluster_not_supported)
    if cluster_not_supported_:
        pytest.skip("Cluster not supported")
    init_kwargs = get_default_fixture_ray_kwargs()
    num_nodes = 0
    do_init = False
    # num_nodes & do_init are not arguments for ray.init, so delete them.
    if "num_nodes" in kwargs:
        num_nodes = kwargs["num_nodes"]
        del kwargs["num_nodes"]
    if "do_init" in kwargs:
        do_init = kwargs["do_init"]
        del kwargs["do_init"]
    elif num_nodes > 0:
        do_init = True
    init_kwargs.update(kwargs)
    namespace = init_kwargs.pop("namespace")
    cluster = Cluster()
    remote_nodes = []
    for i in range(num_nodes):
        if i > 0 and "_system_config" in init_kwargs:
            del init_kwargs["_system_config"]
        remote_nodes.append(
            cluster.add_node(
                **init_kwargs,
                env_vars={
                    "RAY_NODE_TYPE_NAME": TEMPLATE_ID_PREFIX + str(i % NTEMPLATE)
                },
            )
        )
        # We assume driver will connect to the head (first node),
        # so ray init will be invoked if do_init is true
        if len(remote_nodes) == 1 and do_init:
            ray.init(address=cluster.address, namespace=namespace)
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest_asyncio.fixture
def is_virtual_cluster_empty(request):
    param = getattr(request, "param", True)
    yield param


@pytest_asyncio.fixture
async def job_sdk_client(request, make_sure_dashboard_http_port_unused):
    param = getattr(request, "param", {})
    with _ray_start_virtual_cluster(do_init=True, num_nodes=10, **param) as res:
        ip, _ = res.webui_url.split(":")
        agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
        assert wait_until_server_available(agent_address)
        head_address = res.webui_url
        assert wait_until_server_available(head_address)
        yield (
            JobAgentSubmissionClient(format_web_url(agent_address)),
            JobSubmissionClient(format_web_url(head_address)),
            res.gcs_address,
        )


async def create_virtual_cluster(
    gcs_address, virtual_cluster_id, replica_sets, allocation_mode=AllocationMode.Mixed
):
    channel = GcsChannel(gcs_address, aio=True)
    channel.connect()
    gcs_virtual_cluster_info_stub = (
        gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(channel.channel())
    )
    request = CreateOrUpdateVirtualClusterRequest(
        virtual_cluster_id=virtual_cluster_id,
        virtual_cluster_name=virtual_cluster_id,
        mode=allocation_mode,
        replica_sets=replica_sets,
    )
    reply = await (gcs_virtual_cluster_info_stub.CreateOrUpdateVirtualCluster(request))
    assert reply.status.code == 0
    return reply.node_instances


@pytest.mark.parametrize(
    "job_sdk_client",
    [{"_system_config": {"gcs_actor_scheduling_enabled": True}}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_mixed_virtual_cluster(job_sdk_client):
    agent_client, head_client, gcs_address = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    for i in range(NTEMPLATE):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 2}
        )
        assert len(nodes) != 0
        for node_id in nodes:
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    for i in range(NTEMPLATE):
        actor_name = f"test_actors_{i}"
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            driver_script = f"""
import ray
ray.init(address='auto')

@ray.remote
class Actor:
    def __init__(self):
        pass

    def run(self):
        pass

a = Actor.options(name="{actor_name}", num_cpus=1).remote()
ray.get(a.run.remote())
            """
            test_script_file = path / "test_script.py"
            with open(test_script_file, "w+") as file:
                file.write(driver_script)

            runtime_env = {"working_dir": tmp_dir}
            runtime_env = upload_working_dir_if_needed(
                runtime_env, tmp_dir, logger=logger
            )
            runtime_env = RuntimeEnv(**runtime_env).to_dict()

            request = validate_request_type(
                {
                    "runtime_env": runtime_env,
                    "entrypoint": "python test_script.py",
                    "virtual_cluster_id": virtual_cluster_id,
                },
                JobSubmitRequest,
            )
            submit_result = await agent_client.submit_job_internal(request)
            job_id = submit_result.submission_id

            wait_for_condition(
                partial(
                    _check_job,
                    client=head_client,
                    job_id=job_id,
                    status=JobStatus.SUCCEEDED,
                ),
                timeout=100,
            )
            actors = ray.state.actors()
            for _, actor_info in actors.items():
                if actor_info["Name"] == actor_name:
                    node_id = actor_info["Address"]["NodeID"]
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id


@pytest.mark.parametrize(
    "job_sdk_client,is_virtual_cluster_empty",
    [
        ({"_system_config": {"gcs_actor_scheduling_enabled": True}}, True),
        ({"_system_config": {"gcs_actor_scheduling_enabled": True}}, False),
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_primary_virtual_cluster(
    request, job_sdk_client, is_virtual_cluster_empty
):
    agent_client, head_client, gcs_address = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    non_primary_nodes = set()
    for i in range(NTEMPLATE - 1):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 2}
        )
        assert len(nodes) != 0
        for node_id in nodes:
            non_primary_nodes.add(node_id)

    actor_name = "test_actor_primary"
    virtual_cluster_id = "kPrimaryClusterID"
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        driver_script = f"""
import ray
ray.init(address='auto')

@ray.remote
class Actor:
    def __init__(self):
        pass

    def run(self):
        pass

a = Actor.options(name="{actor_name}", num_cpus=1).remote()
ray.get(a.run.remote())
            """
        test_script_file = path / "test_script.py"
        with open(test_script_file, "w+") as file:
            file.write(driver_script)

        runtime_env = {"working_dir": tmp_dir}
        runtime_env = upload_working_dir_if_needed(runtime_env, tmp_dir, logger=logger)
        runtime_env = RuntimeEnv(**runtime_env).to_dict()
        if is_virtual_cluster_empty:
            virtual_cluster_id = ""

        request = validate_request_type(
            {
                "runtime_env": runtime_env,
                "entrypoint": "python test_script.py",
                "virtual_cluster_id": virtual_cluster_id,
            },
            JobSubmitRequest,
        )
        submit_result = await agent_client.submit_job_internal(request)
        job_id = submit_result.submission_id

        wait_for_condition(
            partial(
                _check_job,
                client=head_client,
                job_id=job_id,
                status=JobStatus.SUCCEEDED,
            ),
            timeout=100,
        )
        actors = ray.state.actors()
        for _, actor_info in actors.items():
            if actor_info["Name"] == actor_name:
                node_id = actor_info["Address"]["NodeID"]
                assert node_id not in non_primary_nodes


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
