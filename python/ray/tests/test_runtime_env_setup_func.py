import logging
import os
import platform
import sys
import tempfile
import threading

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import format_web_url
from ray.dashboard import consts as dashboard_consts
from ray.job_submission import JobStatus, JobSubmissionClient
from ray._common.network_utils import find_free_port


def _hook():
    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)


@pytest.mark.parametrize("is_module", [False, True])
def test_setup_func_basic(shutdown_only, is_module):
    def configure_logging(level: int):
        logger = logging.getLogger("")
        logger.setLevel(level)

    if is_module:
        runtime_env = {
            "worker_process_setup_hook": "ray.tests.test_runtime_env_setup_func._hook",  # noqa
            "env_vars": {"ABC": "123"},
        }
    else:
        runtime_env = {
            "worker_process_setup_hook": lambda: configure_logging(logging.DEBUG),
            "env_vars": {"ABC": "123"},
        }

    ray.init(num_cpus=1, runtime_env=runtime_env)

    @ray.remote
    def f(level):
        logger = logging.getLogger("")
        assert logging.getLevelName(logger.getEffectiveLevel()) == level
        return True

    @ray.remote
    class Actor:
        def __init__(self, level):
            logger = logging.getLogger("")
            assert logging.getLevelName(logger.getEffectiveLevel()) == level

        def ready(self):
            return True

        def get_env_var(self, key):
            return os.getenv(key)

    # Test basic.
    for _ in range(10):
        assert ray.get(f.remote("DEBUG"))
    a = Actor.remote("DEBUG")
    assert ray.get(a.__ray_ready__.remote())

    # Make sure env var is not overwritten.
    assert ray.get(a.get_env_var.remote("ABC")) == "123"

    # Test override.
    # TODO(sang)
    # ray.get(
    #     f.options(
    #         runtime_env={
    #             "worker_process_setup_hook": lambda: configure_logging(logging.INFO)}
    #     ).remote("INFO"))
    # a = Actor.optinos(
    #     runtime_env={
    #       "worker_process_setup_hook": lambda: configure_logging(logging.INFO)
    #     }
    # ).remote("INFO")
    # assert ray.get(a.__ray_ready__.remote())


def test_setup_func_failure(shutdown_only):
    """
    Verify when deserilization failed, it raises an exception.
    """

    class CustomClass:
        """
        Custom class that can serialize but canont deserialize.
        It is used to test deserialization failure.
        """

        def __getstate__(self):
            # This method is called during serialization
            return self.__dict__

        def __setstate__(self, state):
            # This method is called during deserialization
            raise RuntimeError("Deserialization not allowed")

    c = CustomClass()

    def setup():
        print(c)

    ray.init(
        num_cpus=1,
        runtime_env={
            "worker_process_setup_hook": setup,
        },
    )

    @ray.remote
    class A:
        pass

    a = A.remote()
    # TODO(sang): Maybe we should raise RuntimeEnvSetupError?
    # It is pretty difficult now. See
    # https://github.com/ray-project/ray/pull/34738#discussion_r1189553716
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.__ray_ready__.remote())
    assert "Deserialization not allowed" in str(e.value)

    """
    Verify when the serialization fails, ray.init fails.
    """
    ray.shutdown()
    lock = threading.Lock()

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as e:
        ray.init(
            num_cpus=0,
            runtime_env={
                "worker_process_setup_hook": lambda: print(lock),
            },
        )
    assert "Failed to export the setup function." in str(e.value)

    """
    Verify when the setup hook failed, it raises an exception.
    """
    ray.shutdown()

    def setup_func():
        raise ValueError("Setup Failed")

    ray.init(
        num_cpus=1,
        runtime_env={
            "worker_process_setup_hook": setup_func,
        },
    )

    @ray.remote
    class A:
        pass

    a = A.remote()
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.__ray_ready__.remote())
    assert "Setup Failed" in str(e.value)
    assert "Failed to execute the setup hook method." in str(e.value)


def test_setup_hook_module_failure(shutdown_only):
    # Use a module that cannot be found.
    ray.init(
        runtime_env={
            "worker_process_setup_hook": (
                "ray.tests.test_runtime_env_setup_func._hooks"
            )
        },
    )

    @ray.remote
    class A:
        pass

    a = A.remote()
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.__ray_ready__.remote())
    assert "Failed to execute the setup hook method" in str(e.value)


def _wait_for_dashboard_agent_ready(timeout=30):
    """Wait for the dashboard agent to register its info in internal KV.

    This is the proper way to wait for agent readiness, as the agent
    must register its info in KV before JobSubmissionClient can work.
    """
    for node in ray.nodes():
        node_id = node["NodeID"]
        key = f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}"

        def get_addr():
            return ray.experimental.internal_kv._internal_kv_get(
                key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
            )

        wait_for_condition(lambda: get_addr() is not None, timeout=timeout)


@pytest.mark.skipif(platform.system() == "Windows", reason="Doesn't support Windows.")
def test_job_submission_not_allowed_for_callable(ray_start_cluster):
    """Test that validates job submission with worker process setup hooks."""
    cluster = ray_start_cluster
    # Add head node with explicit dashboard agent port to avoid port conflicts
    cluster.add_node(num_cpus=1, dashboard_agent_listen_port=find_free_port())
    ray.init(address=cluster.address)

    # Wait for the dashboard agent to be fully ready (registered in KV)
    _wait_for_dashboard_agent_ready(timeout=60)

    temp_dir = None
    file_path = None

    try:
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()

        # Define pytest tests as a string
        pytest_content = """
import ray
import sys

@ray.remote
def f():
    import logging
    logger = logging.getLogger("")
    logger.debug("this is debug")
    assert logger.getEffectiveLevel() == logging.DEBUG

ray.get(f.remote())
        """

        # Create a temporary Python file with pytest content
        file_path = os.path.join(temp_dir, "test_temp.py")

        with open(file_path, "w") as file:
            file.write(pytest_content)

        # Get the absolute path
        absolute_path = os.path.abspath(file_path)

        # Get the dashboard URL
        address = ray._private.worker._global_node.webui_url
        address = format_web_url(address)
        client = JobSubmissionClient(address)

        with pytest.raises(
            ValueError,
            match="Invalid type <class 'function'> for `worker_process_setup_hook`.",
        ):
            job = client.submit_job(
                entrypoint=f"python {absolute_path}",
                runtime_env={"worker_process_setup_hook": lambda: print("abc")},
            )

        job = client.submit_job(
            entrypoint=f"python {absolute_path}",
            runtime_env={
                "worker_process_setup_hook": (
                    "ray.tests.test_runtime_env_setup_func._hook"
                )
            },
        )

        def verify():
            status = client.get_job_status(job)
            if status == JobStatus.FAILED:
                print("job status is ", status)
                print("job logs are", client.get_job_logs(job))

            return client.get_job_status(job) == JobStatus.SUCCEEDED

        wait_for_condition(verify)

    finally:
        if file_path:
            os.remove(file_path)
        if temp_dir:
            os.rmdir(temp_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
