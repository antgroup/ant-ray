import sys
import base64

import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import run_in_container, NESTED_IMAGE_NAME
from ray._private.test_utils import (
    wait_for_condition,
    check_logs_by_keyword,
)


# NOTE(zcin): The actual test code are in python scripts under
# python/ray/tests/runtime_env_container. The scripts are copied over to
# the docker container that's started by the `podman_docker_cluster`
# fixture, so that the tests can be run by invoking the test scripts
# using `python test.py` from within the pytests in this file


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_put_get(podman_docker_cluster, use_image_uri_api):
    """Test ray.put and ray.get."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_put_get.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_shared_memory(podman_docker_cluster, use_image_uri_api):
    """Test shared memory."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_shared_memory.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_log_file_exists(podman_docker_cluster, use_image_uri_api):
    """Verify worker log file exists"""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_log_file_exists.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_ray_env_vars(podman_docker_cluster, use_image_uri_api):
    """Test that env vars with prefix 'RAY_' are propagated to container."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_ray_env_vars.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_container_with_env_vars(podman_docker_cluster):
    """Test blah blah."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_with_env_vars.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_worker_exit_intended_system_exit_and_user_error(
    podman_docker_cluster, use_image_uri_api
):
    """
    INTENDED_SYSTEM_EXIT
    - (not tested, hard to test) Unused resource removed
    - (tested) Pg removed
    - (tested) Idle
    USER_ERROR
    - (tested) Actor init failed
    """

    container_id = podman_docker_cluster
    cmd = [
        "python",
        "tests/test_worker_exit_intended_system_exit_and_user_error.py",
        "--image",
        NESTED_IMAGE_NAME,
    ]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_serve_basic(podman_docker_cluster, use_image_uri_api):
    """Test Serve deployment."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_serve_basic.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_job(podman_docker_cluster):

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_job.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_serve_telemetry(podman_docker_cluster, use_image_uri_api):
    """Test Serve deployment telemetry."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_serve_telemetry.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


EXPECTED_ERROR = "The '{0}' field currently cannot be used together with"


@pytest.mark.parametrize("api_version", ["container", "image_uri"])
class TestContainerRuntimeEnvWithOtherRuntimeEnv:
    def test_container_with_config(self, api_version):
        """`config` should be allowed with `container`"""

        runtime_env = {"config": {"setup_timeout_seconds": 10}}

        if api_version == "container":
            runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
        else:
            runtime_env["image_uri"] = NESTED_IMAGE_NAME

        @ray.remote(runtime_env=runtime_env)
        def f():
            return ray.put((1, 10))

    def test_container_with_env_vars(self, api_version):
        """`env_vars` should be allowed with `container`"""

        runtime_env = {"env_vars": {"HELLO": "WORLD"}}

        if api_version == "container":
            runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
        else:
            runtime_env["image_uri"] = NESTED_IMAGE_NAME

        @ray.remote(runtime_env=runtime_env)
        def f():
            return ray.put((1, 10))

    def test_container_with_pip(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"pip": ["requests"]}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_conda(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"conda": ["requests"]}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_py_modules(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"py_modules": ["requests"]}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_working_dir(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"working_dir": "."}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_pip_and_working_dir(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"pip": ["requests"], "working_dir": "."}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))


@ray.remote
class Counter(object):
    def __init__(self):
        self.value = 0
        ray.put(self.value)

    def increment(self):
        self.value += 1
        return self.value


@pytest.mark.parametrize("api_version", ["container", "image_uri"])
class TestContainerRuntimeEnvCommandLine:
    def test_container_mount_path_deduplication(self, api_version, ray_start_regular):
        runtime_env = {
            api_version: {
                "image": "unknown_image",
                "run_options": [
                    "-v",
                    "/home/admin/tmp/.pyenv:/home/admin/.pyenv",
                    "-v",
                    "/tmp/fake_dir/:/tmp/fake_dir/",
                    "fake_command",
                ],
            },
        }

        a = Counter.options(
            runtime_env=runtime_env,
        ).remote()
        try:
            ray.get(a.increment.remote(), timeout=1)
        except (ray.exceptions.RuntimeEnvSetupError, ray.exceptions.GetTimeoutError):
            # ignore the exception because container mode don't work in common
            # test environments.
            pass
        keyword1 = "\-v /home/admin/tmp/.pyenv:/home/admin/.pyenv"
        keyword2 = "\-v /home/admin/.pyenv:/home/admin/.pyenv"
        keyword3 = "fake_command"
        keyword4 = "\-v /tmp/fake_dir/:/tmp/fake_dir/"
        log_file_pattern = "raylet.err"
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword1, log_file_pattern), timeout=20
        )
        # The default mount path used to exist in container
        # '/home/admin/.pyenv:/home/admin/.pyenv'
        # Check that the default mount path is replaced with the user-specified source_path
        wait_for_condition(
            lambda: not check_logs_by_keyword(keyword2, log_file_pattern), timeout=20
        )
        # Check other command wihout mount path also in the container command
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword3, log_file_pattern), timeout=10
        )
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword4, log_file_pattern), timeout=10
        )

    def test_check_install_ray_with_pip_packages(self, api_version, ray_start_regular):
        runtime_env = {
            api_version: {
                "image": "unknown_image",
                "_install_ray": True,
            },
            "pip": {
                "packages": [
                    "protobuf<=3.20.3",
                    "jmespath<1.0.0,>=0.9.3",
                ],
            },
        }

        a = Counter.options(
            runtime_env=runtime_env,
        ).remote()
        try:
            ray.get(a.increment.remote(), timeout=1)
        except (ray.exceptions.RuntimeEnvSetupError, ray.exceptions.GetTimeoutError):
            # ignore the exception because container mode don't work in common
            # test environments.
            time.sleep(2)
        # Checkout the worker logs to ensure if the cgroup params is set correctly
        # in the podman command.
        base64string = base64.b64encode(
            json.dumps(runtime_env["pip"]["packages"]).encode("utf-8")
        ).decode("utf-8")
        keyword = f"\--packages {base64string}"
        log_file_pattern = "raylet.err"
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword, log_file_pattern), timeout=20
        )

    def test_container_command_with_py_executable(self, api_version, ray_start_regular):
        runtime_env = {
            api_version: {
                "image": "unknown_image",
                "py_executable": "/fake/python/bin",
            },
        }

        a = Counter.options(
            runtime_env=runtime_env,
        ).remote()
        try:
            ray.get(a.increment.remote(), timeout=1)
        except (ray.exceptions.RuntimeEnvSetupError, ray.exceptions.GetTimeoutError):
            # ignore the exception because container mode don't work in common
            # test environments.
            pass
        # Checkout the worker logs to ensure if the cgroup params is set correctly
        # in the podman command.
        keyword = "/fake/python/bin -m ray._private.workers.default_worker"
        log_file_pattern = "raylet.err"
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword, log_file_pattern), timeout=20
        )


@pytest.mark.parametrize(
    "runtime_env",
    [
        {
            "container": {
                "image": "unknown_image",
                "pip": ["triton_on_ray"],
                "_pip_install_without_python_path": True,
            },
            "pip": ["serving_common_lib"],
        },
        {
            "container": {"image": "unknown_image", "pip": ["triton_on_ray"]},
            "pip": ["serving_common_lib"],
        },
        {
            "container": {
                "image": "unknown_image",
                "pip": ["triton_on_ray"],
                "_install_ray": True,
            },
            "pip": ["serving_common_lib"],
        },
    ],
)
def test_container_with_pip_packages(runtime_env, ray_start_regular):
    a = Counter.options(runtime_env=runtime_env).remote()

    try:
        ray.get(a.increment.remote(), timeout=1)
    except (ray.exceptions.RuntimeEnvSetupError, ray.exceptions.GetTimeoutError):
        # ignore the exception because container mode don't work in common
        # test environments.
        pass

    log_file_pattern_1 = "raylet.err"
    log_file_pattern_2 = "runtime_env_setup-01000000.log"
    pip_packages = runtime_env.get("pip")
    container_pip_packages = runtime_env.get("container").get("pip")
    merge_pip_packages = list(dict.fromkeys(pip_packages + container_pip_packages))
    json_dumps_container_pip_packages = base64.b64encode(
        json.dumps(container_pip_packages).encode("utf-8")
    ).decode("utf-8")
    json_dumps_merge_pip_packages = base64.b64encode(
        json.dumps(merge_pip_packages).encode("utf-8")
    ).decode("utf-8")
    keyword1 = "Installing python requirements to"
    keyword2 = f"\--packages {json_dumps_container_pip_packages}"
    keyword3 = f"\--packages {json_dumps_merge_pip_packages}"
    print(runtime_env.get("container").get("_install_ray", False))
    if runtime_env.get("_pip_install_without_python_path", False):
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword1, log_file_pattern_2), timeout=20
        )
        wait_for_condition(
            lambda: check_logs_by_keyword(keyword2, log_file_pattern_1), timeout=20
        )
    else:
        if runtime_env.get("container").get("_install_ray", False):
            print(keyword3)
            wait_for_condition(
                lambda: check_logs_by_keyword(keyword3, log_file_pattern_1), timeout=20
            )
        else:
            wait_for_condition(
                lambda: check_logs_by_keyword(keyword2, log_file_pattern_1), timeout=20
            )


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"_system_config": {"worker_resource_limits_enabled": True}}],
    indirect=True,
)
def test_container_with_resources_limit(ray_start_regular):
    a = Counter.options(
        runtime_env={
            "container": {
                "image": "unknown_image",
            }
        },
        num_cpus=1,
        memory=100 * 1024 * 1024,
    ).remote()

    try:
        ray.get(a.increment.remote(), timeout=1)
    except (ray.exceptions.RuntimeEnvSetupError, ray.exceptions.GetTimeoutError):
        # ignore the exception because container mode don't work in common
        # test environments.
        pass

    log_file_pattern = "raylet.err"
    keyword1 = "\--cpus=1"
    keyword2 = "\--memory=10485"
    wait_for_condition(
        lambda: check_logs_by_keyword(keyword1, log_file_pattern), timeout=20
    )

    wait_for_condition(
        lambda: check_logs_by_keyword(keyword2, log_file_pattern), timeout=20
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
