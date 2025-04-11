import logging
import os
import json
from typing import List, Optional
import ray
import ray._private.runtime_env.constants as runtime_env_constants
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd

from ray._private.utils import (
    get_ray_site_packages_path,
    get_pyenv_path,
    get_ray_whl_dir,
    get_dependencies_installer_path,
)

default_logger = logging.getLogger(__name__)

dependencies_installer_path = "/tmp/scripts/dependencies_installer.py"

# NOTE(chenk008): it is moved from setup_worker. And it will be used
# to setup resource limit.
def parse_allocated_resource(allocated_instances_serialized_json):
    container_resource_args = []
    allocated_resource = json.loads(allocated_instances_serialized_json)
    if "CPU" in allocated_resource.keys():
        cpu_resource = allocated_resource["CPU"]
        if isinstance(cpu_resource, list):
            # cpuset: because we may split one cpu core into some pieces,
            # we need set cpuset.cpu_exclusive=0 and set cpuset-cpus
            cpu_ids = []
            cpus = 0
            for idx, val in enumerate(cpu_resource):
                if val > 0:
                    cpu_ids.append(idx)
                    cpus += val
            container_resource_args.append("--cpus=" + str(int(cpus / 10000)))
            container_resource_args.append(
                "--cpuset-cpus=" + ",".join(str(e) for e in cpu_ids)
            )
        else:
            # cpushare
            container_resource_args.append("--cpus=" + str(int(cpu_resource / 10000)))
    if "memory" in allocated_resource.keys():
        container_resource_args.append(
            "--memory=" + str(int(allocated_resource["memory"] / 10000 / 10000)) + "m"
        )
    return container_resource_args


async def _create_impl(image_uri: str, logger: logging.Logger):
    # Pull image if it doesn't exist
    # Also get path to `default_worker.py` inside the image.
    pull_image_cmd = [
        "podman",
        "run",
        "--rm",
        image_uri,
        "python",
        "-c",
        (
            "import ray._private.workers.default_worker as default_worker; "
            "print(default_worker.__file__)"
        ),
    ]
    logger.info("Pulling image %s", image_uri)
    worker_path = await check_output_cmd(pull_image_cmd, logger=logger)
    return worker_path.strip()


container_placeholder = runtime_env_constants.CONTAINER_ENV_PLACEHOLDER


def _modify_container_context_impl(
    runtime_env: "RuntimeEnv",  # noqa: F821
    context: RuntimeEnvContext,
    ray_tmp_dir: str,
    worker_path: str,
    logger: Optional[logging.Logger] = default_logger,
):
    if not runtime_env.py_container_image():
        return
    context.override_worker_entrypoint = worker_path

    logger.info(f"container setup for {runtime_env}")
    container_option = runtime_env.get("container")

    # Use the user's python executable if py_executable is not None.
    py_executable = container_option.get("py_executable")

    install_ray = runtime_env.container_install_ray()
    pip_install_without_python_path = (
        runtime_env.container_pip_install_without_python_path()
    )

    container_driver = "podman"
    # todo add cgroup config
    container_command = [
        container_driver,
        "run",
        "--rm",
        "-v",
        ray_tmp_dir + ":" + ray_tmp_dir,
        "--cgroup-manager=cgroupfs",
        "--network=host",
        "--pid=host",
        "--ipc=host",
        "--env-host",
        "--cgroups=no-conmon",  # ANT-INTERNAL
        # NOTE(zcin): Mounted volumes in rootless containers are
        # owned by the user `root`. The user on host (which will
        # usually be `ray` if this is being run in a ray docker
        # image) who started the container is mapped using user
        # namespaces to the user `root` in a rootless container. In
        # order for the Ray Python worker to access the mounted ray
        # tmp dir, we need to use keep-id mode which maps the user
        # as itself (instead of as `root`) into the container.
        # https://www.redhat.com/sysadmin/rootless-podman-user-namespace-modes
        "--userns=keep-id",
    ]

    # Note(Jacky): If the same target_path is present in the container mount,
    # the image will not start due to the duplicate mount target path error,
    # so we create a reverse dict mapping,
    # which can modify the latest source_path.
    container_to_host_mount_dict = {}

    # in ANT-INTERNAL, we need mount /home/admin/ray-pack,
    # because some config generated on starting
    tmp_dir = "/home/admin/ray-pack"
    if os.path.exists(tmp_dir):
        container_to_host_mount_dict[tmp_dir] = tmp_dir

    entrypoint_args = []
    if install_ray and pip_install_without_python_path:
        raise RuntimeError(
            "_install_ray and _pip_install_without_python_path can't both be True, "
            "please check your runtime_env field."
        )
    container_pip_packages = runtime_env.py_container_pip_list()
    pip_packages = runtime_env.pip_config().get("packages", [])

    install_ray_or_pip_packages_command = None
    if install_ray:
        if install_ray_or_pip_packages_command is None:
            install_ray_or_pip_packages_command = [
                "python",
                dependencies_installer_path,
            ]
            if runtime_env_constants.RAY_USE_WHL_PACKAGE:
                install_ray_or_pip_packages_command.extend(
                    [
                        "--whl-dir",
                        get_ray_whl_dir(),
                    ]
                )
            else:
                install_ray_or_pip_packages_command.extend(
                    [
                        "--ray-version",
                        f"{ray.__version__}",
                    ]
                )
            if pip_packages or container_pip_packages:
                merge_pip_packages = list(
                    dict.fromkeys(pip_packages + container_pip_packages)
                )
                install_ray_or_pip_packages_command.extend(
                    [
                        "--packages",
                        json.dumps(merge_pip_packages),
                    ]
                )
            context.py_executable = "python"

    if container_pip_packages:
        if install_ray_or_pip_packages_command is None:
            install_ray_or_pip_packages_command = [
                "python",
                dependencies_installer_path,
                "--packages",
                json.dumps(container_pip_packages),
            ]
            if pip_install_without_python_path:
                install_ray_or_pip_packages_command.extend(
                    ["--without-python-path", "true"]
                )

    if install_ray_or_pip_packages_command is not None:
        install_ray_or_pip_packages_command.append("&&")
        entrypoint_args.extend(install_ray_or_pip_packages_command)

    context.entrypoint_prefix = entrypoint_args
    context.env_vars["RAY_RAYLET_PID"] = os.getenv("RAY_RAYLET_PID")
    """"
    container_command.extend(
        parse_allocated_resource(serialized_allocated_resource_instances)
    )
    """
    # we need 'sudo' and 'admin', mount logs
    container_command = ["sudo", "-E"] + container_command
    container_command.append("-u")
    user = container_option.get("user")
    if user:
        container_command.append(user)
    else:
        container_command.append("admin")
    container_command.append("-w")
    if context.working_dir:
        container_command.append(context.working_dir)
    else:
        container_command.append(os.getcwd())
    container_command.append("--cap-add=AUDIT_WRITE")
    if os.path.exists(runtime_env_constants.RAY_PODMAN_SYSTEM_LOG_DIR):
        log_dir = runtime_env_constants.RAY_PODMAN_SYSTEM_LOG_DIR
        if container_option.get("customize_log_dir"):
            customize_log_path = container_option.get("customize_log_dir")
            if not os.path.exists(customize_log_path):
                os.makedirs(customize_log_path)
                container_to_host_mount_dict[log_dir] = customize_log_path
                ray_log_dir = os.path.join(log_dir, "ray_logs")
                container_to_host_mount_dict[ray_log_dir] = ray_log_dir
                container_to_host_mount_dict[os.path.join(log_dir, "share")] = log_dir
        else:
            container_to_host_mount_dict[log_dir] = log_dir
    if os.path.exists(runtime_env_constants.RAY_PODMAN_APSARA_SYSTEM_CONFIG_DIR):
        aspara_system_config_dir = (
            runtime_env_constants.RAY_PODMAN_APSARA_SYSTEM_CONFIG_DIR
        )
        container_to_host_mount_dict[
            aspara_system_config_dir
        ] = aspara_system_config_dir
    if install_ray or container_pip_packages:
        container_to_host_mount_dict[
            dependencies_installer_path
        ] = get_dependencies_installer_path()
        container_to_host_mount_dict[get_ray_whl_dir()] = get_ray_whl_dir()

    # If install_ray is not selected, we will mount the pyenv virtual environment
    # and the directory where `ant-ray` is located into the image.
    if not install_ray:
        host_site_packages_path = get_ray_site_packages_path()
        if py_executable:
            # Replace the pyenv path in container to
            # avoid overwriting the user's pyenv.
            pyenv_folder = "ray/.pyenv"
        else:
            pyenv_folder = ".pyenv"
        host_pyenv_path = get_pyenv_path()
        container_pyenv_path = host_pyenv_path.replace(".pyenv", pyenv_folder)
        container_to_host_mount_dict[container_pyenv_path] = host_pyenv_path
        context.pyenv_folder = pyenv_folder
        container_to_host_mount_dict[host_site_packages_path] = host_site_packages_path

    if os.path.exists(runtime_env_constants.INTERNAL_SYSTEM_CONFIG_DYNAMIC_FILE):
        system_dynamic_config_file_path = (
            runtime_env_constants.INTERNAL_SYSTEM_CONFIG_DYNAMIC_FILE
        )
        container_to_host_mount_dict[
            system_dynamic_config_file_path
        ] = system_dynamic_config_file_path

    # For loop `run options` and append each item to the command line of podman
    if runtime_env.py_container_run_options():
        run_options_list = runtime_env.py_container_run_options()
        index = 0
        while index < len(run_options_list):
            run_option = run_options_list[index]
            if run_option == "-v":
                if index == len(run_options_list) - 1:
                    raise RuntimeError(
                        "Incorrect mount path command, "
                        "please check the container field "
                        "in the run_options field mount path, "
                        "`-v host_path:container_path`."
                    )
                next_option = run_options_list[index + 1]
                paths = next_option.split(":")
                if len(paths) != 2:
                    raise RuntimeError(
                        "Incorrect mount path command, "
                        "please check the container field "
                        "in the run_options field mount path, "
                        f"got {next_option}"
                    )
                source_path = paths[0].strip()
                target_path = paths[1].strip()
                # Iterate over the key of
                # 'container_to_host_mount_dict'
                # to find if target_path already exists
                container_to_host_mount_dict[target_path] = source_path
                index += 2
            else:
                container_command.append(run_option)
                index += 1

    for (
        target_path,
        source_path,
    ) in container_to_host_mount_dict.items():
        container_command.append("-v")
        container_command.append(f"{source_path}:{target_path}")
    if container_option.get("native_libraries"):
        container_native_libraries = container_option["native_libraries"]
        context.native_libraries["code_search_path"].append(container_native_libraries)
    context.env_vars["RAY_JOB_DATA_DIR_BASE"] = os.getenv("RAY_JOB_DATA_DIR_BASE", "")
    # unset PYENV_VERSION, the image may use pyenv with other python.
    context.env_vars["PYENV_VERSION"] = ""
    # Append env vars to container
    if context.env_vars.get("PYTHONPATH") and py_executable:
        context.env_vars["PYTHONPATH"] = context.env_vars["PYTHONPATH"].replace(
            ".pyenv", "ray/.pyenv"
        )
    container_command.append(container_placeholder)
    container_command.append("--entrypoint")
    # Some docker image use conda to run python, it depend on ~/.bashrc.
    # So we need to use bash as container entrypoint.
    container_command.append("bash")

    # If podman integrate nydus, we use nydus image as rootfs
    if runtime_env_constants.RAY_PODMAN_UES_NYDUS:
        container_command.append("--rootfs")
        container_command.append(runtime_env.py_container_image() + ":O")
    else:
        container_command.append(runtime_env.py_container_image())
    container_command.extend(["-l", "-c"])
    context.container["container_command"] = container_command

    if py_executable:
        context.py_executable = py_executable
    # Example:
    # sudo -E podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host --env-host --cgroups=no-conmon
    # --userns=keep-id -v /home/admin/ray-pack:/home/admin/ray-pack --env RAY_RAYLET_PID=23478 --env RAY_JOB_ID=$RAY_JOB_ID
    # --entrypoint python rayproject/ray:nightly-py39
    logger.info(
        "start worker in container with prefix: {}".format(" ".join(container_command))
    )


def _modify_context_impl(
    image_uri: str,
    worker_path: str,
    run_options: Optional[List[str]],
    context: RuntimeEnvContext,
    logger: logging.Logger,
    ray_tmp_dir: str,
):
    context.override_worker_entrypoint = worker_path

    container_driver = "podman"
    container_command = [
        container_driver,
        "run",
        "-v",
        ray_tmp_dir + ":" + ray_tmp_dir,
        "--cgroup-manager=cgroupfs",
        "--network=host",
        "--pid=host",
        "--ipc=host",
        # NOTE(zcin): Mounted volumes in rootless containers are
        # owned by the user `root`. The user on host (which will
        # usually be `ray` if this is being run in a ray docker
        # image) who started the container is mapped using user
        # namespaces to the user `root` in a rootless container. In
        # order for the Ray Python worker to access the mounted ray
        # tmp dir, we need to use keep-id mode which maps the user
        # as itself (instead of as `root`) into the container.
        # https://www.redhat.com/sysadmin/rootless-podman-user-namespace-modes
        "--userns=keep-id",
    ]

    # Environment variables to set in container
    env_vars = dict()

    # Propagate all host environment variables that have the prefix "RAY_"
    # This should include RAY_RAYLET_PID
    for env_var_name, env_var_value in os.environ.items():
        if env_var_name.startswith("RAY_"):
            env_vars[env_var_name] = env_var_value

    # Support for runtime_env['env_vars']
    env_vars.update(context.env_vars)

    # Set environment variables
    for env_var_name, env_var_value in env_vars.items():
        container_command.append("--env")
        container_command.append(f"{env_var_name}='{env_var_value}'")

    # The RAY_JOB_ID environment variable is needed for the default worker.
    # It won't be set at the time setup() is called, but it will be set
    # when worker command is executed, so we use RAY_JOB_ID=$RAY_JOB_ID
    # for the container start command
    container_command.append("--env")
    container_command.append("RAY_JOB_ID=$RAY_JOB_ID")

    if run_options:
        container_command.extend(run_options)
    # TODO(chenk008): add resource limit
    container_command.append("--entrypoint")
    container_command.append("python")
    container_command.append(image_uri)

    # Example:
    # podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host
    # --userns=keep-id --env RAY_RAYLET_PID=23478 --env RAY_JOB_ID=$RAY_JOB_ID
    # --entrypoint python rayproject/ray:nightly-py39
    container_command_str = " ".join(container_command)
    logger.info(f"Starting worker in container with prefix {container_command_str}")

    context.py_executable = container_command_str


class ImageURIPlugin(RuntimeEnvPlugin):
    """Starts worker in a container of a custom image."""

    name = "image_uri"

    @staticmethod
    def get_compatible_keys():
        return {"image_uri", "config", "env_vars"}

    def __init__(self, ray_tmp_dir: str):
        self._ray_tmp_dir = ray_tmp_dir

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        if not runtime_env.image_uri():
            return

        self.worker_path = await _create_impl(runtime_env.image_uri(), logger)

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.image_uri():
            return

        _modify_context_impl(
            runtime_env.image_uri(),
            self.worker_path,
            [],
            context,
            logger,
            self._ray_tmp_dir,
        )


class ContainerPlugin(RuntimeEnvPlugin):
    """Starts worker in container."""

    name = "container"

    def __init__(self, ray_tmp_dir: str):
        self._ray_tmp_dir = ray_tmp_dir

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        if runtime_env.py_container_worker_path():
            logger.warning(
                "You are using `container.worker_path`, but the path to "
                "`default_worker.py` is now automatically detected from the image. "
                "`container.worker_path` is deprecated and will be removed in future "
                "versions."
            )

        _modify_container_context_impl(
            runtime_env=runtime_env,
            context=context,
            ray_tmp_dir=self._ray_tmp_dir,
            worker_path=runtime_env.py_container_worker_path(),
            logger=logger,
        )
