import argparse
import json
import logging
import os

from ray._private.utils import (import_attr, get_ray_site_packages_path,
                                get_specify_python_package)
from ray._private.ray_logging import setup_logger
from ray.ray_constants import (LOGGER_FORMAT, RAY_CONTAINER_MOUNT_DIR)

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description=(
        "Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--worker-setup-hook",
    type=str,
    help="the module path to a Python function to run to set up the "
    "environment for a worker and launch the worker.")

parser.add_argument(
    "--serialized-runtime-env",
    type=str,
    help="the serialized parsed runtime env dict")

parser.add_argument(
    "--language", type=str, help="the language type of the worker")

parser.add_argument(
    "--allocated-instances-serialized-json",
    type=str,
    help="the worker allocated resource")


def get_tmp_dir(remaining_args):
    for arg in remaining_args:
        if arg.startswith("--temp-dir="):
            return arg[11:]
    return None


# when work with Ant vgpu, the LD_PRELOAD env will set to
# /dev/shm/xpdk-lite/libcuda.so.1.
# Now job_agent will parse LD_PRELOAD and set into job_config,
# so we need add ORIGINAL_LD_PRELOAD into LD_PRELOAD.
def get_ld_preload():
    original_ld_preload = os.getenv("ORIGINAL_LD_PRELOAD", "")
    ld_preload = os.getenv("LD_PRELOAD", "")
    if ld_preload == "":
        return original_ld_preload
    if original_ld_preload == "":
        return ld_preload
    if ld_preload.endswith(":"):
        return ld_preload + original_ld_preload
    return ld_preload + ":" + original_ld_preload


def parse_allocated_resource(allocated_instances_serialized_json):
    container_resource_args = []
    allocated_resource = json.loads(allocated_instances_serialized_json)
    if "CPU" in allocated_resource.keys():
        cpu_resource = allocated_resource["CPU"]
        if isinstance(cpu_resource, list):
            cpus = 0
            for _, val in enumerate(cpu_resource):
                if val > 0:
                    cpus += val
            container_resource_args.append("--cpus=" + str(cpus))
        else:
            container_resource_args.append("--cpus=" + str(cpu_resource))
    if "memory" in allocated_resource.keys():
        container_resource_args.append("--memory=" + str(
            int(allocated_resource["memory"] / 1000000)) + "m")
    return container_resource_args


def start_worker_in_container(container_option, args, remaining_args):
    worker_setup_hook = args.worker_setup_hook
    last_period_idx = worker_setup_hook.rfind(".")
    module_name = worker_setup_hook[:last_period_idx]
    # python -m ray.workers.setup_runtime_env --session-dir=
    # default_worker.py --node-ip-address= ...
    entrypoint_args = ["python", "-m"]
    entrypoint_args.append(module_name)
    if args.language == "PYTHON":
        # In custom image, the python executable path is different,
        # we need to replace worker-entrypoint with python.
        remaining_args.append("--worker-entrypoint=python")
        # replace default_worker.py path
        if container_option.get("worker_path"):
            remaining_args[1] = container_option.get("worker_path")
        # in ANT-INTERNAL, it is required to specify worker_path.
        # because mount the ray package into RAY_CONTAINER_MOUNT_DIR.
        # When the raycluster upgrade, the job container don't need to upgrade.
        remaining_args[1] = \
            RAY_CONTAINER_MOUNT_DIR + "ray/workers/default_worker.py"
    elif args.language == "JAVA":
        remaining_args[1] = "--worker-entrypoint=java"
        for idx, remaining_arg in enumerate(remaining_args):
            if remaining_arg == "-cp":
                remaining_args[idx +
                               1] = RAY_CONTAINER_MOUNT_DIR + "ray/jars/*"
    entrypoint_args.extend(remaining_args)
    # setup_runtime_env will install conda,pip according to
    # serialized-runtime-env, so add this argument
    entrypoint_args.append("--serialized-runtime-env")
    entrypoint_args.append("'" + args.serialized_runtime_env + "'")
    # now we will start a container, add argument worker-shim-pid
    entrypoint_args.append("--worker-shim-pid={}".format(os.getpid()))

    tmp_dir = get_tmp_dir(remaining_args)
    if not tmp_dir:
        logger.error(
            "failed to get tmp_dir, the args: {}".format(remaining_args))

    # in ANT-INTERNAL, we need mount /home/admin/ray-pack,
    # because some config generated on starting
    tmp_dir = "/home/admin/ray-pack"

    container_driver = "podman"
    # todo add cgroup config
    container_command = [
        container_driver, "run", "--rm", "-v", tmp_dir + ":" + tmp_dir,
        "--cgroup-manager=cgroupfs", "--network=host", "--pid=host",
        "--ipc=host", "--env-host", "--cgroups=no-conmon"
    ]
    if container_option.get("name"):
        container_command.append("--name=" + container_option.get("name"))
    container_command.append("--env")
    container_command.append("RAY_RAYLET_PID=" + str(os.getppid()))
    if container_option.get("run_options"):
        container_command.extend(container_option.get("run_options"))
    container_command.extend(
        parse_allocated_resource(args.allocated_instances_serialized_json))

    # in ANT-INTERNAL, we need 'sudo' and 'admin', mount logs
    container_command = ["sudo", "-E"] + container_command
    container_command.append("-u")
    container_command.append("admin")
    container_command.append("-w")
    container_command.append(os.getcwd())
    container_command.append("--cap-add=AUDIT_WRITE")
    if container_option.get("customize_log_dir"):
        customize_log_path = container_option.get("customize_log_dir")
        if not os.path.exists(customize_log_path):
            os.makedirs(customize_log_path)
        container_command.append("-v")
        container_command.append(customize_log_path + ":/home/admin/logs")
        container_command.append("-v")
        container_command.append("/home/admin/logs/ray-logs/:/home/admin/logs"
                                 "/ray-logs")
        container_command.append("-v")
        container_command.append("/home/admin/logs:/home/admin/logs/share")
    else:
        container_command.append("-v")
        container_command.append("/home/admin/logs:/home/admin/logs")
    container_command.append("-v")
    container_command.append("/apsara:/apsara")
    host_site_packages_path = get_ray_site_packages_path()
    python_version = container_option.get("python")
    if python_version is not None:
        host_site_packages_path = get_specify_python_package(python_version)
        if host_site_packages_path is None:
            raise RuntimeError(
                "Python {} is not supported on current platform".format(
                    python_version))

    container_command.append("-v")
    container_command.append(host_site_packages_path + ":" +
                             RAY_CONTAINER_MOUNT_DIR)
    container_command.append("--env")
    container_command.append("PYTHONPATH=" + RAY_CONTAINER_MOUNT_DIR)
    # pass LD_PRELOAD env
    container_command.append("--env")
    container_command.append("LD_PRELOAD=" + get_ld_preload())
    # pass LD_LIBRARY_PATH env
    container_command.append("--env")
    container_command.append("LD_LIBRARY_PATH=" +
                             os.getenv("LD_LIBRARY_PATH", ""))
    # unset PYENV_VERSION, the image may use pyenv with other python.
    container_command.append("--env")
    container_command.append("PYENV_VERSION=")
    # pass CONTAINERNAME env
    if container_option.get("name"):
        container_command.append("--env")
        container_command.append("CONTAINER_NAME=" +
                                 container_option.get("name"))
    if args.language == "JAVA":
        container_command.append("--env")
        container_command.append("WORKER_SHIM_PID={}".format(os.getpid()))
    container_command.append("--entrypoint")
    # Some docker image use conda to run python, it depend on ~/.bashrc.
    # So we need to use bash as container entrypoint.
    container_command.append("bash")
    # in ANT-INTERNAL, we use nydus image as rootfs
    container_command.append("--rootfs")
    container_command.append(container_option.get("image") + ":O")
    container_command.extend(["-l", "-c", " ".join(entrypoint_args)])
    logger.info("start worker in container: {}".format(
        " ".join(container_command)))
    os.execvp(container_command[0], container_command)


if __name__ == "__main__":
    setup_logger(logging.INFO, LOGGER_FORMAT)
    args, remaining_args = parser.parse_known_args()
    runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")
    container_option = runtime_env.get("container")
    if container_option and container_option.get("image"):
        start_worker_in_container(container_option, args, remaining_args)
    else:
        mock_file = os.environ.get("mock_file")
        if not os.path.exists(mock_file):
            raise RuntimeError("mock error")

        remaining_args.append("--serialized-runtime-env")
        remaining_args.append(args.serialized_runtime_env or "{}")
        setup = import_attr(args.worker_setup_hook)
        setup(remaining_args)
