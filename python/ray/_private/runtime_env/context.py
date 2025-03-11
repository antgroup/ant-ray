import json
import logging
import os
import subprocess
import shlex
import sys
import base64
from typing import Dict, List, Optional, Any

import ray._private.runtime_env.constants as runtime_env_constants
from ray.util.annotations import DeveloperAPI
from ray.core.generated.common_pb2 import Language
from ray._private.services import get_ray_jars_dir
from ray._private.utils import update_envs

logger = logging.getLogger(__name__)


def set_java_jar_dirs_to_env_vars(
    java_jar_dirs_list: List[str], context: "RuntimeEnvContext"
):
    """Insert the path in RAY_JAVA_JARS_DIRS in the runtime env."""
    java_jar_dirs = ":".join(java_jar_dirs_list)
    logger.info(f"Setting archive path {java_jar_dirs} to context {context}.")
    if runtime_env_constants.RAY_JAVA_JARS_DIRS in context.env_vars:
        raise RuntimeError(
            f"{runtime_env_constants.RAY_JAVA_JARS_DIRS} already exists in "
            f"context.env_vars {context.env_vars}, this is not allowed, "
            "please check your runtime_env."
        )
    context.env_vars[runtime_env_constants.RAY_JAVA_JARS_DIRS] = java_jar_dirs


@DeveloperAPI
class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(
        self,
        command_prefix: List[str] = None,
        env_vars: Dict[str, str] = None,
        py_executable: Optional[str] = None,
        override_worker_entrypoint: Optional[str] = None,
        java_jars: List[str] = None,
        cwd: Optional[str] = None,
        symlink_dirs_to_cwd: List[str] = None,
        pyenv_folder: Optional[str] = None,
        native_libraries: List[Dict[str, str]] = None,
        container: Dict[str, Any] = None,
        entrypoint_prefix: List[str] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        self.override_worker_entrypoint: Optional[str] = override_worker_entrypoint
        self.java_jars = java_jars or []
        self.cwd = cwd
        self.symlink_dirs_to_cwd = symlink_dirs_to_cwd or []
        self.native_libraries = native_libraries or {
            "lib_path": [],
            "code_search_path": [],
        }
        self.pyenv_folder = pyenv_folder
        self.container = container or {}
        self.entrypoint_prefix = entrypoint_prefix or []

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str], language: Language):
        set_java_jar_dirs_to_env_vars(self.java_jars, self)

        if language == Language.PYTHON and sys.platform == "win32":
            executable = [self.py_executable]
        elif language == Language.PYTHON:
            executable = ["exec", self.py_executable]
        elif language == Language.JAVA:
            executable = ["java"]
            ray_jars = os.path.join(get_ray_jars_dir(), "*")

            local_java_jars = []
            for java_jar in self.java_jars:
                local_java_jars.append(f"{java_jar}/*")
                local_java_jars.append(java_jar)

            class_path_args = ["-cp", ray_jars + ":" + str(":".join(local_java_jars))]
            passthrough_args = class_path_args + passthrough_args
        elif sys.platform == "win32":
            executable = []
        else:
            executable = ["exec"]

        # By default, raylet uses the path to default_worker.py on host.
        # However, the path to default_worker.py inside the container
        # can be different. We need the user to specify the path to
        # default_worker.py inside the container.
        if self.override_worker_entrypoint:
            logger.debug(
                f"Changing the worker entrypoint from {passthrough_args[0]} to "
                f"{self.override_worker_entrypoint}."
            )
            passthrough_args[0] = self.override_worker_entrypoint

        updated_envs = update_envs(self.env_vars)

        if "container_command" in self.container:
            container_command = self.container["container_command"]
            updated_envs_list = []
            for k, v in updated_envs.items():
                updated_envs_list.append("--env")
                updated_envs_list.append(k + "=" + v)
            if runtime_env_constants.CONTAINER_ENV_PLACEHOLDER in container_command:
                container_placeholder = runtime_env_constants.CONTAINER_ENV_PLACEHOLDER
                index_to_replace = container_command.index(container_placeholder)
                container_command = (
                    container_command[:index_to_replace]
                    + updated_envs_list
                    + container_command[index_to_replace + 1 :]
                )

            if language == Language.PYTHON:
                passthrough_args.insert(0, self.py_executable)
                passthrough_args[1] = "-m ray._private.workers.default_worker"
            elif language == Language.JAVA:
                cp_param_index = 0
                passthrough_args.insert(0, "java")
                for idx, remaining_arg in enumerate(passthrough_args):
                    if remaining_arg == "-cp":
                        cp_param_index = idx
                        passthrough_args[idx + 1] = passthrough_args[idx + 1].replace(
                            ".pyenv", self.pyenv_folder
                        )
                passthrough_args.insert(
                    cp_param_index, "-DWORKER_SHIM_PID={}".format(os.getpid())
                )
            if self.entrypoint_prefix:
                # update install_ray pip packages to base64
                if "--packages" in self.entrypoint_prefix:
                    index = self.entrypoint_prefix.index("--packages")
                    pip_packages_str = self.entrypoint_prefix[index + 1]
                    logger.info(f"Install ray pip packages {pip_packages_str}")
                    self.entrypoint_prefix[index + 1] = base64.b64encode(
                        pip_packages_str.encode("utf-8")
                    ).decode("utf-8")
                passthrough_args = self.entrypoint_prefix + passthrough_args
            container_command.append(" ".join(passthrough_args))
            os.execvp(container_command[0], container_command)
            return

        if sys.platform == "win32":

            def quote(s):
                s = s.replace("&", "%26")
                return s

            passthrough_args = [quote(s) for s in passthrough_args]

            cmd = [*self.command_prefix, *executable, *passthrough_args]
            logger.debug(f"Exec'ing worker with command: {cmd}")
            subprocess.Popen(cmd, shell=True).wait()
        else:
            # We use shlex to do the necessary shell escape
            # of special characters in passthrough_args.
            passthrough_args = [shlex.quote(s) for s in passthrough_args]
            cmd = [*self.command_prefix, *executable, *passthrough_args]
            # TODO(SongGuyang): We add this env to command for macOS because it doesn't
            # work for the C++ process of `os.execvp`. We should find a better way to
            # fix it.
            MACOS_LIBRARY_PATH_ENV_NAME = "DYLD_LIBRARY_PATH"
            if MACOS_LIBRARY_PATH_ENV_NAME in os.environ:
                cmd.insert(
                    0,
                    f"{MACOS_LIBRARY_PATH_ENV_NAME}="
                    f"{os.environ[MACOS_LIBRARY_PATH_ENV_NAME]}",
                )
            logger.info(f"Exec'ing worker with command: {cmd}")
            # PyCharm will monkey patch the os.execvp at
            # .pycharm_helpers/pydev/_pydev_bundle/pydev_monkey.py
            # The monkey patched os.execvp function has a different
            # signature. So, we use os.execvp("executable", args=[])
            # instead of os.execvp(file="executable", args=[])
            os.execvp("bash", args=["bash", "-c", " ".join(cmd)])
