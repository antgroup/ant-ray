import json
import logging
import os
import subprocess
import shlex
import sys
from typing import Dict, List, Optional
import ray._private.runtime_env.constants as runtime_env_constants

from ray.util.annotations import DeveloperAPI
from ray.core.generated.common_pb2 import Language
from ray._private.services import get_ray_jars_dir, get_ray_native_library_dir
from ray._private.utils import update_envs

logger = logging.getLogger(__name__)


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
        native_libraries: List[Dict[str, str]] = None,
        preload_libraries: List[str] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        self.override_worker_entrypoint: Optional[str] = override_worker_entrypoint
        self.java_jars = java_jars or []
        self.native_libraries = native_libraries or {
            "lib_path": [],
            "code_search_path": [],
        }
        self.preload_libraries = preload_libraries or []

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str], language: Language):
        update_envs(self.env_vars)

        if language == Language.PYTHON and sys.platform == "win32":
            executable = [self.py_executable]
        elif language == Language.PYTHON:
            executable = ["exec", self.py_executable]
        elif language == Language.JAVA:
            executable = ["java"]
            ray_jars = os.path.join(get_ray_jars_dir(), "*")

            local_java_jars = []
            local_java_code_search_path = []
            for java_jar in self.java_jars:
                local_java_jars.append(f"{java_jar}/*")
                local_java_jars.append(java_jar)
                local_java_code_search_path.append(java_jar)

            # TODO(Jacky): Add working dir to classpath of Java workers.
            if self.native_libraries["code_search_path"]:
                local_java_code_search_path += self.native_libraries["code_search_path"]

            if local_java_code_search_path:
                old_path = None
                index = 0
                for args in passthrough_args:
                    if args.startswith("-Dray.job.code-search-path="):
                        old_path = args.split("-Dray.job.code-search-path=", 1)[-1]
                        break
                    index += 1
                new_path = ":".join(local_java_code_search_path)
                if old_path:
                    passthrough_args[
                        index
                    ] = f"-Dray.job.code-search-path={new_path}:{old_path}"
                else:
                    passthrough_args += [f"-Dray.job.code-search-path={new_path}"]

            class_path_args = ["-cp", ray_jars + ":" + str(":".join(local_java_jars))]
            passthrough_args = class_path_args + passthrough_args
        elif language == Language.CPP:
            executable = ["exec"]

            # set library path
            all_library_paths = get_ray_native_library_dir()
            ld_preload_env = os.environ.get(runtime_env_constants.PRELOAD_ENV_NAME, "")

            if self.native_libraries.get("lib_path", []):
                all_library_paths += ":"
                all_library_paths += ":".join(self.native_libraries["lib_path"])
            if self.preload_libraries:
                if ld_preload_env:
                    ld_preload_env += ":"
                ld_preload_env += ":".join(self.preload_libraries)
            os_paths = os.environ.get(runtime_env_constants.LIBRARY_PATH_ENV_NAME, None)
            if os_paths:
                all_library_paths += ":"
                all_library_paths += os_paths
            # TODO(Jacky Ma): Add working dir to library path of C++ workers.

            os.environ[runtime_env_constants.LIBRARY_PATH_ENV_NAME] = all_library_paths
            os.environ[runtime_env_constants.PRELOAD_ENV_NAME] = ld_preload_env
            # set code search path
            if self.native_libraries.get("code_search_path", []):
                old_path = None
                index = 0
                for args in passthrough_args:
                    if args.startswith("--ray_code_search_path="):
                        old_path = args.split("--ray_code_search_path=", 1)[-1]
                        break
                    index += 1
                new_path = ":".join(self.native_libraries["code_search_path"])
                if old_path:
                    passthrough_args[
                        index
                    ] = f"--ray_code_search_path={new_path}:{old_path}"
                else:
                    passthrough_args += [f"--ray_code_search_path={new_path}"]

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
