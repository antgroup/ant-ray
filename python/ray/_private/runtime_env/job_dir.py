import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.context import RuntimeEnvContext
import ray._private.runtime_env.constants as runtime_env_constans
from ray._private.runtime_env.packaging import (
    Protocol,
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
    get_uri_for_directory,
    get_uri_for_package,
    parse_uri,
    is_zip_uri,
    upload_package_if_needed,
    upload_package_to_gcs,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import (
    get_directory_size_bytes,
    try_to_create_directory,
)
from ray._private.runtime_env.working_dir import set_pythonpath_in_context
from ray._private.runtime_env.utils import rmtree_directory_with_privilege

default_logger = logging.getLogger(__name__)


def get_dir():
    return os.environ.get("RAY_RUNTIME_ENV_JOB_DIR")


def set_job_dir_path_in_context(
    local_job_working_dir_uris: str, context: RuntimeEnvContext
):
    """Insert the path in RAY_RUNTIME_ENV_JOB_DIR in the runtime env."""
    default_logger.info(
        f"Setting runtime env job dir path {local_job_working_dir_uris} to "
        f"context {context}."
    )
    if "RAY_RUNTIME_ENV_JOB_DIR" in context.env_vars:
        runtime_env_job_dir = context.env_vars["RAY_RUNTIME_ENV_JOB_DIR"]
        if runtime_env_job_dir != local_job_working_dir_uris:
            raise RuntimeError(
                f"RAY_RUNTIME_ENV_JOB_DIR already exists in "
                f"context.env_vars {context.env_vars}, "
                "this is not allowed, please check your in runtime_env."
            )
    context.env_vars["RAY_RUNTIME_ENV_JOB_DIR"] = local_job_working_dir_uris


def upload_job_dir_if_needed(
    runtime_env: Dict[str, Any],
    scratch_dir: Optional[str] = os.getcwd(),
    logger: Optional[logging.Logger] = default_logger,
    upload_fn=None,
) -> Dict[str, Any]:
    job_dir = runtime_env.get("job_dir")
    if job_dir is None:
        return runtime_env
    if isinstance(job_dir, bool):
        return runtime_env

    if not isinstance(job_dir, str) and not isinstance(job_dir, Path):
        raise TypeError(
            "job_dir must be a string or Path (either a local path "
            f"or remote URI), got {type(job_dir)}."
        )

    if isinstance(job_dir, Path):
        job_dir = str(job_dir)

    is_zip = False
    try:
        protocol, _ = parse_uri(job_dir)
        is_zip = is_zip_uri(job_dir)
    except ValueError:
        protocol, _ = None, None

    if protocol is not None:
        if protocol in Protocol.remote_protocols() and not is_zip:
            raise ValueError("Only .zip files supported for remote URIs.")
        return runtime_env

    excludes = runtime_env.get("excludes", None)
    try:
        job_dir_uri = get_uri_for_directory(job_dir, excludes=excludes)
    except ValueError:  # job_dir is not a directory
        package_path = Path(job_dir)
        if not package_path.exists() or package_path.suffix != ".zip":
            raise ValueError(
                f"directory {package_path} must be an existing "
                "directory or a zip package"
            )

        pkg_uri = get_uri_for_package(package_path)
        upload_package_to_gcs(pkg_uri, package_path.read_bytes())
        runtime_env["job_dir"] = pkg_uri
        return runtime_env
    if upload_fn is None:
        upload_package_if_needed(
            job_dir_uri,
            scratch_dir,
            job_dir,
            include_parent_dir=False,
            excludes=excludes,
            logger=logger,
        )
    else:
        upload_fn(job_dir, excludes=excludes)

    runtime_env["job_dir"] = job_dir_uri
    return runtime_env


class JobDirPlugin(RuntimeEnvPlugin):

    name = "job_dir"
    job_dir_placeholder = "$JOB_DIR_PLACEHOLDER"

    def __init__(self, resources_dir: str, gcs_aio_client: GcsAioClient):
        self._resources_dir = os.path.join(resources_dir, "job_dir_files")
        self._job_dirs = os.path.join(resources_dir, "job_dirs")
        self._gcs_aio_client = gcs_aio_client
        try_to_create_directory(self._resources_dir)

    async def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = await delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        job_dir_uri = runtime_env.job_dir()
        if isinstance(job_dir_uri, bool):
            return []
        if job_dir_uri != "":
            return [job_dir_uri]
        return []

    async def create(
        self,
        uri: Optional[str],
        runtime_env: dict,
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        job_dir = runtime_env.get("job_dir")
        if isinstance(job_dir, bool):
            return
        local_dir = await download_and_unpack_package(
            uri,
            self._resources_dir,
            self._gcs_aio_client,
            logger=logger,
            downloader=self.get_downloader(context),
        )
        return get_directory_size_bytes(local_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if (
            isinstance(runtime_env_dict["job_dir"], bool)
            and runtime_env_dict["job_dir"] is False
        ):
            return

        set_job_dir_path_in_context(JobDirPlugin.job_dir_placeholder, context=context)
        set_pythonpath_in_context(JobDirPlugin.job_dir_placeholder, context=context)
        context.job_dir = JobDirPlugin.job_dir_placeholder
        # JobDirPlugin uses a single URI.
        if uris:
            uri = uris[0]
            local_dir = get_local_dir_from_uri(uri, self._resources_dir)
            if not local_dir.exists():
                raise ValueError(
                    f"Local directory {local_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading or unpacking the job_dir."
                )

            context.symlink_dirs_to_job_dir.append(str(local_dir))

    async def pre_job_startup(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        job_id: str,
    ) -> None:
        if not runtime_env.get("job_dir") or runtime_env["job_dir"] is False:
            return
        default_logger.info(f"Creating job dir for job {job_id}.")
        job_dir = os.path.join(self._job_dirs, job_id)
        os.makedirs(job_dir, exist_ok=True)
        context.job_dir = job_dir

        # Add symbol links to the job dir.
        for symlink_dir in context.symlink_dirs_to_job_dir:
            for name in os.listdir(symlink_dir):
                src_path = os.path.join(symlink_dir, name)
                link_path = os.path.join(job_dir, name)
                default_logger.info(f"Creating symlink from {src_path} to {link_path}.")
                try:
                    os.symlink(src_path, link_path)
                except FileExistsError:
                    pass
        return

    async def pre_worker_startup(
        self,
        runtime_env: "RuntimeEnv",  # noqa F821
        context: RuntimeEnvContext,
        worker_id: str,
        job_id: str,
    ) -> None:
        if not runtime_env.get("job_dir"):
            return
        job_dir = os.path.join(self._job_dirs, job_id)
        # Replace the placeholder with the real job dir.
        for k, v in context.env_vars.copy().items():
            context.env_vars[k] = v.replace(
                JobDirPlugin.job_dir_placeholder, job_dir
            ).replace("${RAY_RUNTIME_ENV_JOB_DIR}", job_dir)
        return

    async def post_job_exit(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        job_id: str,
    ) -> None:

        if runtime_env_constans.DISABLE_JOB_DIR_GC:
            default_logger.info(
                f"Job directory GC has been disabled. The dir {job_id} "
                "won't be deleted"
            )
            return
        default_logger.info(f"Deleting job dir for job {job_id}.")
        job_dir = os.path.join(self._job_dirs, job_id)
        rmtree_directory_with_privilege(job_dir, default_logger, ignore_errors=True)
        return
