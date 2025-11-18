import logging
import os
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd

default_logger = logging.getLogger(__name__)


async def _create_impl(image_uri: str, logger: logging.Logger):
    # Pull image if it doesn't exist
    # Also get path to `default_worker.py` inside the image.
    import os
    use_nydus = False
    if os.getenv("RAY_PODMAN_USE_NYDUS", "false").lower() == "true":
        use_nydus = True
        python_executable = os.getenv("PYTHON_EXECUTABLE_PATH", "/tmp/run_python.sh")
        dir = os.path.dirname(os.path.abspath(__file__))
        get_worker_script = os.path.join(dir, "get_worker_path.py")

        pull_image_cmd = [
            "podman",
            "run",
            "--rm",
            "-v",
            f"{python_executable}:/tmp/run_python.sh",
            "-v",
            f"{get_worker_script}:/tmp/get_worker_path.py",
            "--rootfs",
            image_uri + ":O",
            "/tmp/run_python.sh",
            "/tmp/get_worker_path.py",
        ]
        if os.geteuid() != 0:
            pull_image_cmd = ["sudo", "-E"] + pull_image_cmd
    else:
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
    if use_nydus:
        lines = worker_path.strip().split("\n")
        for line in lines:
            if not line.startswith("time="):
                worker_path = line
                break
    return worker_path.strip()


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
    podman_use_nydus = False

    # Check if nydus is enabled
    if os.getenv("RAY_PODMAN_USE_NYDUS", "false").lower() == "true":
        podman_use_nydus = True

    # Propagate all host environment variables that have the prefix "RAY_"
    # This should include RAY_RAYLET_PID
    for env_var_name, env_var_value in os.environ.items():
        if env_var_name.startswith("RAY_"):
            env_vars[env_var_name] = env_var_value


    # When using nydus, inherit specific environment variables from os.environ
    if podman_use_nydus:
        # Define the specific environment variables to inherit
        nydus_env_keys = [
            "CLUSTER_NAME",
            "POD_IP",
            "NODE_IP",
            "ASTRA_TCPSTORE_MASTER_ADDR",
            "ASTRA_TCPSTORE_MASTER_PORT",
            "ALIPAY_POD_NAME",
            "OUTPUT_RESULT_FILE",
            "RAY_prestart_worker_first_driver",
            "RAY_memory_monitor_refresh_ms",
            "RAY_memory_usage_threshold",
            "NVTE_FUSED_ATTN",
            "RAY_DEDUP_LOGS",
            "RAY_FLOW_INSIGHT",
            "NCCL_DEBUG",
            "HYDRA_FULL_ERROR",
            "RAY_ULIMIT_FILE",
            "RAY_max_grpc_message_size",
            "PYTHONPATH",
            "OSS_KEY",
            "OSS_SECRET",
            "PYTORCH_CUDA_ALLOC_CONF",
            "NCCL_NVLS_ENABLE",
            "NCCL_CUMEM_ENABLE",
            "NCCL_AMEM_ENABLE",
            "NCCL_MAX_NCHANNELS",
            "TORCH_NCCL_AVOID_RECORD_STREAMS",
            "NCCL_DEBUG_SUBSYS",
            "GLOO_SOCKET_IFNAME",
            "KILL_ABNORMAL_JOB_AFTER_S",
            "NCCL_SOCKET_IFNAME",
            "LD_PRELOAD",
            "REAL_DISABLE_GPU_COMM",
            "ASTRA_SHARED_PATH",
            "CLUSTER_CACHE_DIR",
            "CHECKPOINTS_DIR",
            "SKIP_LOAD_CHECK",
            "CONFIG_BASE64",
            "EVALUATION_CONFIG_BASE64",
            "ANTLLM_PATH",
            "USE_MAX_V2",
            "ENABLE_JOB_MONITOR",
            "GLOO_SOCKET_IFNAME",
            "NCCL_SOCKET_IFNAME",
            "REAL_CUDA_VISIBLE_DEVICES",
        ]
        extra_env_keys = os.getenv("PODMAN_ENV_KEYS", "")
        extra_env_keys = extra_env_keys.split(",")
        nydus_env_keys.extend(extra_env_keys)

        # Inherit from os.environ if they exist
        for key in nydus_env_keys:
            if key in os.environ:
                env_vars[key] = os.environ[key]
    logger.info(f"Use podman with nydus: {podman_use_nydus}")

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
    if podman_use_nydus:
        if os.geteuid() != 0:
            container_command = ["sudo", "-E"] + container_command
        python_executable = os.getenv("PYTHON_EXECUTABLE_PATH", "/tmp/run_python.sh")
        container_command.extend(["-v", f"{python_executable}:/tmp/run_python.sh"])

        # Use glob patterns to discover and mount NVIDIA libraries dynamically
        import glob

        mount_commands = ["--privileged"]

        # NVIDIA device mounts - validate existence
        nvidia_devices = [
            "/dev/nvidiactl",
            "/dev/nvidia-uvm",
            "/dev/nvidia-uvm-tools",
            "/dev/nvidia-modeset"
        ]

        for device_path in nvidia_devices:
            if os.path.exists(device_path):
                mount_commands.extend(["-v", f"{device_path}:{device_path}"])

        # Discover NVIDIA GPU devices dynamically using glob
        gpu_device_pattern = "/dev/nvidia[0-9]*"
        for device_path in glob.glob(gpu_device_pattern):
            if os.path.exists(device_path):
                mount_commands.extend(["--device", device_path])

        # Discover CUDA installations dynamically using glob
        cuda_patterns = [
            "/usr/local/cuda",
            "/usr/local/cuda-*"
        ]

        for pattern in cuda_patterns:
            for cuda_path in glob.glob(pattern):
                if os.path.exists(cuda_path) and os.path.isdir(cuda_path):
                    mount_commands.extend(["-v", f"{cuda_path}:{cuda_path}:ro"])

        # Discover NCCL installations dynamically
        nccl_patterns = [
            "/usr/local/nccl*"
        ]

        for pattern in nccl_patterns:
            for nccl_path in glob.glob(pattern):
                if os.path.exists(nccl_path) and os.path.isdir(nccl_path):
                    mount_commands.extend(["-v", f"{nccl_path}:{nccl_path}:ro"])

        # Vulkan and EGL mounts - validate existence
        vulkan_egl_paths = [
            "/etc/vulkan",
            "/usr/share/egl",
            "/usr/share/glvnd"
        ]

        for path in vulkan_egl_paths:
            if os.path.exists(path) and os.path.isdir(path):
                mount_commands.extend(["-v", f"{path}:{path}:ro"])

        # RDMA/InfiniBand mounts - validate existence
        rdma_paths = [
            "/usr/lib64/libibverbs",
            "/usr/lib64/librdmacm",
            "/sys/class/infiniband",
            "/dev/infiniband"
        ]

        for path in rdma_paths:
            if os.path.exists(path):
                if os.path.isdir(path):
                    mount_commands.extend(["-v", f"{path}:{path}:ro"])
                else:  # device file
                    mount_commands.extend(["-v", f"{path}:{path}"])

        # Shared memory for NCCL - validate existence
        if os.path.exists("/dev/shm"):
            mount_commands.extend(["-v", "/dev/shm:/dev/shm"])

        # NVIDIA runtime sockets - validate existence
        socket_paths = [
            "/run/nvidia-persistenced/socket",
            "/run/nvidia-fabricmanager/socket"
        ]

        for socket_path in socket_paths:
            if os.path.exists(socket_path):
                mount_commands.extend(["-v", f"{socket_path}:{socket_path}"])

        # Additional system mounts - validate existence
        system_paths = [
            "/etc/ld.so.conf.d",
        ]

        for path in system_paths:
            if os.path.exists(path):
                mount_commands.extend(["-v", f"{path}:{path}:ro"])

        # Discover NVIDIA libraries in /usr/lib64 using glob
        nvidia_lib64_patterns = [
            "/usr/lib64/libnvidia-*.so",
            "/usr/lib64/libcuda.so",
            "/usr/lib64/libGLX_nvidia.so",
            "/usr/lib64/libEGL_nvidia.so",
            "/usr/lib64/libGLESv*_nvidia.so",
            "/usr/lib64/libnvidia-*.so.*",
            "/usr/lib64/libcuda.so.*",
            "/usr/lib64/libGLX_nvidia.so.*",
            "/usr/lib64/libEGL_nvidia.so.*",
            "/usr/lib64/libGLESv*_nvidia.so.*",
        ]

        for pattern in nvidia_lib64_patterns:
            for lib_path in glob.glob(pattern):
                if os.path.exists(lib_path):
                    mount_commands.extend(["-v", f"{lib_path}:{lib_path}:ro"])

        # Discover NVIDIA libraries in /usr/lib (32-bit) using glob
        nvidia_lib_patterns = [
            "/usr/lib/libnvidia-*.so",
            "/usr/lib/libcuda.so",
            "/usr/lib/libvdpau_nvidia.so",
            "/usr/lib/libnvcuvid.so"
            "/usr/lib/libnvidia-*.so.*",
            "/usr/lib/libcuda.so.*",
            "/usr/lib/libvdpau_nvidia.so.*",
            "/usr/lib/libnvcuvid.so.*"
        ]

        for pattern in nvidia_lib_patterns:
            for lib_path in glob.glob(pattern):
                if os.path.exists(lib_path):
                    mount_commands.extend(["-v", f"{lib_path}:{lib_path}:ro"])

        # Discover NVIDIA firmware files using glob
        firmware_patterns = [
            "/usr/lib/firmware/nvidia/*/gsp_*.bin"
        ]

        for pattern in firmware_patterns:
            for fw_path in glob.glob(pattern):
                if os.path.exists(fw_path):
                    mount_commands.extend(["-v", f"{fw_path}:{fw_path}:ro"])

        # Discover Vulkan and EGL config files using glob
        vulkan_config_patterns = [
            "/etc/vulkan/icd.d/nvidia*.json",
            "/etc/vulkan/implicit_layer.d/nvidia*.json",
            "/usr/share/egl/egl_external_platform.d/*nvidia*.json",
            "/usr/share/glvnd/egl_vendor.d/*nvidia*.json"
        ]

        for pattern in vulkan_config_patterns:
            for config_path in glob.glob(pattern):
                if os.path.exists(config_path):
                    mount_commands.extend(["-v", f"{config_path}:{config_path}:ro"])

        # Discover NVIDIA binaries using glob
        nvidia_bin_patterns = [
            "/usr/bin/nvidia-*",
            "/usr/bin/nv-fabricmanager",
        ]

        for pattern in nvidia_bin_patterns:
            for bin_path in glob.glob(pattern):
                if os.path.exists(bin_path) and os.path.isfile(bin_path):
                    mount_commands.extend(["-v", f"{bin_path}:{bin_path}:ro"])

        container_command.extend(mount_commands)
        container_command.append("--env")
        container_command.append("PATH=/root/.tnvm/versions/alinode/v5.20.3/bin:/opt/conda/bin:/opt/conda/bin:/opt/conda/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/odpscmd_public/bin:/opt/taobao/java/bin:/usr/local/cuda/bin")
        container_command.append("--env")
        container_command.append("LD_LIBRARY_PATH=/usr/lib64:/root/workdir/astra-build/Asystem-HybridEngine/astra_cache/build/lib:/opt/conda/lib/python3.10/site-packages/aistudio_common/reader/libs/:/opt/taobao/java/jre/lib/amd64/server/:/usr/local/cuda/lib64:/usr/local/nccl/lib:/usr/local/nccl2/lib:/usr/lib64/libibverbs:/usr/lib64/librdmacm")
        container_command.append("--ulimit")
        container_command.append("host")
        container_command.append("--pids-limit")
        container_command.append("0")

        container_command.append("--entrypoint")
        container_command.append("/tmp/run_python.sh")
        container_command.append("--rootfs")
        container_command.append(image_uri + ":O")
    else:
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

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        self.worker_path = await _create_impl(runtime_env.py_container_image(), logger)

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

        _modify_context_impl(
            runtime_env.py_container_image(),
            runtime_env.py_container_worker_path() or self.worker_path,
            runtime_env.py_container_run_options(),
            context,
            logger,
            self._ray_tmp_dir,
        )
