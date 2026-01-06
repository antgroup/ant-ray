import sys
import os
from ray._private.ray_constants import env_bool, env_integer

# Env var set by job manager to pass runtime env and metadata to subprocess
RAY_JOB_CONFIG_JSON_ENV_VAR = "RAY_JOB_CONFIG_JSON_ENV_VAR"

# The plugin config which should be loaded when ray cluster starts.
# It is a json formatted config,
# e.g. [{"class": "xxx.xxx.xxx_plugin", "priority": 10}].
RAY_RUNTIME_ENV_PLUGINS_ENV_VAR = "RAY_RUNTIME_ENV_PLUGINS"

# The field name of plugin class in the plugin config.
RAY_RUNTIME_ENV_CLASS_FIELD_NAME = "class"

# The field name of priority in the plugin config.
RAY_RUNTIME_ENV_PRIORITY_FIELD_NAME = "priority"

# The default priority of runtime env plugin.
RAY_RUNTIME_ENV_PLUGIN_DEFAULT_PRIORITY = 10

# The minimum priority of runtime env plugin.
RAY_RUNTIME_ENV_PLUGIN_MIN_PRIORITY = 0

# The maximum priority of runtime env plugin.
RAY_RUNTIME_ENV_PLUGIN_MAX_PRIORITY = 100

# The schema files or directories of plugins which should be loaded in workers.
RAY_RUNTIME_ENV_PLUGIN_SCHEMAS_ENV_VAR = "RAY_RUNTIME_ENV_PLUGIN_SCHEMAS"

# The file suffix of runtime env plugin schemas.
RAY_RUNTIME_ENV_PLUGIN_SCHEMA_SUFFIX = ".json"

# The names of the LIBRARY environment variable on different platforms.
_LINUX = sys.platform.startswith("linux")
_MACOS = sys.platform.startswith("darwin")
if _LINUX:
    LIBRARY_PATH_ENV_NAME = "LD_LIBRARY_PATH"
elif _MACOS:
    LIBRARY_PATH_ENV_NAME = "DYLD_LIBRARY_PATH"
else:
    # Win32
    LIBRARY_PATH_ENV_NAME = "PATH"

PRELOAD_ENV_NAME = "LD_PRELOAD"


# Container or image uri plugin placeholder, which will be replaced by env_vars.
CONTAINER_ENV_PLACEHOLDER = "$CONTAINER_ENV_PLACEHOLDER"

# the key for java jar dirs in the environment variable.
RAY_JAVA_JARS_DIRS = "RAY_JAVA_JARS_DIRS"

# Whether podman integrate nydus
RAY_PODMAN_USE_NYDUS = env_bool("RAY_PODMAN_USE_NYDUS", False)

# Default mount points for Podman containers.
# The format allows "{source_path}:{target_path}" for bind mounts
# Entries are separated by semicolons (e.g., "A:A;B:C").
RAY_PODMAN_DEFAULT_MOUNT_POINTS = os.environ.get("RAY_PODMAN_DEFAULT_MOUNT_POINTS", "")

# Dependencies installer script path in container, default is `/tmp/scripts/dependencies_installer.py`
RAY_PODMAN_DEPENDENCIES_INSTALLER_PATH = os.environ.get(
    "RAY_DEPENDENCIES_INSTALLER_PATH", "/tmp/scripts/dependencies_installer.py"
)

# Whether to use ray whl when `install_ray` is True in the container.
RAY_PODMAN_UES_WHL_PACKAGE = env_bool("RAY_PODMAN_UES_WHL_PACKAGE", False)

VGPU_DEVICES = os.getenv("NVIDIA_VISIBLE_DEVICES")

NVIDIA_PATTERNS=[
            # x86_64 libraries
            "/usr/lib/x86_64-linux-gnu/libnvidia-*.so",
            "/usr/lib/x86_64-linux-gnu/libcuda.so",
            "/usr/lib/x86_64-linux-gnu/libvdpau_nvidia.so",
            "/usr/lib/x86_64-linux-gnu/libnvcuvid.so",
            "/usr/lib/x86_64-linux-gnu/libnvidia-*.so.*",
            "/usr/lib/x86_64-linux-gnu/libcuda.so.*",
            "/usr/lib/x86_64-linux-gnu/libvdpau_nvidia.so.*",
            "/usr/lib/x86_64-linux-gnu/libnvcuvid.so.*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-cfg.so*",
            "/usr/lib/x86_64-linux-gnu/libcudadebugger.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-opencl.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-gpucomp.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-ptxjitcompiler.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-allocator.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-pkcs11.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-pkcs11-openssl3.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-nvvm.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-ngx.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-encode.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-opticalflow.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-eglcore.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-glcore.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-tls.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-glsi.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-fbc.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-rtcore.so*",
            "/usr/lib/x86_64-linux-gnu/libnvoptix.so*",
            "/usr/lib/x86_64-linux-gnu/libGLX_nvidia.so*",
            "/usr/lib/x86_64-linux-gnu/libEGL_nvidia.so*",
            "/usr/lib/x86_64-linux-gnu/libGLESv2_nvidia.so*",
            "/usr/lib/x86_64-linux-gnu/libGLESv1_CM_nvidia.so*",
            "/usr/lib/x86_64-linux-gnu/libnvidia-glvkspirv.so*",
            # i386 libraries
            "/usr/lib/i386-linux-gnu/libnvidia-*.so",
            "/usr/lib/i386-linux-gnu/libcuda.so",
            "/usr/lib/i386-linux-gnu/libvdpau_nvidia.so",
            "/usr/lib/i386-linux-gnu/libnvcuvid.so",
            "/usr/lib/i386-linux-gnu/libnvidia-*.so.*",
            "/usr/lib/i386-linux-gnu/libcuda.so.*",
            "/usr/lib/i386-linux-gnu/libvdpau_nvidia.so.*",
            "/usr/lib/i386-linux-gnu/libnvcuvid.so.*",
            "/usr/lib/i386-linux-gnu/libnvidia-ml.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-opencl.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-gpucomp.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-ptxjitcompiler.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-allocator.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-nvvm.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-encode.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-opticalflow.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-eglcore.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-glcore.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-tls.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-glsi.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-fbc.so*",
            "/usr/lib/i386-linux-gnu/libGLX_nvidia.so*",
            "/usr/lib/i386-linux-gnu/libEGL_nvidia.so*",
            "/usr/lib/i386-linux-gnu/libGLESv2_nvidia.so*",
            "/usr/lib/i386-linux-gnu/libGLESv1_CM_nvidia.so*",
            "/usr/lib/i386-linux-gnu/libnvidia-glvkspirv.so*",
            # lib64 libraries
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
            "/usr/lib64/libnvidia-egl-gbm.so*",
            "/usr/lib64/libnvidia-egl-wayland.so*",
            # lib libraries
            "/usr/lib/libnvidia-*.so",
            "/usr/lib/libcuda.so",
            "/usr/lib/libvdpau_nvidia.so",
            "/usr/lib/libnvcuvid.so",
            "/usr/lib/libnvidia-*.so.*",
            "/usr/lib/libcuda.so.*",
            "/usr/lib/libvdpau_nvidia.so.*",
            "/usr/lib/libnvcuvid.so.*",
            # firmware files
            "/usr/lib/firmware/nvidia/*/gsp_*.bin",
            "/usr/share/nvidia/nvoptix.bin",
            # Vulkan and EGL config files
            "/etc/vulkan/icd.d/nvidia*.json",
            "/etc/vulkan/implicit_layer.d/nvidia*.json",
            "/usr/share/egl/egl_external_platform.d/*nvidia*.json",
            "/usr/share/glvnd/egl_vendor.d/*nvidia*.json",
            # NVIDIA binaries
            "/usr/bin/nvidia-*",
            "/usr/bin/nv-fabricmanager",
            "/usr/bin/nvidia-smi",
            "/usr/bin/nvidia-debugdump",
            "/usr/bin/nvidia-persistenced",
            "/usr/bin/nvidia-cuda-mps-control",
            "/usr/bin/nvidia-cuda-mps-server",
            # Xorg modules
            "/usr/lib64/xorg/modules/drivers/nvidia_drv.so*",
            "/usr/lib64/xorg/modules/extensions/libglxserver_nvidia.so*",
            # Additional system libraries
            "/usr/lib64/libsysconf-alipay.so*",
            "/usr/local/cuda/compat/",
        ]