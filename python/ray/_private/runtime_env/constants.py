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

# ANT-INTERNAL: Alipay internal system config dynamic depot
INTERNAL_SYSTEM_CONFIG_DYNAMIC_FILE = "/lib/libsysconf-alipay.so"

# the key for java jar dirs in the environment variable.
RAY_JAVA_JARS_DIRS = "RAY_JAVA_JARS_DIRS"

# Whether to use ray whl when install_ray is True in the container.
RAY_USE_WHL_PACKAGE = env_bool("RAY_USE_WHL_PACKAGE", False)

# Whether podman integrate nydus
RAY_PODMAN_UES_NYDUS = env_bool("RAY_PODMAN_UES_NYDUS", True)

# The system log dir
RAY_PODMAN_SYSTEM_LOG_DIR = os.environ.get(
    "RAY_PODMAN_SYSTEM_LOG_DIR", "/home/admin/logs"
)

# Apsara Cloud system config dir
RAY_PODMAN_APSARA_SYSTEM_CONFIG_DIR = os.environ.get(
    "RAY_PODMAN_APSARA_SYSTEM_CONFIG_DIR", "/apsara"
)
