"""Ray constants used in the Python code."""

import logging
import math
import os

logger = logging.getLogger(__name__)


def gcs_task_scheduling_enabled():
    return not os.environ.get("RAY_GCS_TASK_SCHEDULING_ENABLED") == "false"


def runtime_resource_scheduling_enabled():
    return not os.environ.get(
        "RAY_runtime_resource_scheduling_enabled") == "false"


def runtime_resources_calculation_interval_s():
    return int(
        os.environ.get("RAY_runtime_resources_calculation_interval_s", 600))


def runtime_memory_tail_percentile():
    return float(os.environ.get("RAY_runtime_memory_tail_percentile", 1.0))


def runtime_cpu_tail_percentile():
    return float(os.environ.get("RAY_runtime_cpu_tail_percentile", 0.95))


def default_job_total_memory_mb():
    return int(os.environ.get("RAY_default_job_total_memory_mb", 4000))


def memory_monitor_refresh_ms():
    return int(os.environ.get("RAY_memory_monitor_refresh_ms", 0))


def env_integer(key, default):
    if key in os.environ:
        value = os.environ[key]
        if value.isdigit():
            return int(os.environ[key])

        logger.debug(f"Found {key} in environment, but value must "
                     f"be an integer. Got: {value}. Returning "
                     f"provided default {default}.")
        return default
    return default


def env_bool(key, default):
    if key in os.environ:
        return True if os.environ[key].lower() == "true" else False
    return default


# Whether event logging to driver is enabled. Set to 0 to disable.
AUTOSCALER_EVENTS = env_integer("RAY_SCHEDULER_EVENTS", 1)

# Internal kv keys for storing monitor debug status.
DEBUG_AUTOSCALING_ERROR = "__autoscaling_error"
DEBUG_AUTOSCALING_STATUS = "__autoscaling_status"
DEBUG_AUTOSCALING_STATUS_LEGACY = "__autoscaling_status_legacy"

ID_SIZE = 28

# The default maximum number of bytes to allocate to the object store unless
# overridden by the user.
# ANT-INTERNAL: Cap object_store_memory to 8G.
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 8 * 10**9
# The default proportion of available memory allocated to the object store
# ANT-INTERNAL: use 10% memory for object store.
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.1
# The smallest cap on the memory used by the object store that we allow.
# This must be greater than MEMORY_RESOURCE_UNIT_BYTES
OBJECT_STORE_MINIMUM_MEMORY_BYTES = 75 * 1024 * 1024
# The default maximum number of bytes that the non-primary Redis shards are
# allowed to use unless overridden by the user.
DEFAULT_REDIS_MAX_MEMORY_BYTES = 10**10
# The smallest cap on the memory used by Redis that we allow.
REDIS_MINIMUM_MEMORY_BYTES = 10**7
# Above this number of bytes, raise an error by default unless the user sets
# RAY_ALLOW_SLOW_STORAGE=1. This avoids swapping with large object stores.
REQUIRE_SHM_SIZE_THRESHOLD = 10**10
# If a user does not specify a port for the primary Ray service,
# we attempt to start the service running at this port.
DEFAULT_PORT = 6379

RAY_ADDRESS_ENVIRONMENT_VARIABLE = "RAY_ADDRESS"

DEFAULT_DASHBOARD_IP = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 8265
# ANT-INTERNAL
REDIS_KEY_DASHBOARD = "api_server"
PROMETHEUS_SERVICE_DISCOVERY_FILE = "prom_metrics_service_discovery.json"
# Default resource requirements for actors when no resource requirements are
# specified.
DEFAULT_ACTOR_METHOD_CPU_SIMPLE = 0 if gcs_task_scheduling_enabled() else 1
DEFAULT_ACTOR_CREATION_CPU_SIMPLE = 0.01 if gcs_task_scheduling_enabled(
) else 0
# Default resource requirements for actors when some resource requirements are
# specified in .
DEFAULT_ACTOR_METHOD_CPU_SPECIFIED = 0
DEFAULT_ACTOR_CREATION_CPU_SPECIFIED = 0.01 if gcs_task_scheduling_enabled(
) else 1
# Default number of return values for each actor method.
DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS = 1

# If a remote function or actor (or some other export) has serialized size
# greater than this quantity, print an warning.
PICKLE_OBJECT_WARNING_SIZE = 10**7

# If remote functions with the same source are imported this many times, then
# print a warning.
DUPLICATE_REMOTE_FUNCTION_THRESHOLD = 100

# The maximum resource quantity that is allowed. TODO(rkn): This could be
# relaxed, but the current implementation of the node manager will be slower
# for large resource quantities due to bookkeeping of specific resource IDs.
MAX_RESOURCE_QUANTITY = 100e12

# Each memory "resource" counts as this many bytes of memory.
MEMORY_RESOURCE_UNIT_BYTES = 1

# Number of units 1 resource can be subdivided into.
MIN_RESOURCE_GRANULARITY = 0.0001


def round_to_memory_units(memory_bytes, round_up):
    """Round bytes to the nearest memory unit."""
    return from_memory_units(to_memory_units(memory_bytes, round_up))


def from_memory_units(memory_units):
    """Convert from memory units -> bytes."""
    return memory_units * MEMORY_RESOURCE_UNIT_BYTES


def to_memory_units(memory_bytes, round_up):
    """Convert from bytes -> memory units."""
    value = memory_bytes / MEMORY_RESOURCE_UNIT_BYTES
    if value < 1:
        raise ValueError(
            "The minimum amount of memory that can be requested is {} bytes, "
            "however {} bytes was asked.".format(MEMORY_RESOURCE_UNIT_BYTES,
                                                 memory_bytes))
    if isinstance(value, float) and not value.is_integer():
        # TODO(ekl) Ray currently does not support fractional resources when
        # the quantity is greater than one. We should fix memory resources to
        # be allocated in units of bytes and not 100MB.
        if round_up:
            value = int(math.ceil(value))
        else:
            value = int(math.floor(value))
    return int(value)


def is_multiple_of_memory_unit(memory_bytes):
    """Check that if the memory bytes is the positive integral multiple of
        memory units."""
    memory_units = memory_bytes / MEMORY_RESOURCE_UNIT_BYTES
    return memory_bytes >= MEMORY_RESOURCE_UNIT_BYTES and memory_units == int(
        memory_units)


# Different types of Ray errors that can be pushed to the driver.
# TODO(rkn): These should be defined in flatbuffers and must be synced with
# the existing C++ definitions.
WAIT_FOR_CLASS_PUSH_ERROR = "wait_for_class"
PICKLING_LARGE_OBJECT_PUSH_ERROR = "pickling_large_object"
WAIT_FOR_FUNCTION_PUSH_ERROR = "wait_for_function"
TASK_PUSH_ERROR = "task"
REGISTER_REMOTE_FUNCTION_PUSH_ERROR = "register_remote_function"
FUNCTION_TO_RUN_PUSH_ERROR = "function_to_run"
VERSION_MISMATCH_PUSH_ERROR = "version_mismatch"
CHECKPOINT_PUSH_ERROR = "checkpoint"
REGISTER_ACTOR_PUSH_ERROR = "register_actor"
WORKER_CRASH_PUSH_ERROR = "worker_crash"
WORKER_DIED_PUSH_ERROR = "worker_died"
WORKER_POOL_LARGE_ERROR = "worker_pool_large"
PUT_RECONSTRUCTION_PUSH_ERROR = "put_reconstruction"
INFEASIBLE_TASK_ERROR = "infeasible_task"
RESOURCE_DEADLOCK_ERROR = "resource_deadlock"
REMOVED_NODE_ERROR = "node_removed"
MONITOR_DIED_ERROR = "monitor_died"
LOG_MONITOR_DIED_ERROR = "log_monitor_died"
DASHBOARD_AGENT_DIED_ERROR = "dashboard_agent_died"
DASHBOARD_DIED_ERROR = "dashboard_died"
RAYLET_CONNECTION_ERROR = "raylet_connection_error"
DETACHED_ACTOR_ANONYMOUS_NAMESPACE_ERROR = "detached_actor_anonymous_namespace"

# Used in gpu detection
RESOURCE_CONSTRAINT_PREFIX = "accelerator_type:"

RESOURCES_ENVIRONMENT_VARIABLE = "RAY_OVERRIDE_RESOURCES"
RESOURCE_CONSTRAINT_PREFIX = "GPUType:"

# Default number of cpu instruction set resources.
DEFAULT_CPU_INSTRUCTION_SET_RESOURCE_NUMBER = 512.0

# Default list of cpu instruction set, like "avx, avx2".
CPU_INSTRUCTION_SETS = ""
CPU_INSTRUCTION_SETS_ENV = "CPU_INSTRUCTION_SET"
CPU_INSTRUCTION_SETS_FLAGS = "flags"

# The reporter will report its statistics this often (milliseconds).
REPORTER_UPDATE_INTERVAL_MS = env_integer("REPORTER_UPDATE_INTERVAL_MS", 2500)

# Number of attempts to ping the Redis server. See
# `services.py:wait_for_redis_to_start`.
START_REDIS_WAIT_RETRIES = env_integer("RAY_START_REDIS_WAIT_RETRIES", 60)

LOGGER_FORMAT = (
    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s")
LOGGER_FORMAT_HELP = f"The logging format. default='{LOGGER_FORMAT}'"
LOGGER_LEVEL = "INFO"
# We define the intersect logging levels of Python and Java.
# Python logging
# _nameToLevel = {
#     'CRITICAL': CRITICAL,
#     'FATAL': FATAL,
#     'ERROR': ERROR,
#     'WARN': WARNING,
#     'WARNING': WARNING,
#     'INFO': INFO,
#     'DEBUG': DEBUG,
#     'NOTSET': NOTSET,
# }
#
# Standard log levels built-in to Log4J
# Standard Level	intLevel
# OFF	0
# FATAL	100
# ERROR	200
# WARN	300
# INFO	400
# DEBUG	500
# TRACE	600
# ALL	Integer.MAX_VALUE
LOGGER_LEVEL_CHOICES = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
LOGGER_LEVEL_HELP = ("The logging level threshold, choices=['debug', 'info',"
                     " 'warning', 'error', 'critical'], default='info'")
# Default param for RotatingFileHandler
# maxBytes. 50MB by default.
LOGGING_ROTATE_BYTES = 50 * 1000 * 1000
# The default will grow logs up until 500MB without log loss.
LOGGING_ROTATE_BACKUP_COUNT = 10  # backupCount

# Constants used to define the different process types.
PROCESS_TYPE_REAPER = "reaper"
PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_RAY_CLIENT_SERVER = "ray_client_server"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
# TODO(sang): Delete it.
PROCESS_TYPE_REPORTER = "reporter"
PROCESS_TYPE_DASHBOARD = "dashboard"
PROCESS_TYPE_DASHBOARD_AGENT = "dashboard_agent"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"
PROCESS_TYPE_GCS_SERVER = "gcs_server"
PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER = "python-core-driver"
PROCESS_TYPE_PYTHON_CORE_WORKER = "python-core-worker"

# Log file names
MONITOR_LOG_FILE_NAME = f"{PROCESS_TYPE_MONITOR}.log"
LOG_MONITOR_LOG_FILE_NAME = f"{PROCESS_TYPE_LOG_MONITOR}.log"

WORKER_PROCESS_TYPE_IDLE_WORKER = "ray::IDLE"
WORKER_PROCESS_TYPE_SPILL_WORKER_NAME = "SpillWorker"
WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME = "RestoreWorker"
WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE = (
    f"ray::IDLE_{WORKER_PROCESS_TYPE_SPILL_WORKER_NAME}")
WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE = (
    f"ray::IDLE_{WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME}")
WORKER_PROCESS_TYPE_SPILL_WORKER = (
    f"ray::SPILL_{WORKER_PROCESS_TYPE_SPILL_WORKER_NAME}")
WORKER_PROCESS_TYPE_RESTORE_WORKER = (
    f"ray::RESTORE_{WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME}")
WORKER_PROCESS_TYPE_SPILL_WORKER_DELETE = (
    f"ray::DELETE_{WORKER_PROCESS_TYPE_SPILL_WORKER_NAME}")
WORKER_PROCESS_TYPE_RESTORE_WORKER_DELETE = (
    f"ray::DELETE_{WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME}")

LOG_MONITOR_MAX_OPEN_FILES = 200

# The object metadata field uses the following format: It is a comma
# separated list of fields. The first field is mandatory and is the
# type of the object (see types below) or an integer, which is interpreted
# as an error value. The second part is optional and if present has the
# form DEBUG:<breakpoint_id>, it is used for implementing the debugger.

# A constant used as object metadata to indicate the object is cross language.
OBJECT_METADATA_TYPE_CROSS_LANGUAGE = b"XLANG"
# A constant used as object metadata to indicate the object is python specific.
OBJECT_METADATA_TYPE_PYTHON = b"PYTHON"
# A constant used as object metadata to indicate the object is raw bytes.
OBJECT_METADATA_TYPE_RAW = b"RAW"

# A constant used as object metadata to indicate the object is an actor handle.
# This value should be synchronized with the Java definition in
# ObjectSerializer.java
# TODO(fyrestone): Serialize the ActorHandle via the custom type feature
# of XLANG.
OBJECT_METADATA_TYPE_ACTOR_HANDLE = b"ACTOR_HANDLE"

# A constant indicating the debugging part of the metadata (see above).
OBJECT_METADATA_DEBUG_PREFIX = b"DEBUG:"

AUTOSCALER_RESOURCE_REQUEST_CHANNEL = b"autoscaler_resource_request"

# The default password to prevent redis port scanning attack.
# Hex for ray.
REDIS_DEFAULT_PASSWORD = "5241590000000000"

# The default module path to a Python function that sets up the worker env.
DEFAULT_WORKER_SETUP_HOOK = "ray.workers.setup_runtime_env.setup"

# The default ip address to bind to.
NODE_DEFAULT_IP = "127.0.0.1"

# The Mach kernel page size in bytes.
MACH_PAGE_SIZE_BYTES = 4096

# Max 64 bit integer value, which is needed to ensure against overflow
# in C++ when passing integer values cross-language.
MAX_INT64_VALUE = 9223372036854775807

# Object Spilling related constants
DEFAULT_OBJECT_PREFIX = "ray_spilled_objects"

GCS_PORT_ENVIRONMENT_VARIABLE = "RAY_GCS_SERVER_PORT"

HEALTHCHECK_EXPIRATION_S = os.environ.get("RAY_HEALTHCHECK_EXPIRATION_S", 10)

# Filename of "shim process" that sets up Python worker environment.
# Should be kept in sync with kSetupWorkerFilename in
# src/ray/common/constants.h.
SETUP_WORKER_FILENAME = "setup_worker.py"

# Process failover constants arguments
RESTART_MIN_TIME_SECONDS = 3
RESTART_MAX_TIMES_WITHIN_MIN_SECONDS_BEFORE_DEAD = 6

# Job Data Key
JOB_DATA_KEY_RESULT = b"RESULT"

# The ray directory in worker container.
RAY_CONTAINER_MOUNT_DIR = "/home/admin/ray/python/"

# head high-availability feature
HEAD_NODE_LEADER_ELECTION_KEY = "head_node_leader_election_key"
HEAD_ROLE_ACTIVE = "active_head"
HEAD_ROLE_STANDBY = "standby_head"
GCS_ADDRESS_KEY = "GcsServerAddress"
