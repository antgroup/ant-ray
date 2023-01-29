import os
import sys
import zipfile

from ray.ray_constants import env_integer, env_bool
from ray.new_dashboard.utils import LazySemaphore

# Default Timeout
INITIALIZE_ENV_TIMEOUT_SECONDS = env_integer("INITIALIZE_ENV_TIMEOUT_SECONDS",
                                             60 * 10)  # default 10 minutes
INITIALIZE_ENV_TIMEOUT_SECONDS_LIMIT = 60 * 60  # 1 hour
RETRY_GET_ALL_JOB_INFO_INTERVAL_SECONDS = 2
# Job agent consts
JOB_RETRY_INTERVAL_SECONDS = 5
JOB_RETRY_TIMES_FOR_DRIVER_NODE = 3
JOB_RETRY_TIMES_FOR_WORKER_NODE = sys.maxsize
JOB_PREPARE_ENVIRONMENT_SEMAPHORE = LazySemaphore(
    max(env_integer("JOB_ENVIRONMENT_PREPARATION_CONCURRENCY", 3), 1))
JOB_ROOT_DIR = "{temp_dir}/job"
JOB_DIR = os.path.join(JOB_ROOT_DIR, "{job_id}")
JOB_UNPACK_DIR = os.path.join(JOB_DIR, "package")
JOB_DRIVER_ENTRY_FILE = os.path.join(JOB_DIR, "driver-{uuid}.py")
JOB_MARK_ENVIRON_READY_FILE = os.path.join(JOB_DIR, ".environ")
JOB_STATS_REPORT_TIMEOUT_SECONDS = 10
JOB_STATS_REPORT_INTERVAL_SECONDS = 2
JOB_DATA_DIR_BASE_NAME = "data"
JOB_DATA_DIR_BASE_TEMPLATE = os.path.join(JOB_DIR, JOB_DATA_DIR_BASE_NAME)
JOB_DATA_DIR_BASE_ENV_KEY = "RAY_JOB_DATA_DIR_BASE"
JOB_DATA_DIR_CLEAN_INTERVAL = 10
# Downloader constants
DOWNLOAD_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_PACKAGE_FILE = os.path.join(JOB_DIR, "package.zip")
# Download manager constants
DOWNLOAD_MANAGER_TASK_CANCEL_RETRY_INTERVAL_SECONDS = 1
DOWNLOAD_MANAGER_DOWNLOAD_TEMP_DIR = \
    "{temp_dir}/download_manager/download_manager-{datetime}"
DOWNLOAD_MANAGER_SEMAPHORE = LazySemaphore(
    max(env_integer("DOWNLOAD_CONCURRENCY", 15), 1))
# Python package constants
PYTHON_PACKAGE_INDEX = os.environ.get("PYTHON_PACKAGE_INDEX",
                                      "https://pypi.antfin-inc.com/simple/")
PYTHON_VIRTUALENV_DIR = os.path.join(JOB_DIR, "pyenv")
PYTHON_VIRTUALENV_APP_DATA_DIR = os.path.join(JOB_DIR, "virtualenv_app_data")
PYTHON_VIRTUALENV_PIP_TMP_DIR = os.path.join(JOB_DIR, "virtualenv_pip_tmp")
PYTHON_WHEELS_DIR = os.path.join(JOB_DIR, "wheels")
PYTHON_SHARED_WHEELS_DIR = "{temp_dir}/shared_wheels"
PYTHON_SHARED_NUMBA_CACHE_DIR = "{temp_dir}/shared_numba_caches"
PYTHON_REQUIREMENTS_FILE = os.path.join(JOB_DIR, "requirements.txt")
# Java package constants
JAVA_SHARED_LIBRARY_DIR = "{temp_dir}/shared_java_lib"
# Cpp package constants
CPP_SHARED_LIBRARY_DIR = "{temp_dir}/shared_cpp_lib"
# Archive constants
ARCHIVE_DIR = os.path.join(JOB_DIR, "archives")
ARCHIVE_SHARED_DIR = "{temp_dir}/shared_archives"
ARCHIVE_SHARED_UNZIP_DIR = os.path.join(ARCHIVE_SHARED_DIR,
                                        "unpack_{pack_filename}_{pack_hash}")
# Redis key
REDIS_KEY_JOB_COUNTER = "JobCounter"
JOB_CHANNEL = "JOB"
JOB_QUOTA_CHANNEL = f"{JOB_CHANNEL}_QUOTA"
# Downloader
AIOHTTP_DOWNLOADER = "aiohttp"
WGET_DOWNLOADER = "wget"
DRAGONFLY_DOWNLOADER = "dfget"

DEFAULT_DOWNLOADER = os.environ.get("DEFAULT_DOWNLOADER", WGET_DOWNLOADER)
if DEFAULT_DOWNLOADER not in [
        AIOHTTP_DOWNLOADER, WGET_DOWNLOADER, DRAGONFLY_DOWNLOADER
]:
    raise RuntimeError(
        "Expected to get DEFAULT_DOWNLOADER in"
        f" {[AIOHTTP_DOWNLOADER, WGET_DOWNLOADER, DRAGONFLY_DOWNLOADER]}, "
        f"but DEFAULT_DOWNLOADER is: {DEFAULT_DOWNLOADER}")

# Unpacker
SHUTIL_UNPACKER = "shutil"
UNZIP_UNPACKER = "unzip"
# Virualenv Cache Consts
GET_ETAG_TIMEOUT_SECONDS = env_integer("GET_ETAG_TIMEOUT_SECONDS", 5)
PYTHON_VIRTUALENV_CACHE_DIR = "{temp_dir}/shared_python_venv"
PYTHON_VIRTUALENV_CACHE_FREE_COUNT = 10
PYTHON_VIRTUALENV_META_DIR = "{virtualenv_dir}/.meta"
PYTHON_VIRTUALENV_HASH = os.path.join(PYTHON_VIRTUALENV_META_DIR, "hash")
PYTHON_VIRTUALENV_JOBS_DIR = os.path.join(PYTHON_VIRTUALENV_META_DIR, "jobs")
# The last number of Subprocess stdout/error lines, -1 means all.
LAST_N_LINS_OF_STDOUT = env_integer("LAST_N_LINS_OF_STDOUT", 10)
LAST_N_LINS_OF_STDERR = env_integer("LAST_N_LINS_OF_STDERR", 10)
# We need retry pip install wheel with no cache when we got those exception.
EXCEPTION_NEED_PIP_INSTALL_WITH_ON_CACHE = [
    rf"(.*){zipfile.BadZipFile.__name__}(.*)",
    rf"(.*){EOFError.__name__}(.*)",
    rf"(.*){OSError.__name__}(.*)",
    # If pip >= 22, It will raise InvalidWheel when got bad wheel package.
    # details in here:
    # https://github.com/pypa/pip/blob/22.0.3/src/pip/_internal/exceptions.py#L352
    r"(.*)Wheel (.*) located at (.*) is invalid\.(.*)"
]
# We need retry pip install wheel with no cache when we retry this times.
PIP_INSTALL_WITH_NO_CACHE_RETRY_THRESHOLD = 5
DEFAULT_INACTIVE_JOBS_CAPACITY = "100"
DEFAULT_DEAD_NODES_CAPACITY = "50"

# the archive minimum retention time.
ARCHIVE_MIN_RETRNTION_SECONDS = env_integer(
    "RAY_ARCHIVE_MIN_RETRNTION_SECONDS", 600)
ARCHIVE_CLEAR_INTERVAL_SECONDS = env_integer(
    "RAY_ARCHIVE_CLEAR_INTERVAL_SECONDS", 600)
ENABLE_AUTO_CLEAN_ARCHIVE = env_bool("ENABLE_AUTO_CLEAN_ARCHIVE", False)
