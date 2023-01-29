import asyncio
import json
import logging
import os
import os.path
import shutil
import subprocess
import sys
import uuid
import time
import shlex
import pathlib
import psutil
import contextlib
import traceback
import itertools
import collections
import re
from abc import abstractmethod
import stat
from urllib.parse import urlparse, unquote, ParseResult
from typing import NamedTuple, Union
import hashlib
from functools import partial
from zipfile import ZipFile
from uuid import UUID

import async_timeout
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.modules.job import job_consts, job_utils
from ray.new_dashboard.modules.job.virtualenv_cache import VenvCache
from ray.new_dashboard.utils import create_task
from ray.core.generated import job_agent_pb2
from ray.core.generated import job_agent_pb2_grpc
from ray.core.generated import agent_manager_pb2
from ray._private.services import RAY_HOME, get_ray_jars_dirs
from ray._private.utils import (
    hex_to_binary, binary_to_hex, try_to_create_directory,
    get_ray_site_packages_path, get_specify_python_package)
from ray.ray_constants import env_bool, LOGGER_LEVEL, LOGGER_LEVEL_CHOICES, \
    RAY_CONTAINER_MOUNT_DIR
from ray.new_dashboard.modules.job.system_job_utils import (
    load_cluster_config_from_url, )

logger = logging.getLogger(__name__)

DEFAULT_GC_OPTIONS = [
    "-XX:+UseCMSInitiatingOccupancyOnly",
    "-XX:CMSInitiatingOccupancyFraction=68",
    "-XX:+UseConcMarkSweepGC",
    "-XX:+UseParNewGC",
    "-XX:CMSFullGCsBeforeCompaction=5",
    "-XX:+UseCMSCompactAtFullCollection",
    "-XX:+PrintGCDateStamps",
    # Reference counting requires System.gc()
    # "-XX:+DisableExplicitGC",
]

COMMON_JVM_OPTIONS = [
    # Use `os.environ.get` to make the the job agent work even if
    # the environment var does not exist. e.g. CI test.
    f"-XX:ErrorFile={os.environ.get('RAY_LOG_DIR')}/hs_err_pid%p.log",
    "-XX:+CrashOnOutOfMemoryError",
    "-ea",
    "-server",
    "-verbose:gc",
    "-XX:-OmitStackTraceInFastThrow",
]


def _select_gc_options(jdk_version):
    if jdk_version == "jdk8":
        COMMON_JVM_OPTIONS.extend([
            "-XX:+PrintGCDetails",
            f"-Xloggc:{os.environ.get('RAY_LOG_DIR')}/jvm_gc_%p.log"
        ])
    else:
        COMMON_JVM_OPTIONS.extend([
            "-Xlog:gc*=info:stdout:time",
            f"-Xlog:gc:{os.environ.get('RAY_LOG_DIR')}/jvm_gc_%p.log:time"
        ])


class JvmOptionsHelper:
    def __init__(self):
        self.__first_time_to_get = True
        self.__gc_options_cache = None

    # Due to incompatible versions between jdk8 and jdk11 for gc, we set
    # different gc for different jdk by the ray_jvm_gc_options which is
    # set when cluster is built. We can use "jdk_version=jdk11" and the
    # default is "jdk8".

    async def __fetch_and_init_gc_options(self):
        self.__first_time_to_get = False
        cluster_config_url = os.environ.get("ray_config_file")
        if cluster_config_url is None:
            self.__gc_options_cache = []
            return
        cluster_conf = await load_cluster_config_from_url(cluster_config_url)
        if "ray_jvm_gc_options" in cluster_conf:
            ray_cluster_gc_options_str = cluster_conf["ray_jvm_gc_options"]
            ray_cluster_jdk_version = cluster_conf["jdk_version"]
            # Now, we only support jdk11 and jdk8 for cluster config.
            # If we set jdk_version, and jdk_version isn't "jdk8",
            # we will consume that it is jdk11.
            if len(ray_cluster_jdk_version) != 0:
                _select_gc_options(ray_cluster_jdk_version.lower())
            else:
                _select_gc_options("jdk8")
            if ray_cluster_gc_options_str is not None:
                self.__gc_options_cache = ray_cluster_gc_options_str.split(",")

    async def _get_cluster_default_jvm_options(self):
        if self.__first_time_to_get:
            await self.__fetch_and_init_gc_options()
        if self.__gc_options_cache is None or len(
                self.__gc_options_cache) == 0:
            return COMMON_JVM_OPTIONS + DEFAULT_GC_OPTIONS
        else:
            return COMMON_JVM_OPTIONS + self.__gc_options_cache


class JobFatalError(Exception):
    pass


class JobInfo:
    def __init__(self, job_id, job_info, temp_dir, log_dir):
        self._job_id = job_id
        self._job_info = job_info
        self._temp_dir = temp_dir
        self._log_dir = log_dir
        self._driver = None
        self._initialize_task = None
        self._job_is_stopped = False

    def job_is_stopped(self):
        return self._job_is_stopped

    def stop_job(self):
        self._job_is_stopped = True

    def temp_dir(self):
        return self._temp_dir

    def log_dir(self):
        return self._log_dir

    def language(self):
        return self._job_info["language"]

    def supported_languages(self):
        return self._job_info.get("supportedLanguages",
                                  ["PYTHON", "JAVA", "CPP"])

    def url(self):
        return self._job_info["url"]

    def job_id(self):
        return self._job_id

    def driver_entry(self):
        return self._job_info["driverEntry"]

    def driver_args(self):
        driver_args = self._job_info["driverArgs"]
        assert isinstance(driver_args, list)
        # TODO(fyrestone): Remove this after antc implement ray features.
        if self.language() == "JAVA":
            return [arg.strip("'") for arg in driver_args]
        else:
            return driver_args

    def initialize_env_timeout_seconds(self):
        timeout = self._job_info.get("initializeEnvTimeoutSeconds",
                                     job_consts.INITIALIZE_ENV_TIMEOUT_SECONDS)
        timeout = min(timeout, job_consts.INITIALIZE_ENV_TIMEOUT_SECONDS_LIMIT)
        timeout = max(timeout, 1)
        return timeout

    def enable_virtualenv_cache(self):
        default_enable_virtualenv_cache = env_bool(
            "DEFAULT_ENABLE_VIRTUALENV_CACHE", True)
        return self._job_info.get("virtualenvCache",
                                  default_enable_virtualenv_cache)

    def downloader(self):
        downloader = self._job_info.get("downloader",
                                        job_consts.DEFAULT_DOWNLOADER)
        assert downloader in [
            job_consts.WGET_DOWNLOADER,
            job_consts.AIOHTTP_DOWNLOADER,
            job_consts.DRAGONFLY_DOWNLOADER,
        ]
        return downloader

    def unpacker(self):
        unpacker = self._job_info.get("unpacker", job_consts.SHUTIL_UNPACKER)
        assert unpacker in [
            job_consts.UNZIP_UNPACKER, job_consts.SHUTIL_UNPACKER
        ]
        return unpacker

    def is_pip_check(self):
        return self._job_info.get("pipCheck", True)

    def is_pip_no_deps(self):
        return self._job_info.get("pipNoDeps", False)

    def is_pip_no_cache(self):
        return self._job_info.get("pipNoCache", None)

    def set_pip_no_cache(self, value):
        if value is True or value is False:
            self._job_info["pipNoCache"] = value
        else:
            raise ValueError(f"Expected True or False, but got value: {value}")

    def pip_version(self):
        return self._job_info.get("pipVersion")

    def pypi_url(self):
        return self._job_info.get("pypiUrl")

    def java_dependency_list(self):
        dependencies = self._job_info.get("dependencies", {}).get("java", [])
        if not dependencies:
            return []
        return [dashboard_utils.Bunch(d) for d in dependencies]

    def python_requirement_list(self):
        return self._job_info.get("dependencies", {}).get("python", [])

    def archive_list(self):
        return self._job_info.get("dependencies", {}).get("archive", [])

    def cpp_dependency_list(self):
        dependencies = self._job_info.get("dependencies", {}).get("cpp", [])
        if not dependencies:
            return []
        return [dashboard_utils.Bunch(d) for d in dependencies]

    def runtime_env(self):
        return self._job_info.get("runtimeEnv", {})

    def container_runtime_env(self):
        return self._job_info.get("runtimeEnv", {}).get("container", {})

    def set_driver(self, driver):
        self._driver = driver

    def driver(self):
        return self._driver

    def set_initialize_task(self, task):
        self._initialize_task = task

    def initialize_task(self):
        return self._initialize_task

    def env(self):
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=self._temp_dir, job_id=self._job_id)
        env_dict = {
            "RAY_JOB_DIR": job_unpack_dir,
            "NUMBA_CACHE_DIR": job_consts.PYTHON_SHARED_NUMBA_CACHE_DIR.format(
                temp_dir=self._temp_dir),
            # Used to identify the driver process of the current job to
            # prevent the wrong process(with the same PID) from being killed
            # in the end.
            "RAY_JOB_ID_FOR_DRIVER": self.job_id(),
        }
        eval_vars = {
            "${RAY_JOB_DIR}": job_unpack_dir,
            "${LD_LIBRARY_PATH}": os.environ.get("LD_LIBRARY_PATH", ""),
            "${LD_PRELOAD}": os.environ.get("LD_PRELOAD", ""),
            "${PATH}": os.environ.get("PATH", ""),
        }

        def _eval_var_in_str(s):
            for ev_k, ev_v in eval_vars.items():
                s = s.replace(ev_k, ev_v)
            return s

        # Support json values for env.
        for k, v in self._job_info.get("env", {}).items():
            if isinstance(v, str):
                env_dict[k] = _eval_var_in_str(v)
            else:
                env_dict[k] = json.dumps(v)
        return env_dict

    def setenv(self, key, value):
        if "env" in self._job_info:
            env = self._job_info.get("env")
            env[key] = value
        else:
            env = dict()
            env[key] = value
            self._job_info["env"] = env

    def num_java_workers_per_process(self):
        return self._job_info.get("numJavaWorkersPerProcess")

    def java_worker_process_default_memory_mb(self):
        return self._job_info.get("javaWorkerProcessDefaultMemoryMb")

    def num_initial_java_worker_processes(self):
        return self._job_info.get("numInitialJavaWorkerProcesses")

    def total_memory_mb(self):
        return self._job_info.get("totalMemoryMb")

    def max_total_memory_mb(self):
        return self._job_info.get("maxTotalMemoryMb")

    def java_heap_fraction(self):
        return self._job_info.get("javaHeapFraction")

    def jvm_options(self):
        options = self._job_info.get("jvmOptions", [])
        assert isinstance(options, list)
        return options

    def long_running(self):
        return self._job_info.get("longRunning")

    def logging_level(self):
        logging_level = self._job_info.get("loggingLevel")
        if logging_level is not None:
            logging_level = logging_level.upper()
            if logging_level not in LOGGER_LEVEL_CHOICES:
                raise ValueError(
                    f"The loggingLevel {logging_level} is invalid, "
                    f"available logging levels {LOGGER_LEVEL_CHOICES}")
        return logging_level

    def mark_environ_ready(self):
        mark_file = job_consts.JOB_MARK_ENVIRON_READY_FILE.format(
            temp_dir=self._temp_dir, job_id=self._job_id)
        pathlib.Path(mark_file).touch()

    def is_environ_ready(self):
        mark_file = job_consts.JOB_MARK_ENVIRON_READY_FILE.format(
            temp_dir=self._temp_dir, job_id=self._job_id)
        return pathlib.Path(mark_file).exists()

    def actor_task_back_pressure_enabled(self):
        return self._job_info.get("actorTaskBackPressureEnabled")

    def max_pending_calls(self):
        return self._job_info.get("maxPendingCalls")

    def namespace(self):
        return self._job_info.get("namespace", None)


class StatsCollector:
    # Python 3.7+ has dataclass, use simple class for compatibility.
    class Stats:
        start_time = 0
        end_time = 0
        duration = 0

    _stats = collections.defaultdict(collections.OrderedDict)

    @classmethod
    def stats_context(cls, job_id, name):
        stats = cls._stats

        class _StatsObject:
            def __enter__(self):
                stats_item = StatsCollector.Stats()
                stats_item.start_time = time.time()
                stats[job_id][name] = stats_item

            def __exit__(self, exc_type, exc_val, exc_tb):
                stats_item = stats[job_id][name]
                stats_item.end_time = time.time()
                stats_item.duration = \
                    stats_item.end_time - stats_item.start_time

        return _StatsObject()

    @classmethod
    def clear(cls, job_id):
        return cls._stats.pop(job_id, {})

    @classmethod
    def get(cls, job_id, auto_end_time=True):
        job_stats = cls._stats.get(job_id, {})
        if auto_end_time:
            results = []
            end_time = time.time()
            for name, stats in job_stats.items():
                if stats.start_time and stats.end_time:
                    results.append(f"{name}: {stats.duration:.2f}s")
                else:
                    results.append(
                        f"{name}: {end_time - stats.start_time:.2f}s")
            return "\n".join(results)
        else:
            return "\n".join([
                f"{name}: {stats.duration:.2f}s"
                for name, stats in job_stats.items()
            ])


class CommandRunner:
    _cmd_index_gen = itertools.count(1)

    @contextlib.contextmanager
    def _running_context(self, proc, logger_prefix):
        try:
            yield
        finally:
            self._kill_process(proc)
            create_task(self._wait_process(proc))
            logger.info(
                "%sClean cmd[%s]",
                logger_prefix,
                proc.cmd_index,
            )

    @staticmethod
    def _kill_process(process):
        try:
            process.kill()
        except ProcessLookupError:
            pass

    @staticmethod
    async def _wait_process(process):
        await process.wait()
        assert process.returncode is not None

    async def _check_output_cmd(self, cmd, *, logger_prefix=None, **kwargs):
        """Run command with arguments and return its output.

        If the return code was non-zero it raises a CalledProcessError. The
        CalledProcessError object will have the return code in the returncode
        attribute and any output in the output attribute.

        Args:
            cmd: The cmdline should be a sequence of program arguments or else
                a single string or path-like object. The program to execute is
                the first item in cmd.
            logger_prefix: The logger prefix string.
            kwargs: All arguments are passed to the create_subprocess_exec.

        Returns:
            The stdout of cmd.

        Raises:
            CalledProcessError: If the return code of cmd is not 0.
        """
        logger_prefix = dashboard_utils.logger_prefix_str(logger_prefix)

        cmd_index = next(self._cmd_index_gen)

        logger.info("%sRun cmd[%s] %s", logger_prefix, cmd_index, repr(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwargs)
        proc.cmd_index = cmd_index

        # TODO: In Python 3.7+ we can import `asynccontextmanager` from
        # `contextlib`, So, use context manage `proc` when we drop suport
        # python3.6, create in `__aenter__` and kill in `__aexit__`
        with self._running_context(proc, logger_prefix):
            stdout, stderr = await proc.communicate()

        stdout = stdout.decode("utf-8")
        if stdout:
            logger.info("%sOutput of cmd[%s]: %s", logger_prefix, cmd_index,
                        stdout)
        else:
            logger.info("%sNo output for cmd[%s]", logger_prefix, cmd_index)
        if proc.returncode != 0:
            stderr = stderr.decode("utf-8")
            logger.error("%sOutput of cmd[%s]: %s", logger_prefix, cmd_index,
                         stderr)
            exception = dashboard_utils.SubprocessCalledProcessError(
                proc.returncode, cmd, output=stdout, stderr=stderr)
            exception.last_n_lines_of_stdout = job_consts.LAST_N_LINS_OF_STDOUT
            exception.last_n_lines_of_stderr = job_consts.LAST_N_LINS_OF_STDERR
            raise exception
        return stdout


class DownloadManager(CommandRunner):

    DownloadTask = NamedTuple("DownloadTask", [
        ("index", int),
        ("task", asyncio.Task),
        ("targets", set),
        ("url", str),
        ("temp_filename", str),
    ])

    _download_index_gen = itertools.count(1)

    def __init__(self, temp_dir):
        super().__init__()
        # Store `DownloadTask`
        self._tasks_store = dict()
        self._temp_dir = \
            job_consts.DOWNLOAD_MANAGER_DOWNLOAD_TEMP_DIR.format(
                temp_dir=temp_dir,
                datetime=time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()))
        try_to_create_directory(self._temp_dir)

    async def download(self,
                       downloader,
                       http_session,
                       url,
                       filename,
                       job_id=None,
                       trace_index=None):
        download_task = self._create_download_task(
            downloader,
            http_session,
            url,
            filename,
            job_id=job_id,
            trace_index=trace_index)
        try:
            await download_task.task
        except Exception as ex:
            # Avoid use download_task.exception(), otherwise,
            # out of task(create from `JobProcessor._download_package`)
            # will get `asyncio.CancelledError`
            raise RuntimeError(
                f"Download failed, downloader: {downloader}, url: {url}, "
                f"filename: {download_task.temp_filename}") from ex

    def cancel(self, target_filename, url):
        download_task = self._tasks_store.get(url, None)
        if download_task is None:
            return
        try:
            download_task.targets.remove(target_filename)
        except KeyError:
            # Just ignore KeyError, cause we maybe cancel the download task
            # before `_create_download_task` been called.
            pass

        if len(download_task.targets) == 0:
            download_task.task.cancel()

    @staticmethod
    def _get_url_hash(url):
        return hashlib.blake2b(url.encode("utf-8"), digest_size=20).hexdigest()

    def _create_download_task(self,
                              downloader,
                              http_session,
                              url,
                              target_filename,
                              job_id=None,
                              trace_index=None):
        # In `DownloadManager`, url can be used as the only key for target
        # downloaded file

        task_hash_key = self._get_url_hash(url)
        temp_filename = os.path.join(self._temp_dir, task_hash_key)

        old_download_task = self._tasks_store.get(url, None)
        if old_download_task:
            old_download_task.targets.add(target_filename)
            logger.info(
                "[%s][%s] Share download[%s] for job %s "
                "with %s targets: %s to %s.",
                type(self).__name__, old_download_task.index, trace_index,
                job_id, len(old_download_task.targets), url, temp_filename)
            self._tasks_store[url] = old_download_task
            return old_download_task

        download_index = next(self._download_index_gen)
        logger.info("[%s][%s] New download[%s] for job %s: %s to %s.",
                    type(self).__name__, download_index, trace_index, job_id,
                    url, temp_filename)

        if downloader == job_consts.AIOHTTP_DOWNLOADER:
            task = create_task(
                self._download_by_http_request(http_session, url,
                                               temp_filename))
        elif downloader == job_consts.WGET_DOWNLOADER:
            wget_cmd = ["wget", "-O", temp_filename, url]
            task = create_task(
                self._check_output_cmd(
                    wget_cmd,
                    logger_prefix=(type(self).__name__, download_index)))
        elif downloader == job_consts.DRAGONFLY_DOWNLOADER:
            dfget_cmd = [
                "/home/staragent/plugins/dragonfly/dfget",
                "-u",
                url,
                "-o",
                temp_filename,
            ]
            task = create_task(
                self._check_output_cmd(
                    dfget_cmd,
                    logger_prefix=(type(self).__name__, download_index)))
        else:
            raise Exception(f"Unsupported downloader {downloader}")

        task.add_done_callback(partial(self._download_post_process, url))
        new_download_task = self.DownloadTask(
            index=download_index,
            task=task,
            targets={target_filename},
            url=url,
            temp_filename=temp_filename,
        )
        self._tasks_store[url] = new_download_task

        return new_download_task

    def _download_post_process(self, url, raw_task):
        download_task = self._tasks_store.pop(url)

        temp_filename = download_task.temp_filename
        try:
            raw_task.result()
        except asyncio.CancelledError:
            # It means task has been cancelled
            logger.info(
                "[%s][%s] Download task has been canceled, "
                "url: %s, temporary filename: %s",
                type(self).__name__, download_task.index, url, temp_filename)
        except Exception:
            # It means some exceptions were acquired when the download task
            # was running and we just ignore it.
            pass
        else:
            # Hard link to all targets
            for target_filename in download_task.targets:
                dashboard_utils.force_hardlink(
                    temp_filename,
                    target_filename,
                    logger_prefix=(type(self).__name__, download_task.index))
        finally:
            # TODO: Handle temporary download file leaky.
            # If we kill download process (wget command) when downloading,
            # may leave a temporary file whose name we don’t know.
            logger.info("[%s][%s] Remove %s",
                        type(self).__name__, download_task.index,
                        temp_filename)
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

    async def _download_by_http_request(self, http_session, url,
                                        temp_filename):
        async with http_session.get(
                url,
                ssl=False,
                timeout=job_consts.INITIALIZE_ENV_TIMEOUT_SECONDS_LIMIT
        ) as response:
            with open(temp_filename, "wb") as f:
                while True:
                    chunk = await response.content.read(
                        job_consts.DOWNLOAD_BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)


download_manager: Union[DownloadManager, None] = None


class JobProcessor(CommandRunner):
    def __init__(self, job_info):
        assert isinstance(job_info, JobInfo)
        self._job_info = job_info
        self._download_tasks = []

    async def clean(self):
        # Cancel all running download task, just remove current job's
        # filename from download manager's targets
        for task_info in self._download_tasks:
            download_manager.cancel(*task_info)
        self._download_tasks = []

    async def _download_package(self, http_session, url, filename):
        job_id = self._job_info.job_id()
        trace_index = next(self._cmd_index_gen)
        logger.info("[%s] Start download[%s] %s to %s", job_id, trace_index,
                    url, filename)
        downloader = self._job_info.downloader()

        self._download_tasks.append((filename, url))
        await download_manager.download(
            downloader,
            http_session,
            url,
            filename,
            job_id=job_id,
            trace_index=trace_index)

        logger.info("[%s] Finished download[%s] %s to %s", job_id, trace_index,
                    url, filename)

    async def _download_package_with_cache(self,
                                           http_session,
                                           download_dir,
                                           cache_dir,
                                           url,
                                           content_hash=None,
                                           async_validator=None,
                                           filename_extractor=None):
        job_id = self._job_info.job_id()
        parse_result = urlparse(unquote(url))
        if filename_extractor is None:
            filename = os.path.basename(parse_result.path)
        else:
            filename = filename_extractor(parse_result)
        if content_hash is None:
            # Auto generate content hash from http if async_validator exists.
            content_hash = await job_utils.get_url_content_hash(
                http_session, job_id, url) if async_validator else ""
        if content_hash:
            filename_with_hash = filename + "." + content_hash
        else:
            filename_with_hash = filename
        # The full path of file in cache dir.
        cache_filename = os.path.join(cache_dir, filename_with_hash)
        # The full path of file in download dir.
        download_filename = os.path.join(download_dir, filename)

        # Download to a unique filename, Avoid conflicts of files with
        # the same name.
        file_uuid = str(uuid.uuid4())
        download_filename_with_uuid = download_filename + "." + file_uuid
        cache_filename_with_uuid = cache_filename + "." + file_uuid
        # Download the file and cache to the cache dir as follw step.
        #     1. download file to the `download_filename_with_uuid`
        #     2. check file `download_filename_with_uuid`
        #     3. set `download_filename_with_uuid` as readonly.
        #     4. force link `download_filename_with_uuid` to `cache_filename`
        #     5. rename `download_filename_with_uuid` to `download_filename`
        if not os.path.exists(cache_filename):
            logger.info("[%s] Cache miss: %s", job_id, cache_filename)
            async with job_consts.DOWNLOAD_MANAGER_SEMAPHORE:
                await self._download_package(http_session, url,
                                             download_filename_with_uuid)
            if async_validator and content_hash:
                try:
                    download_ok = await async_validator(
                        download_filename_with_uuid, content_hash)
                    if not download_ok:
                        raise Exception(
                            f"[{job_id}] Validate download file failed: {url}")

                    dashboard_utils.file_readonly(
                        download_filename_with_uuid,
                        ignore_error=False,
                        logger_prefix=job_id)
                    # We first link `download_filename_with_uuid` to
                    # `cache_filename_with_uuid` than rename
                    # `cache_filename_with_uuid` to `cache_filename` cause
                    # the renaming will be an atomic operation
                    # (this is a POSIX requirement).
                    dashboard_utils.force_hardlink(
                        download_filename_with_uuid,
                        cache_filename_with_uuid,
                        logger_prefix=job_id)
                    os.rename(download_filename_with_uuid, download_filename)
                    os.rename(cache_filename_with_uuid, cache_filename)
                except Exception as error:
                    dashboard_utils.force_remove(
                        download_filename, ignore_error=True)
                    dashboard_utils.force_remove(
                        cache_filename, ignore_error=True)
                    logger.info(
                        "[%s] Got error when download file and cache it,"
                        " error: %s", job_id, error)
                    raise Exception(
                        f"[{job_id}] Got error when download file and"
                        f" cache it, error: {error}") from error
                finally:
                    dashboard_utils.force_remove(
                        cache_filename_with_uuid, ignore_error=True)
                    dashboard_utils.force_remove(
                        download_filename_with_uuid, ignore_error=True)

            else:
                os.rename(download_filename_with_uuid, download_filename)
                logger.info(
                    "[%s] Skip verifying and caching, "
                    "hash(%s) or validator(%s) of %s is empty.", job_id,
                    content_hash, async_validator, url)
        else:
            logger.info("[%s] Cache hit: %s", job_id, cache_filename)
            dashboard_utils.force_hardlink(
                cache_filename, download_filename, logger_prefix=job_id)
        return download_filename

    async def _unpack_package(self, filename, path):
        try:
            unpacker = self._job_info.unpacker()
            if unpacker == job_consts.SHUTIL_UNPACKER:
                code = f"import shutil; " \
                    f"shutil.unpack_archive({repr(filename)}, {repr(path)})"
                unzip_cmd = [self._get_current_python(), "-c", code]
            elif unpacker == job_consts.UNZIP_UNPACKER:
                unzip_cmd = ["unzip", "-o", "-d", path, filename]
            else:
                raise Exception(f"Unsupported unpacker {unpacker}")
            await self._check_output_cmd(unzip_cmd)
        except PermissionError as error:
            raise Exception(
                "Dependent file has some conflict, please"
                "check your dependencies, package, and archive") from error

    async def _check_output_cmd(self, cmd, *, logger_prefix=None, **kwargs):
        if logger_prefix is None:
            logger_prefix = self._job_info.job_id()
        return await super()._check_output_cmd(
            cmd, logger_prefix=logger_prefix, **kwargs)

    async def _start_driver(self, cmd, stdout, stderr, env):
        job_id = self._job_info.job_id()
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=self._job_info.temp_dir(), job_id=job_id)
        logger.info("[%s] Start driver cmd %s, cwd=%s", job_id, repr(cmd),
                    job_unpack_dir)
        self._add_so_path_to_ld_library_path_env(env, job_unpack_dir)
        cmd_str = subprocess.list2cmdline(cmd)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=stdout,
            stderr=stderr,
            env={
                **os.environ,
                **env,
                "CMDLINE": cmd_str,
                "RAY_JOB_DIR": job_unpack_dir,
            },
            cwd=job_unpack_dir,
        )
        proc.cmdline = cmd_str
        logger.info("[%s] Start driver cmd %s with pid %s, cwd=%s", job_id,
                    repr(cmd), proc.pid, job_unpack_dir)
        return proc

    @staticmethod
    def _get_current_python():
        return sys.executable

    @staticmethod
    def _get_virtualenv_python(virtualenv_path):
        return os.path.join(virtualenv_path, "bin/python")

    @staticmethod
    def _is_in_virtualenv():
        return (hasattr(sys, "real_prefix")
                or (hasattr(sys, "base_prefix")
                    and sys.base_prefix != sys.prefix))

    @staticmethod
    def _new_log_files(log_dir, filename):
        if log_dir is None:
            return None, None
        stdout = open(
            os.path.join(log_dir, filename + ".out"), "a", buffering=1)
        stderr = open(
            os.path.join(log_dir, filename + ".err"), "a", buffering=1)
        return stdout, stderr

    def _force_symlink(self, src, dst):
        logger.info("[%s] Link %s to %s", self._job_info.job_id(), src, dst)
        try:
            os.remove(dst)
        except Exception:
            pass
        os.symlink(src, dst)

    def _add_so_path_to_ld_library_path_env(self, env, unpack_dir):
        ld_library_path = env.get("LD_LIBRARY_PATH", "")
        ray_api_so_path = os.path.join(get_ray_site_packages_path(),
                                       "ray/cpp/lib")
        env["LD_LIBRARY_PATH"] = ld_library_path + ":" \
            + unpack_dir + ":" + ray_api_so_path

    @abstractmethod
    async def run(self):
        pass


class DownloadPackage(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        url = self._job_info.url()
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        filename = job_consts.DOWNLOAD_PACKAGE_FILE.format(
            temp_dir=temp_dir, job_id=job_id)
        unzip_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        with StatsCollector.stats_context(job_id, "DownloadPackage"):
            await self._download_package(self._http_session, url, filename)
        with StatsCollector.stats_context(job_id, "UnzipPackage"):
            await self._unpack_package(filename, unzip_dir)


_global_unpack_locks = collections.defaultdict(asyncio.Lock)


class ArchiveCacheClear:
    def __init__(self, temp_dir):
        self._temp_dir = temp_dir
        self._shared_archive_dir = pathlib.Path(
            job_consts.ARCHIVE_SHARED_DIR.format(temp_dir=temp_dir))

    @dashboard_utils.async_loop_forever(
        job_consts.ARCHIVE_CLEAR_INTERVAL_SECONDS)
    async def clean_archive_cache(self):
        """
        For an archive file the following files may be
        generated in directory `shared_archive_dir`:
            1. zipfile: Used for hard linking to other job directories.
            2. unqiue zipfile: 1 is a hard link of 2, this is to avoid
               the download destination conflict when the url is different
               but the file name and md5 are the same.
            3. unzip dir: Unzip from 1.
        For example:
            - shared_archives/
                - tritonserver_06092146_manual_4bff548c.zip.d392d7278f2269f63046fa9a03340662
                - tritonserver_06092146_manual_4bff548c.zip.d392d7278f2269f63046fa9a03340662.c4a600f9-734b-4a28-bbe1-0a5e3fa561a3
                - tritonserver_06092146_manual_4bff548c.zip.d392d7278f2269f63046fa9a03340662.69f3f4a5-c6bf-40af-9af6-fba8d1f72539
                - unpack_tritonserver_06092146_manual_4bff548c.zip_d392d7278f2269f63046fa9a03340662/
        """  # noqa: E501

        def _check_uuid_zip_file(zip_path):
            """ check if zip_path is unqiue zipfile.
            """
            zip_path_str = str(zip_path)
            uuid_str = zip_path_str.rsplit(".", 1)[-1]
            try:
                uuid_obj = UUID(uuid_str, version=4)
            except ValueError:
                return False
            return str(uuid_obj) == uuid_str

        zip_paths = collections.defaultdict(list)
        for data_name in os.listdir(self._shared_archive_dir):
            zip_path = self._shared_archive_dir / data_name
            if zip_path.is_file():
                zip_path_str = str(zip_path)
                if _check_uuid_zip_file(data_name):
                    zip_path_str, _ = zip_path_str.rsplit(".", 1)
                    zip_paths[zip_path_str].append(zip_path)
                else:
                    zip_paths[zip_path_str]

        for zipfile_path_str, uuid_zip_paths in zip_paths.items():
            zipfile_path = pathlib.Path(zipfile_path_str)
            if not zipfile_path.exists():
                continue
            if time.time() - os.stat(
                    zipfile_path
            ).st_mtime < job_consts.ARCHIVE_MIN_RETRNTION_SECONDS:
                continue
            for uuid_zip_path in uuid_zip_paths:
                dashboard_utils.force_remove(uuid_zip_path, ignore_error=True)
            if os.stat(zipfile_path).st_nlink != 1:
                continue

            filename, content_hash = zipfile_path.name.rsplit(".", 1)
            unzip_dir = job_consts.ARCHIVE_SHARED_UNZIP_DIR.format(
                temp_dir=self._temp_dir,
                pack_filename=filename,
                pack_hash=content_hash,
            )

            try:
                async with _global_unpack_locks[unzip_dir]:
                    dashboard_utils.force_remove(
                        zipfile_path, ignore_error=True)
                    shutil.rmtree(unzip_dir, ignore_errors=False)
            except Exception as ex:
                logger.error(
                    "[ArchiveCacheClear] failed to delete unpack dir %s"
                    ", error: %s", unzip_dir, str(ex))


class DownloadArchive(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        local_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        local_archive_dir = job_consts.ARCHIVE_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        os.makedirs(local_archive_dir, exist_ok=True)
        shared_archive_dir = job_consts.ARCHIVE_SHARED_DIR.format(
            temp_dir=temp_dir)
        os.makedirs(shared_archive_dir, exist_ok=True)
        http_session = self._http_session

        async def verify_downloaded(file, content_hash):
            # Currently, we only cache the valid zip archive.
            try:
                await self._check_output_cmd(["unzip", "-t", file])
            except Exception:
                return False
            else:
                return True

        def filename_extractor(parse_result: ParseResult):
            basename = os.path.basename(parse_result.path)
            name, ext = os.path.splitext(basename)
            return "".join([
                name, "_",
                hashlib.md5(
                    parse_result.geturl().encode("utf-8")).hexdigest()[:8], ext
            ])

        with StatsCollector.stats_context(job_id, "DownloadArchive"):
            downloaded_archives = await asyncio.gather(*[
                self._download_package_with_cache(
                    http_session=http_session,
                    download_dir=local_archive_dir,
                    cache_dir=shared_archive_dir,
                    url=url,
                    async_validator=verify_downloaded,
                    filename_extractor=filename_extractor)
                for url in self._job_info.archive_list()
            ])

        with StatsCollector.stats_context(job_id, "UnzipArchive"):
            for filename, url in zip(downloaded_archives,
                                     self._job_info.archive_list()):
                content_hash = await job_utils.get_url_content_hash(
                    http_session, job_id, url)
                if content_hash is None:
                    await self._unpack_package(filename, local_dir)
                    continue
                unzip_dir = job_consts.ARCHIVE_SHARED_UNZIP_DIR.format(
                    temp_dir=temp_dir,
                    pack_filename=os.path.basename(filename),
                    pack_hash=content_hash,
                )
                logger.info("[%s] Pending unpack %s to %s", job_id, filename,
                            unzip_dir)
                async with _global_unpack_locks[unzip_dir]:
                    if not os.path.exists(unzip_dir):
                        logger.info("[%s] Starting unpack %s to %s", job_id,
                                    filename, unzip_dir)
                        try:
                            os.makedirs(unzip_dir, exist_ok=True)
                            await self._unpack_package(filename, unzip_dir)
                        except Exception as ex:
                            shutil.rmtree(unzip_dir, ignore_errors=True)
                            raise ex
                    else:
                        logger.info("[%s] Hit Cache, skip [unpack %s to %s]",
                                    job_id, filename, unzip_dir)
                with ZipFile(filename, "r") as zip_file:
                    file_list = zip_file.filelist
                # Aviod block current loop
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, job_utils.hardlink_all_zip_files, unzip_dir,
                    local_dir, file_list, job_id)


class PreparePythonEnviron(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session
        self._pip_version = None
        self._pip_env = self._get_pip_env()

    def _get_pip_env(self):
        # The pip may create too many files in /tmp, e.g. /tmp/pip-unpack-*,
        # /tmp/pip-req-tracker-*, /tmp/pip-install-*,
        # /tmp/pip-ephem-wheel-cache-*, ...
        # Please refer to: https://github.com/pypa/pip/issues/4462
        pip_tmp_dir = job_consts.PYTHON_VIRTUALENV_PIP_TMP_DIR.format(
            temp_dir=self._job_info.temp_dir(), job_id=self._job_info.job_id())
        return dict(os.environ, TMPDIR=pip_tmp_dir)

    async def _create_virtualenv(self, path, app_data):
        shutil.rmtree(path, ignore_errors=True)
        python = self._get_current_python()
        if self._is_in_virtualenv():
            python_dir = os.path.abspath(
                os.path.join(os.path.dirname(python), ".."))
            clonevirtualenv = os.path.join(
                os.path.dirname(__file__), "clonevirtualenv.py")
            create_venv_cmd = [python, clonevirtualenv, python_dir, path]
        else:
            create_venv_cmd = [
                python, "-m", "virtualenv", "--app-data", app_data,
                "--reset-app-data", "--no-periodic-update",
                "--system-site-packages", "--no-download", path
            ]
        job_id = self._job_info.job_id()
        with StatsCollector.stats_context(job_id, "CreateVirtualEnv"):
            await self._check_output_cmd(create_venv_cmd)

    def _pypi_arg(self):
        pypi_url = self._job_info.pypi_url()
        if pypi_url:
            return ["-i", pypi_url]
        elif job_consts.PYTHON_PACKAGE_INDEX:
            return ["-i", job_consts.PYTHON_PACKAGE_INDEX]
        return []

    async def _install_python_requirements(self, path):
        requirements = await self._localized_requirements()
        if not requirements:
            return

        job_id = self._job_info.job_id()
        await self._ensure_pip_version(path)
        python = self._get_virtualenv_python(path)
        pypi = self._pypi_arg()
        no_deps = ["--no-deps"] if self._job_info.is_pip_no_deps() else []
        no_cache = ["--no-cache-dir"
                    ] if self._job_info.is_pip_no_cache() else []
        pip_install_cmd = [
            python, "-m", "pip", "install", "--disable-pip-version-check"
        ] + pypi + no_deps + no_cache + requirements
        with StatsCollector.stats_context(job_id, "PipInstall"):
            await self._check_output_cmd(pip_install_cmd, env=self._pip_env)

    async def _ray_mark_internal(self, path):
        python = self._get_virtualenv_python(path)
        output = await self._check_output_cmd([
            python, "-c", "import ray; print(ray.__version__, ray.__path__[0])"
        ])
        # print after import ray may have [0m endings, so we strip them by *_
        ray_version, ray_path, *_ = [s.strip() for s in output.split()]
        pathlib.Path(os.path.join(ray_path, ".internal_ray")).touch()
        return ray_version, ray_path

    async def _check_ray_is_internal(self, path, ray_version, ray_path):
        python = self._get_virtualenv_python(path)
        job_id = self._job_info.job_id()
        with StatsCollector.stats_context(job_id, "CheckRay"):
            # print after import ray may have [0m endings,
            # so we strip them by *_
            output = await self._check_output_cmd([
                python, "-c",
                "import ray; print(ray.__version__, ray.__path__[0])"
            ])
        actual_ray_version, actual_ray_path, *_ = [
            s.strip() for s in output.split()
        ]
        is_exists = pathlib.Path(
            os.path.join(actual_ray_path, ".internal_ray")).exists()
        if not is_exists:
            raise JobFatalError("Change ray version is not allowed: \n"
                                f"  current version: {actual_ray_version}, "
                                f"current path: {actual_ray_path}\n"
                                f"  expect version: {ray_version}, "
                                f"expect path: {ray_path}")

    async def _ensure_pip_version(self, path):
        pip_version = self._job_info.pip_version()
        if not pip_version:
            return
        if self._pip_version == pip_version:
            return
        job_id = self._job_info.job_id()
        python = self._get_virtualenv_python(path)
        pypi = self._pypi_arg()
        # Ensure pip version.
        pip_reinstall_cmd = [
            python, "-m", "pip", "install", "--disable-pip-version-check",
            f"pip{self._job_info.pip_version()}"
        ] + pypi
        with StatsCollector.stats_context(job_id, "PipReinstall"):
            await self._check_output_cmd(pip_reinstall_cmd, env=self._pip_env)
        self._pip_version = pip_version

    async def _pip_check(self, path):
        job_id = self._job_info.job_id()
        if not self._job_info.is_pip_check():
            logger.info("[%s] Skip pip check on %s", job_id, path)
            return
        python = self._get_virtualenv_python(path)
        with StatsCollector.stats_context(job_id, "PipCheck"):
            output = await self._check_output_cmd(
                [python, "-m", "pip", "check", "--disable-pip-version-check"],
                env=self._pip_env)
        output = output.strip()
        if output and "no broken" not in output.lower():
            raise JobFatalError(f"pip check on {path} failed:\n{output}")
        else:
            logger.info("[%s] pip check on %s success.", job_id, path)

    async def _run_pip_cmd_requirements(self, path):
        requirements = [
            r for r in self._job_info.python_requirement_list()
            if job_utils.is_pip_cmd(r)
        ]
        if not requirements:
            return

        python = self._get_virtualenv_python(path)
        await self._ensure_pip_version(path)

        for cmd_str in requirements:
            cmd = shlex.split(cmd_str)
            if cmd[0] != "pip":
                raise JobFatalError(
                    f"Unexpected Python requirement: {cmd_str}")
            await self._check_output_cmd(
                [python, "-m"] + cmd, env=self._pip_env)

    async def _localized_requirements(self):
        requirements = [
            r for r in self._job_info.python_requirement_list()
            if not job_utils.is_pip_cmd(r)
        ]
        if not requirements:
            return requirements

        job_id = self._job_info.job_id()
        temp_dir = self._job_info.temp_dir()
        shared_wheels_dir = job_consts.PYTHON_SHARED_WHEELS_DIR.format(
            temp_dir=temp_dir)
        os.makedirs(shared_wheels_dir, exist_ok=True)
        local_wheels_dir = job_consts.PYTHON_WHEELS_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        os.makedirs(local_wheels_dir, exist_ok=True)

        async def verify_downloaded(file, content_hash):
            # The Python wheel is a zip archive, so `unzip -t` can be used to
            # test the integrity. If the url is not a wheel, the test will
            # fail, just skip caching the downloaded file.
            try:
                await self._check_output_cmd(["unzip", "-t", file])
            except Exception:
                return False
            else:
                return True

        url_list = [r for r in requirements if job_utils.is_url(r)]
        local_files = await asyncio.gather(*[
            self._download_package_with_cache(
                http_session=self._http_session,
                download_dir=local_wheels_dir,
                cache_dir=shared_wheels_dir,
                url=url,
                async_validator=verify_downloaded) for url in url_list
        ])

        url_to_local_file = dict(zip(url_list, local_files))
        logger.info("[%s] Url to local requirements: %s", job_id,
                    url_to_local_file)
        return [url_to_local_file.get(r) or r for r in requirements]

    async def _clean_pip_cache(self, path):
        """ Remove all items from the cache.
        """
        python = self._get_virtualenv_python(path)
        cmd = ["pip", "cache", "purge"]
        await self._check_output_cmd([python, "-m"] + cmd, env=self._pip_env)

    async def _new_virtualenv(self, virtualenv_path):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        try:
            # Create a clean directory for pip tmp.
            virtualenv_pip_tmp = \
                job_consts.PYTHON_VIRTUALENV_PIP_TMP_DIR.format(
                        temp_dir=temp_dir, job_id=job_id)
            shutil.rmtree(virtualenv_pip_tmp, ignore_errors=True)
            os.makedirs(virtualenv_pip_tmp, exist_ok=True)

            # Create the virtualenv app data dir per job to avoid cache data
            # broken.
            virtualenv_app_data = \
                job_consts.PYTHON_VIRTUALENV_APP_DATA_DIR.format(
                    temp_dir=temp_dir, job_id=job_id)
            await self._create_virtualenv(virtualenv_path, virtualenv_app_data)

            ray_version, ray_path = await self._ray_mark_internal(
                virtualenv_path)

            # Install requirements.
            await self._install_python_requirements(virtualenv_path)

            # Run pip cmd requirements.
            await self._run_pip_cmd_requirements(virtualenv_path)

            await self._check_ray_is_internal(virtualenv_path, ray_version,
                                              ray_path)
            await self._pip_check(virtualenv_path)
        except Exception as ex:
            try:
                for retry_pattern in \
                        job_consts.EXCEPTION_NEED_PIP_INSTALL_WITH_ON_CACHE:
                    if re.search(retry_pattern, str(ex)):
                        await self._clean_pip_cache(virtualenv_path)
                        break
            except Exception:
                # It will got Exception when virtualenv setup got failed,
                # we just ignore it.
                pass
            logger.info("[%s] Delete incomplete virtualenv: %s", job_id,
                        virtualenv_path)
            shutil.rmtree(virtualenv_path, ignore_errors=True)
            raise ex

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        virtualenv_path = job_consts.PYTHON_VIRTUALENV_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        with StatsCollector.stats_context(job_id, "PreparePythonEnviron"):
            if self._job_info.enable_virtualenv_cache():
                requirement_list = self._job_info.python_requirement_list()
                cache_dir = job_consts.PYTHON_VIRTUALENV_CACHE_DIR.format(
                    temp_dir=temp_dir)
                cached_virtualenv_path = await VenvCache.get_virtualenv(
                    cache_dir, job_id, requirement_list, self._new_virtualenv,
                    self._http_session)
                self._force_symlink(cached_virtualenv_path, virtualenv_path)
            else:
                await self._new_virtualenv(virtualenv_path)


class StartPythonDriver(JobProcessor):
    _template = """import sys
sys.path.append({import_path})
import ray
from ray._private.utils import hex_to_binary
ray.init(ignore_reinit_error=True,
         log_to_driver=False,
         address={redis_address},
         _redis_password={redis_password},
         job_id=ray.JobID({job_id}),
         job_config=ray.job_config.JobConfig({job_config_args}),
         log_dir={log_dir},
         logging_level={logging_level},
         namespace={namespace},
)
import {driver_entry}
{driver_entry}.main({driver_args})
# If the driver exits normally, we invoke Ray.shutdown() again
# here, in case the user code forgot to invoke it.
ray.shutdown()
"""

    def __init__(self, job_info, redis_address, redis_password, log_dir, ppid):
        super().__init__(job_info)
        self._redis_address = redis_address
        self._redis_password = redis_password
        self._log_dir = log_dir
        self._jvm_options_helper = JvmOptionsHelper()
        self._ppid = ppid

    async def _gen_driver_code(self):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        driver_entry_file = job_consts.JOB_DRIVER_ENTRY_FILE.format(
            temp_dir=temp_dir, job_id=job_id, uuid=uuid.uuid4())
        ip, port = self._redis_address

        cluster_default_jvm_options = (
            await self._jvm_options_helper._get_cluster_default_jvm_options())

        # Per job config
        job_config_items = {
            "worker_env": self._job_info.env(),
            "num_java_workers_per_process": self._job_info.
            num_java_workers_per_process(),
            "java_worker_process_default_memory_mb": self._job_info.
            java_worker_process_default_memory_mb(),
            "num_initial_java_worker_processes": self._job_info.
            num_initial_java_worker_processes(),
            "total_memory_mb": self._job_info.total_memory_mb(),
            "max_total_memory_mb": self._job_info.max_total_memory_mb(),
            "java_heap_fraction": self._job_info.java_heap_fraction(),
            "jvm_options": cluster_default_jvm_options +
            self._job_info.jvm_options(),
            "code_search_path": [job_unpack_dir],
            "long_running": self._job_info.long_running(),
            "logging_level": self._job_info.logging_level(),
            "actor_task_back_pressure_enabled": self._job_info.
            actor_task_back_pressure_enabled(),
            "max_pending_calls": self._job_info.max_pending_calls(),
            "runtime_env": self._job_info.runtime_env(),
        }

        job_config_args = ", ".join(f"{key}={repr(value)}"
                                    for key, value in job_config_items.items()
                                    if value is not None)

        driver_args = ", ".join(
            [repr(x) for x in self._job_info.driver_args()])
        driver_code = self._template.format(
            job_id=repr(hex_to_binary(job_id)),
            job_config_args=job_config_args,
            import_path=repr(job_unpack_dir),
            redis_address=repr(ip + ":" + str(port)),
            redis_password=repr(self._redis_password),
            driver_entry=self._job_info.driver_entry(),
            driver_args=driver_args,
            log_dir=repr(self._log_dir),
            logging_level=repr(self._job_info.logging_level() or LOGGER_LEVEL),
            namespace=repr(self._job_info.namespace()))
        logger.info(f"The generated driver code:\n{driver_code}")
        with open(driver_entry_file, "w") as fp:
            fp.write(driver_code)
        return driver_entry_file

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        log_dir = self._job_info.log_dir()
        job_id = self._job_info.job_id()
        virtualenv_path = job_consts.PYTHON_VIRTUALENV_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        python = self._get_virtualenv_python(virtualenv_path)
        driver_file = await self._gen_driver_code()
        container = self._job_info.container_runtime_env()
        if container:
            if "image" not in container:
                raise Exception("No avaiable image in container runtime env")
            # TODO(SongGuyang): Make these hard code driver command
            # configurable.
            # TODO(wuhua.ck): remove AUDIT_WRITE module
            host_site_packages_path = get_ray_site_packages_path()
            python_version = container.get("python")
            if python_version is not None:
                host_site_packages_path = get_specify_python_package(
                    python_version)
                if host_site_packages_path is None:
                    raise RuntimeError(
                        "Python {} is not supported on current platform".
                        format(python_version))
            job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
                temp_dir=self._job_info.temp_dir(), job_id=job_id)
            shell_cmd = " ".join(["python", "-u", driver_file])
            driver_cmd = [
                "sudo", "-E", "podman", "run", "--rm", "-u", "admin", "-w",
                job_unpack_dir, "--cap-add=AUDIT_WRITE", "-v",
                "/home/admin/ray-pack:/home/admin/ray-pack", "-v",
                "/apsara:/apsara", "-v",
                f"{host_site_packages_path}:" + RAY_CONTAINER_MOUNT_DIR,
                "--cgroup-manager=cgroupfs", "--network=host", "--pid=host",
                "--ipc=host", "--env-host", "-v",
                "/home/admin/logs:/home/admin/logs", "--entrypoint", "bash",
                "--env", f"RAY_RAYLET_PID={self._ppid}", "--env",
                "PYTHONPATH=" + RAY_CONTAINER_MOUNT_DIR, "--env",
                "PYENV_VERSION=", "--rootfs", container["image"] + ":O", "-l",
                "-c", shell_cmd
            ]
        else:
            driver_cmd = [python, "-u", driver_file]
        stdout_file, stderr_file = self._new_log_files(log_dir,
                                                       f"driver-{job_id}")
        with StatsCollector.stats_context(job_id, "StartDriver"):
            return await self._start_driver(driver_cmd, stdout_file,
                                            stderr_file, self._job_info.env())


class PrepareJavaEnviron(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        os.makedirs(job_unpack_dir, exist_ok=True)
        java_shared_library_dir = job_consts.JAVA_SHARED_LIBRARY_DIR.format(
            temp_dir=temp_dir)
        os.makedirs(java_shared_library_dir, exist_ok=True)

        async def verify_downloaded(file, content_hash):
            calc_md5_cmd = ["md5sum", file]
            output = await self._check_output_cmd(calc_md5_cmd)
            calc_md5 = output.split()[0]
            if calc_md5.lower() != content_hash.lower():
                raise Exception(f"Downloaded file {file} "
                                f"is corrupted: "
                                f"{calc_md5} != {content_hash}(expected)")
            return True

        with StatsCollector.stats_context(job_id, "DownloadJavaDependency"):
            dependencies = self._job_info.java_dependency_list()
            for d in dependencies:
                with StatsCollector.stats_context(job_id, f"Download {d.url}"):
                    await self._download_package_with_cache(
                        http_session=self._http_session,
                        download_dir=job_unpack_dir,
                        cache_dir=java_shared_library_dir,
                        url=d.url,
                        content_hash=d.md5 or "",
                        async_validator=verify_downloaded)


class StartJavaDriver(JobProcessor):
    def __init__(self, job_info, redis_address, redis_password, node_ip,
                 log_dir):
        super().__init__(job_info)
        self._redis_address = redis_address
        self._redis_password = redis_password
        self._node_ip = node_ip
        self._log_dir = log_dir
        self._jvm_options_helper = JvmOptionsHelper()

    async def _build_java_worker_command(self):
        """This method assembles the command used to start a Java worker.

        Returns:
            The command string for starting Java worker.
        """
        ip, port = self._redis_address
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)

        options = {
            "ray.address": f"{ip}:{port}",
            "ray.node-ip": self._node_ip,
            "ray.redis.password": self._redis_password or "",
            "ray.home": RAY_HOME,
            "ray.run-mode": "CLUSTER",
            "ray.logging.dir": self._log_dir,
            "ray.job.id": self._job_info.job_id(),
            "ray.job.worker-cwd": job_unpack_dir,
            "ray.job.code-search-path": job_unpack_dir,
            "ray.job.num-java-workers-per-process": self._job_info.
            num_java_workers_per_process(),
            "ray.job.java-worker-process-default-memory-mb": self._job_info.
            java_worker_process_default_memory_mb(),
            "ray.job.num-initial-java-worker-processes": self._job_info.
            num_initial_java_worker_processes(),
            "ray.job.total-memory-mb": self._job_info.total_memory_mb(),
            "ray.job.max-total-memory-mb": self._job_info.
            max_total_memory_mb(),
            "ray.job.java-heap-fraction": self._job_info.java_heap_fraction(),
            "ray.job.long-running": self._job_info.long_running(),
            "ray.job.logging-level": self._job_info.logging_level(),
            "ray.job.actor-task-back-pressure-enabled": self._job_info.
            actor_task_back_pressure_enabled(),
            "ray.job.max-pending-calls": self._job_info.max_pending_calls(),
            "ray.job.serialized-runtime-env": json.dumps(
                self._job_info.runtime_env(),
                sort_keys=True,
                separators=(",", ":")),
            "ray.job.namespace": self._job_info.namespace(),
        }

        # Convert all boolean kv from True/False to true/false.
        for key, value in options.items():
            if type(value) == bool:
                options[key] = str(value).lower()

        options.update({
            f"ray.job.worker-env.{key}": value
            for key, value in self._job_info.env().items()
        })

        cluster_default_jvm_options = (
            await self._jvm_options_helper._get_cluster_default_jvm_options())
        options.update({
            f"ray.job.jvm-options.{i}": jvm_option
            for i, jvm_option in enumerate(cluster_default_jvm_options +
                                           self._job_info.jvm_options())
        })

        command = ["java"] + cluster_default_jvm_options + [
            f"-D{key}={value}" for key, value in options.items()
            if value is not None
        ]

        # Add ray jars path to java classpath
        ray_jars = ":".join([
            os.path.join(jar_dir, "*")
            for jar_dir in get_ray_jars_dirs() + [job_unpack_dir]
        ])
        command += ["-cp", ray_jars]
        # Put `jvm_options` in the last, so it can overwrite the
        # above jvm_options.
        command += self._job_info.jvm_options()
        # Driver entry and driver args.
        command += ["io.ray.runtime.runner.worker.DefaultDriver"]
        command += [self._job_info.driver_entry()]
        command += self._job_info.driver_args()

        return command

    async def run(self):
        driver_cmd = await self._build_java_worker_command()
        log_dir = self._job_info.log_dir()
        job_id = self._job_info.job_id()
        stdout_file, stderr_file = self._new_log_files(log_dir,
                                                       f"driver-{job_id}")
        with StatsCollector.stats_context(job_id, "StartDriver"):
            return await self._start_driver(driver_cmd, stdout_file,
                                            stderr_file, self._job_info.env())


class PrepareCppEnviron(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):

        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        os.makedirs(job_unpack_dir, exist_ok=True)
        cpp_shared_library_dir = job_consts.CPP_SHARED_LIBRARY_DIR.format(
            temp_dir=temp_dir)
        os.makedirs(cpp_shared_library_dir, exist_ok=True)

        async def verify_downloaded(file, content_hash):
            calc_md5_cmd = ["md5sum", file]
            output = await self._check_output_cmd(calc_md5_cmd)
            calc_md5 = output.split()[0]
            if calc_md5.lower() != content_hash.lower():
                raise Exception(f"Downloaded file {file} "
                                f"is corrupted: "
                                f"{calc_md5} != {content_hash}(expected)")
            return True

        with StatsCollector.stats_context(job_id, "DownloadCppDependency"):
            dependencies = self._job_info.cpp_dependency_list()
            for d in dependencies:
                with StatsCollector.stats_context(job_id, f"Download {d.url}"):
                    await self._download_package_with_cache(
                        http_session=self._http_session,
                        download_dir=job_unpack_dir,
                        cache_dir=cpp_shared_library_dir,
                        url=d.url,
                        content_hash=d.md5 or "",
                        async_validator=verify_downloaded)
        return


class StartCppDriver(JobProcessor):
    def __init__(self, job_info, redis_address, redis_password, node_ip,
                 log_dir, node_manager_port, raylet_name, object_store_name):
        super().__init__(job_info)
        self._redis_address = redis_address
        self._redis_password = redis_password
        self._node_ip = node_ip
        self._log_dir = log_dir
        self._node_manager_port = node_manager_port
        self._raylet_name = raylet_name
        self._object_store_name = object_store_name

    def _build_cpp_driver_command(self):
        """This method assembles the command used to start a Cpp worker.

        Returns:
            The command string for starting Cpp worker.
        """
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        driver_entry_path = os.path.join(job_unpack_dir,
                                         self._job_info.driver_entry())
        command = [driver_entry_path]

        ip, port = self._redis_address
        redis_address = f"{ip}:{port}"
        command += [f"--ray_address={redis_address}"]
        if self._redis_password is not None:
            command += [f"--ray_redis_password={self._redis_password}"]
        else:
            command += ["--ray_redis_password="]
        command += [f"--ray_job_id={self._job_info.job_id()}"]
        command += [f"--ray_node_manager_port={self._node_manager_port}"]
        command += [f"--ray_raylet_socket_name={self._raylet_name}"]
        command += [
            f"--ray_plasma_store_socket_name={self._object_store_name}"
        ]
        command += [f"--ray_logs_dir={self._log_dir}"]

        total_memory_mb = self._job_info.total_memory_mb()
        max_total_memory_mb = self._job_info.max_total_memory_mb()
        if total_memory_mb is not None:
            command += [f"--ray_job_total_memory_mb={total_memory_mb}"]
        if max_total_memory_mb is not None:
            command += [f"--ray_job_max_total_memory_mb={max_total_memory_mb}"]
        command += [
            "--ray_job_worker_env=" + json.dumps(
                self._job_info.env(), sort_keys=True)
        ]
        if self._job_info.namespace() is not None:
            command += [
                f"--ray_job_namespace={str(self._job_info.namespace())}"
            ]
        command += self._job_info.driver_args()

        os.chmod(driver_entry_path, stat.S_IRUSR | stat.S_IXUSR)
        return command

    async def run(self):
        driver_cmd = self._build_cpp_driver_command()
        log_dir = self._job_info.log_dir()
        job_id = self._job_info.job_id()
        stdout_file, stderr_file = self._new_log_files(log_dir,
                                                       f"driver-{job_id}")
        with StatsCollector.stats_context(job_id, "StartDriver"):
            return await self._start_driver(driver_cmd, stdout_file,
                                            stderr_file, self._job_info.env())


class JobAgent(dashboard_utils.DashboardAgentModule,
               job_agent_pb2_grpc.JobAgentServiceServicer):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_table = {}
        global download_manager
        if download_manager is None:
            download_manager = DownloadManager(self._dashboard_agent.temp_dir)
        logger.info("The concurrency of job environment preparation: %s",
                    job_consts.JOB_PREPARE_ENVIRONMENT_SEMAPHORE.value)
        logger.info("The concurrency of downloads: %s",
                    job_consts.DOWNLOAD_MANAGER_SEMAPHORE.value)

    async def _prepare_job_environ(self, job_info, request):
        job_id = job_info.job_id()
        last_ex = None
        os.makedirs(
            job_consts.JOB_DIR.format(
                temp_dir=job_info.temp_dir(), job_id=job_id),
            exist_ok=True)
        os.makedirs(
            job_consts.PYTHON_SHARED_NUMBA_CACHE_DIR.format(
                temp_dir=job_info.temp_dir()),
            exist_ok=True)
        http_session = self._dashboard_agent.http_session
        if request.start_driver:
            retry_times = job_consts.JOB_RETRY_TIMES_FOR_DRIVER_NODE
            retry_times_str = str(job_consts.JOB_RETRY_TIMES_FOR_DRIVER_NODE)
        else:
            retry_times = job_consts.JOB_RETRY_TIMES_FOR_WORKER_NODE
            retry_times_str = "infinite"
            assert retry_times == sys.maxsize
        for i in range(retry_times):
            processors = []
            concurrent_tasks = []
            if job_info.is_pip_no_cache(
            ) is None and i >= \
                    job_consts.PIP_INSTALL_WITH_NO_CACHE_RETRY_THRESHOLD:
                job_info.set_pip_no_cache(True)

            if job_info.is_pip_no_cache():
                logger.info("[%s] Retry pip install with no cache [%s/%s]",
                            job_id, i, retry_times_str)

            async def _clean_all_concurrent_tasks():
                logger.info("[%s] Clean all concurrent tasks.", job_id)
                for task in concurrent_tasks:
                    task.cancel()
                await asyncio.gather(*concurrent_tasks, return_exceptions=True)
                for p in processors:
                    await p.clean()

            try:
                logger.info("[%s] Job pending.", job_id)
                async with job_consts.JOB_PREPARE_ENVIRONMENT_SEMAPHORE:
                    language_to_processor = {
                        "PYTHON": PreparePythonEnviron(job_info, http_session),
                        "JAVA": PrepareJavaEnviron(job_info, http_session),
                        "CPP": PrepareCppEnviron(job_info, http_session),
                    }
                    processors.extend([
                        DownloadPackage(job_info, http_session),
                        DownloadArchive(job_info, http_session),
                    ])
                    for language in set([job_info.language()] +
                                        job_info.supported_languages()):
                        processors.append(language_to_processor[language])
                    logger.info("[%s] Job starting to initialize env.", job_id)
                    with async_timeout.timeout(
                            job_info.initialize_env_timeout_seconds()):
                        concurrent_tasks = [
                            create_task(p.run()) for p in processors
                        ]
                        await asyncio.gather(*concurrent_tasks)
                break
            except (JobFatalError, asyncio.CancelledError) as ex:
                await _clean_all_concurrent_tasks()
                raise ex
            except Exception as ex:
                logger.exception(ex)
                last_ex = ex
                await _clean_all_concurrent_tasks()
                logger.info("[%s] Retry to prepare job environment [%s/%s].",
                            job_id, i + 1, retry_times_str)
                await asyncio.sleep(job_consts.JOB_RETRY_INTERVAL_SECONDS)
                if not hasattr(ex, "last_n_lines_of_stderr"
                               ) or job_info.is_pip_no_cache() is not None:
                    continue
                for retry_pattern in \
                        job_consts.EXCEPTION_NEED_PIP_INSTALL_WITH_ON_CACHE:
                    if re.search(retry_pattern, ex.stderr):
                        job_info.set_pip_no_cache(True)
                        break
            finally:
                if job_info.job_is_stopped():
                    await _clean_all_concurrent_tasks()
                    raise asyncio.CancelledError
        else:
            logger.error("[%s] Failed to prepare job environment.", job_id)
            raise last_ex
        logger.info("[%s] Finished to prepare job environment.", job_id)

    async def _initialize_job_env(self, request, context):
        def _reply_ok():
            driver_pid = 0
            driver_cmdline = ""
            if request.start_driver:
                driver = job_info.driver()
                if driver:
                    driver_pid = driver.pid
                    driver_cmdline = driver.cmdline

            logger.info("[%s] Initialize job environment success.", job_id)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
                stats=StatsCollector.get(job_id),
                driver_pid=driver_pid,
                driver_cmdline=driver_cmdline)

        job_id = binary_to_hex(request.job_data.job_id)
        job_info = self._job_table.get(job_id)
        if job_info is not None:
            skip = True
            if (job_info.initialize_task()
                    and not job_info.initialize_task().done()):
                logger.info(
                    "[%s] The job environment is initializing, "
                    "skip initializing job environment.", job_id)
            elif job_info.driver():
                logger.info(
                    "[%s] The job driver has been started, "
                    "skip initializing job environment.", job_id)
            else:
                skip = False
                logger.info("[%s] Reinitialize job environment.", job_id)
            if skip:
                return _reply_ok()

        try:
            job_info = JobInfo(job_id, json.loads(
                request.job_data.job_payload), self._dashboard_agent.temp_dir,
                               self._dashboard_agent.log_dir)
        except json.JSONDecodeError as ex:
            error_message = str(ex)
            error_message += f", job_payload:\n{request.job_data.job_payload}"
            logger.error("[%s] Initialize job environment failed, %s.", job_id,
                         error_message)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                stats=StatsCollector.get(job_id),
                error_message=error_message)
        except Exception as ex:
            logger.exception(ex)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                stats=StatsCollector.get(job_id),
                error_message=traceback.format_exc())

        async def _initialize_job_env():
            if job_info.is_environ_ready():
                logger.info("[%s] Job environment is ready.", job_id)
            else:
                await self._prepare_job_environ(job_info, request)
                job_info.mark_environ_ready()
            if request.start_driver:
                logger.info("[%s] Starting driver.", job_id)
                job_info.setenv(
                    job_consts.JOB_DATA_DIR_BASE_ENV_KEY,
                    job_consts.JOB_DATA_DIR_BASE_TEMPLATE.format(
                        temp_dir=job_info.temp_dir(), job_id=job_id))
                language = job_info.language()
                if language == "PYTHON":
                    driver = await StartPythonDriver(
                        job_info, self._dashboard_agent.redis_address,
                        self._dashboard_agent.redis_password,
                        self._dashboard_agent.log_dir,
                        self._dashboard_agent.ppid).run()
                elif language == "JAVA":
                    driver = await StartJavaDriver(
                        job_info, self._dashboard_agent.redis_address,
                        self._dashboard_agent.redis_password,
                        self._dashboard_agent.ip,
                        self._dashboard_agent.log_dir).run()
                elif language == "CPP":
                    driver = await StartCppDriver(
                        job_info, self._dashboard_agent.redis_address,
                        self._dashboard_agent.redis_password,
                        self._dashboard_agent.ip,
                        self._dashboard_agent.log_dir,
                        self._dashboard_agent.node_manager_port,
                        self._dashboard_agent.raylet_name,
                        self._dashboard_agent.object_store_name).run()
                else:
                    raise Exception(f"Unsupported language type: {language}")
                job_info.set_driver(driver)
            else:
                logger.info("[%s] Not start driver.", job_id)

        initialize_task = create_task(_initialize_job_env())
        job_info.set_initialize_task(initialize_task)
        # For CleanJobEnv to cancel the initialize task, it should be set
        # before any await.
        self._job_table[job_id] = job_info

        try:
            await initialize_task
        except asyncio.CancelledError:
            logger.error("[%s] Initialize job environment has been cancelled.",
                         job_id)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                stats=StatsCollector.get(job_id),
                error_message="InitializeJobEnv has been cancelled, "
                "did you call CleanJobEnv?")
        except Exception as ex:
            logger.exception(ex)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                stats=StatsCollector.get(job_id),
                error_message=traceback.format_exc())

        return _reply_ok()

    async def InitializeJobEnv(self, request, context):
        job_id = binary_to_hex(request.job_data.job_id)
        StatsCollector.clear(job_id)
        try:
            with StatsCollector.stats_context(job_id, "InitializeJobEnv"):
                return await self._initialize_job_env(request, context)
        finally:
            StatsCollector.clear(job_id)

    async def CleanJobEnv(self, request, context):
        job_id = binary_to_hex(request.job_id)
        job_info = self._job_table.pop(job_id, None)

        # clean JobAgent's cache
        if job_info:
            assert isinstance(job_info, JobInfo)
            logger.info("[%s] Clean job environment found job info.", job_id)

            # Set job_info._job_is_stopped to True.
            job_info.stop_job()

            # Cancel job task.
            cancel_job_task = [
                job_info.initialize_task(),
            ]
            for cancel_task in cancel_job_task:
                try:
                    if cancel_task and not cancel_task.done():
                        logger.info("[%s] Cancelling task %s.", job_id,
                                    cancel_task)
                        cancel_task.cancel()
                        try:
                            await cancel_task
                        except asyncio.CancelledError:
                            pass
                        logger.info("[%s] Task %s has been cancelled.", job_id,
                                    cancel_task)
                except Exception as ex:
                    logger.exception(ex)
        else:
            logger.info("[%s] Clean job environment not found job info.",
                        job_id)

        # Kill driver.
        driver_pid = request.driver_pid
        if job_info:
            job_info.set_driver(None)
        if driver_pid > 0:
            try:
                driver = psutil.Process(driver_pid)

                if "RAY_JOB_ID_FOR_DRIVER" not in driver.environ(
                ) or driver.environ()["RAY_JOB_ID_FOR_DRIVER"] != job_id:
                    raise psutil.NoSuchProcess(
                        f"[{job_id}] driver process already died, this "
                        f"process cmdline: {' '.join(driver.cmdline())}")

                logger.info("[%s] Killing driver %s.", job_id, driver.pid)
                driver.kill()

                loop = asyncio.get_event_loop()

                await loop.run_in_executor(None, driver.wait)

                logger.info("[%s] Driver %s has been killed.", job_id,
                            driver.pid)
            except psutil.NoSuchProcess as error:
                logger.info(
                    "[%s] Driver pid: %s, already died, error message: %s",
                    job_id, driver_pid, str(error))
        else:
            logger.info("[%s] driver not on current node.", job_id)

        async def _delete_virtualenv(virtualenv_path):
            shutil.rmtree(virtualenv_path, ignore_errors=True)

        # Try to deref virtualenv regardless of whether the job uses
        # virtualenv or not.
        await VenvCache.deref_virtualenv(job_id, _delete_virtualenv)

        job_dir = job_consts.JOB_DIR.format(
            temp_dir=self._dashboard_agent.temp_dir, job_id=job_id)
        logger.info("[%s] Removing job directory %s.", job_id, job_dir)

        shutil.rmtree(job_dir, ignore_errors=True)
        logger.info("[%s] Clean job environment success.", job_id)
        return job_agent_pb2.CleanJobEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK)

    async def run(self, server):
        job_agent_pb2_grpc.add_JobAgentServiceServicer_to_server(self, server)
        cache_dir = job_consts.PYTHON_VIRTUALENV_CACHE_DIR.format(
            temp_dir=self._dashboard_agent.temp_dir)

        async def _delete_virtualenv(virtualenv_path):
            shutil.rmtree(virtualenv_path, ignore_errors=True)

        await VenvCache.init(cache_dir, _delete_virtualenv)
        job_utils.JobData.monitor_job_data(
            job_consts.JOB_ROOT_DIR.format(
                temp_dir=self._dashboard_agent.temp_dir))

        if job_consts.ENABLE_AUTO_CLEAN_ARCHIVE:
            archive_clear = ArchiveCacheClear(self._dashboard_agent.temp_dir)
            await archive_clear.clean_archive_cache()
