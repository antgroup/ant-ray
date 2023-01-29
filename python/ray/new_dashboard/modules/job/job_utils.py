import os
import asyncio
import logging
import psutil
import shutil
import hashlib
import glob
import async_timeout
from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.modules.job import job_consts
from ray.new_dashboard.utils import create_task
import ray.new_dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)


class JobData:
    @staticmethod
    def monitor_job_data(
            job_root_dir,
            scan_interval_seconds=job_consts.JOB_DATA_DIR_CLEAN_INTERVAL):
        def _get_job_data_dirs():
            # {absolute dir path: dir name}
            existing_data_dirs = {}
            data_dir_bases = glob.glob(
                os.path.join(job_root_dir, "*",
                             job_consts.JOB_DATA_DIR_BASE_NAME))
            for job_data_dir_base in data_dir_bases:
                for job_data_dir in os.listdir(job_data_dir_base):
                    existing_data_dirs[os.path.join(
                        job_data_dir_base, job_data_dir)] = job_data_dir
            return existing_data_dirs

        def _is_data_dir_safe_to_delete(path, dir_name):
            worker_pid = int(dir_name)
            if not psutil.pid_exists(worker_pid):
                # Directory not used by any process.
                return True
            worker_process = psutil.Process(worker_pid)
            if worker_process.create_time() > os.stat(path).st_ctime:
                # Directory corresponding process is created after the
                # directory, the pid was reused by the OS.
                return True
            if worker_process.status() == psutil.STATUS_ZOMBIE or \
                    worker_process.status() == psutil.STATUS_DEAD:
                # The process is no longer active
                return True
            # The directory is currently used by an active process which's
            # created before the directory creation.
            return False

        @async_loop_forever(scan_interval_seconds, cancellable=True)
        async def _clean_job_data():
            try:
                loop = asyncio.get_event_loop()
                # Scan data dirs.
                data_dirs = await loop.run_in_executor(None,
                                                       _get_job_data_dirs)
                # Delete directory if safe
                for (path, dir_name) in data_dirs.items():
                    if _is_data_dir_safe_to_delete(path, dir_name):
                        logger.info("Cleaning up worker data directory %s",
                                    path)
                        shutil.rmtree(path, ignore_errors=True)
            except Exception as ex:
                logger.error("Clean job data error: %s", ex)

        logger.info(
            "Start to monitor and clean job data directory, "
            "the temp directory %s", job_root_dir)
        return create_task(_clean_job_data())


def is_vcs(s):
    schema = s.split("://")[0].lower()
    return any(vcs in schema for vcs in ["git", "hg", "svn", "bzr"])


def is_url(s):
    schema = s.split("://")[0].lower()
    return schema in ["http", "https", "ftp", "ftps"]


def is_pip_cmd(s):
    return s.strip().startswith("pip ")


async def get_url_content_hash(http_session,
                               job_id,
                               url,
                               timeout=job_consts.GET_ETAG_TIMEOUT_SECONDS):
    try:
        logger.info("[%s] Get content hash from url: %s", job_id, url)
        async with async_timeout.timeout(timeout):
            headers = {"Range": "bytes=0-0"}
            # HEAD request may not returns Etag and Last-Modified headers,
            # so we use GET with Range 0-0.
            async with http_session.get(
                url, headers=headers, ssl=False) as response:  # noqa
                response.raise_for_status()
                etag = response.headers.get("Etag")
                last_modified = response.headers.get("Last-Modified")
                logger.info("[%s] Content metadata of %s: %s", job_id, url, {
                    "Etag": etag,
                    "Last-Modified": last_modified
                })
                if etag is None and last_modified is None:
                    raise Exception(
                        "Neither Etag nor Last-Modified is supported.")
                else:
                    m = hashlib.md5()
                    if etag is not None:
                        m.update(etag.encode("utf-8"))
                    if last_modified is not None:
                        m.update(last_modified.encode("utf-8"))
                    content_hash = m.hexdigest()
                    logger.info("[%s] Content hash of %s: %s", job_id, url,
                                content_hash)
                    return content_hash
    except Exception as ex:
        logger.info("[%s] Failed to get content hash from %s: %s", job_id, url,
                    ex)
        return None


def hardlink_all_zip_files(src_dir, target_dir, file_list, logger_prefix=None):
    logger.info("[%s] Link all %s file to %s", logger_prefix, src_dir,
                target_dir)
    file_number = 0
    dir_number = 0

    for file_info in file_list:
        if not file_info.is_dir():
            continue
        dir_number += 1
        src_filename = os.path.join(src_dir, file_info.filename)
        if not os.path.exists(src_filename):
            raise RuntimeError("Unzip files are not complete!"
                               f" miss directory: {src_filename}")
        target_filename = os.path.join(target_dir, file_info.filename)
        os.makedirs(target_filename, exist_ok=True)

    for file_info in file_list:
        if file_info.is_dir():
            continue
        file_number += 1
        src_filename = os.path.join(src_dir, file_info.filename)
        if not os.path.exists(src_filename):
            raise RuntimeError("Unzip files are not complete!"
                               f" miss directory: {src_filename}")
        target_filename = os.path.join(target_dir, file_info.filename)
        dashboard_utils.force_hardlink(src_filename, target_filename)

        # set origin file readonly.
        dashboard_utils.file_readonly(
            src_filename, ignore_error=True, logger_prefix=logger_prefix)

    logger.info(
        "[%s] Total link files(%s) and dir(%s) from %s to %s", logger_prefix,
        str(file_number), str(dir_number), src_dir, target_dir)
