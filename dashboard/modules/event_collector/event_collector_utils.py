import os
import time
import json
import fnmatch
import asyncio
import datetime
import itertools
import collections
import logging.handlers

import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.modules.event_collector.event_collector_consts as event_collector_consts
from ray.new_dashboard.utils import async_loop_forever
from ray.core.generated import event_pb2

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)


def _get_running_loop():
    try:
        return asyncio.get_running_loop()
    except AttributeError:
        # For Python 3.6
        return asyncio._get_running_loop()


def setup_persistence_event_logger(
        name,
        log_file,
        level=logging.INFO,
        max_bytes=dashboard_consts.LOGGING_ROTATE_BYTES,
        backup_count=dashboard_consts.LOGGING_ROTATE_BACKUP_COUNT):
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def cleanup_persistence_event_logger(name):
    logger = logging.getLogger(name)
    for handler in logger.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            handler.close()
            logger.removeHandler(handler)
            break
    log_class = logging.getLoggerClass()
    log_class.manager.loggerDict.pop(name, None)


def _get_source_files(event_dir, source_types=None, event_file_filter=None):
    event_log_names = os.listdir(event_dir)
    source_files = {}
    for source_type in source_types or event_pb2.Event.SourceType.keys():
        files = []
        for n in event_log_names:
            if fnmatch.fnmatch(n, f"*{source_type}*"):
                f = os.path.join(event_dir, n)
                if event_file_filter is not None and not event_file_filter(f):
                    continue
                files.append(f)
        if files:
            source_files[source_type] = files
    return source_files


def get_event_strings(event_dir, source_types=None):
    source_files = _get_source_files(event_dir, source_types)
    event_strings = []
    for filename in itertools.chain(*source_files.values()):
        logger.info("Get event strings from %s", filename)
        with open(filename, "r") as f:
            event_strings.extend(f.read().splitlines())
    return event_strings


def _tail_bytes(file, n, pos=0, whence=os.SEEK_END, closefd=True):
    """
    Get tail n bytes from files.
    :param file: File path or fd list, latest file first.
    :param n: bytes.
    :return: tail n bytes in str, aligned to line.
    """
    if n <= 0:
        return ""

    with open(file, "rb", closefd=closefd) as f:
        f.seek(pos, whence)
        file_size = f.tell()
        if n >= file_size:
            f.seek(0, os.SEEK_SET)
            data = f.read(file_size)
        else:
            f.seek(-n, os.SEEK_CUR)
            data = f.read(n)
        return data.decode("utf-8")


def _default_sort_func(source_type, files):
    return sorted(files)


async def get_last_event_strings(event_dir,
                                 last_nbytes,
                                 initialized_files=None,
                                 sort_func=_default_sort_func):
    if last_nbytes <= 0:
        return {}

    loop = _get_running_loop()
    source_files = await loop.run_in_executor(None, _get_source_files,
                                              event_dir)
    if len(source_files) == 0:
        return {}

    logger.info("Get events from logs:\n%s",
                "\n".join(f"\t{k}: {str(v)}" for k, v in source_files.items()))

    def _align_to_line(s):
        first_line_sep = s.find("\n")
        if first_line_sep == -1:
            return ""
        return s[first_line_sep + 1:]

    def _get_tail_bytes_per_source(source_type, files):
        if sort_func is not None:
            files = sort_func(source_type, files)
        open_files = [_open_file(path, os.O_RDONLY) for path in files]
        tail_bytes = []
        try:
            n = last_nbytes
            logger.info("Read tail %s bytes from %s: %s", last_nbytes,
                        source_type, files)
            found_init_pos = False
            for idx, file_info in enumerate(open_files):
                if initialized_files is None or found_init_pos:
                    data = _tail_bytes(file_info.fd, n, closefd=False)
                else:
                    tail_file = initialized_files.get(file_info.ino)
                    if tail_file is None:
                        if idx == 0:
                            logger.warning(
                                "Log files may have rotated, %s: %s.",
                                source_type, files)
                        continue
                    else:
                        found_init_pos = True
                        logger.info("Found initialized pos of %s at %s:%s",
                                    source_type, files[idx],
                                    tail_file.begin_position)
                        data = _tail_bytes(
                            file_info.fd,
                            n,
                            tail_file.begin_position,
                            os.SEEK_SET,
                            closefd=False)
                tail_bytes.append(data)
                n -= len(data)
                if n <= 0:
                    break
        finally:
            for file_info in open_files:
                try:
                    os.close(file_info.fd)
                except OSError:
                    pass
        if len(tail_bytes) == 0:
            return source_type, ""
        elif len(tail_bytes) == 1:
            return source_type, _align_to_line(tail_bytes[0])
        else:
            return source_type, _align_to_line("".join(reversed(tail_bytes)))

    concurrent_tasks = [
        loop.run_in_executor(None, _get_tail_bytes_per_source, source_type,
                             files)
        for source_type, files in source_files.items()
    ]
    return dict(await asyncio.gather(*concurrent_tasks))


def _parse_line(event_str):
    try:
        return json.loads(event_str)
    except json.decoder.JSONDecodeError:
        values = event_str.split("|||")
        keys = ("timestamp", "eventId", "jobId", "nodeId", "taskId",
                "sourceType", "sourceHostname", "sourcePid", "severity",
                "label", "message")
        event = dict(zip(keys, values))
        event["timestamp"] = datetime.datetime.strptime(
            event["timestamp"], '%Y-%m-%d %H:%M:%S.%f').timestamp()
        event["sourcePid"] = int(event["sourcePid"])
        return event


def parse_event_strings(event_string_list):
    events = []
    for data in event_string_list:
        if not data:
            continue
        try:
            event = _parse_line(data)
            events.append(event)
        except Exception:
            logger.exception("Parse event line failed: %s", data)
    return events


class TailFile:
    def __init__(self,
                 file,
                 callback,
                 pos=0,
                 whence=os.SEEK_END,
                 read_bytes=1024,
                 closefd=True,
                 loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._file = file

        self._callback = callback
        self._pos = pos
        self._whence = whence
        self._read_bytes = read_bytes
        self._close_fd = closefd
        if isinstance(file, str):
            assert closefd is True
            fd = os.open(file, os.O_RDONLY)
        elif isinstance(file, int):
            fd = file
        else:
            raise Exception(f"TailFile: file should be str or int, "
                            f"but got {type(file)} instead.")
        try:

            begin_position = self._seek_begin_position(file, fd, pos, whence,
                                                       read_bytes)
        except Exception as ex:
            if closefd:
                os.close(fd)
            raise ex
        self._fd = fd
        self._begin_position = begin_position
        self._finish_position = 0
        self._finish_event = asyncio.Event()
        self._cache_line = ""
        self._exception = None
        try:
            self._loop.add_reader(fd, self._reader)
        except Exception as ex:
            self.cancel()
            raise ex

    @staticmethod
    def _seek_begin_position(file, fileno, pos, whence, read_bytes):
        begin_position = os.lseek(fileno, pos, whence)
        if pos != 0 or whence != os.SEEK_SET:
            # Find nearest \n from begin_position to begin_position - read_bytes.
            max_line_bytes = min(begin_position, read_bytes)
            seek_position = os.lseek(fileno, -max_line_bytes, os.SEEK_CUR)
            sep_index = os.read(fileno, max_line_bytes).rfind(b"\n")
            if sep_index != -1:
                # Found \n, reset the cursor after \n.
                begin_position = os.lseek(
                    fileno, seek_position + sep_index + 1, os.SEEK_SET)
            else:
                if seek_position == 0:
                    # Reach the beginning of file.
                    begin_position = os.lseek(fileno, 0, os.SEEK_SET)
                else:
                    logger.warning(
                        "TailFile: can't find line sep at "
                        "pos: %s, whence: %s, file: %s.", pos, whence, file)
                    begin_position = os.lseek(fileno, pos, whence)
        return begin_position

    def _reader(self):
        try:
            data = os.read(self._fd, self._read_bytes)
        except InterruptedError:
            pass
        except Exception as e:
            logger.exception("TailFile: read file failed: %s", self._file)
            self._exception = e
        else:
            if data:
                data = data.decode()
                lines = data.splitlines()
                if data[-1] == "\n":
                    lines.append("")
                lines[0] = self._cache_line + lines[0]
                self._cache_line = lines[-1]
                if len(lines) > 1:
                    self._callback(lines[:-1])
            else:
                self.cancel()

    @property
    def begin_position(self):
        return self._begin_position

    @property
    def finish_position(self):
        return self._finish_position

    def cancel(self):
        if self._finish_event.is_set():
            return
        try:
            self._loop.remove_reader(self._fd)
            self._finish_position = os.lseek(self._fd, 0, os.SEEK_CUR)
            if self._cache_line:
                self._callback([self._cache_line])
            if self._close_fd:
                os.close(self._fd)
                self._fd = None
        finally:
            self._finish_event.set()

    async def wait(self):
        await self._finish_event.wait()

    def is_valid(self):
        try:
            return os.stat(self._fd).st_nlink != 0
        except OSError:
            return False


FileInfo = collections.namedtuple("FileInfo", ["fd", "ino"])


def _open_file(path, flags, mode=0o777):
    fd = None
    try:
        fd = os.open(path, flags, mode)
        ino = os.stat(fd).st_ino
        if ino:
            return FileInfo(fd, ino)
        else:
            raise Exception(f"Invalid ino {ino}")
    except Exception:
        logger.info("Get file ino failed: %s", path)
        if fd:
            os.close(fd)


MonitorInfo = collections.namedtuple(
    "MonitorInfo", ["task", "initialized_files", "monitor_files"])


async def monitor_events(event_dir,
                         callback,
                         scan_interval_seconds=event_collector_consts.
                         SCAN_EVENT_DIR_INTERVAL_SECONDS,
                         incremental_only=True,
                         source_types=None):
    loop = _get_running_loop()
    monitor_files = {}
    initialized_files = {}
    initialized = asyncio.Event()
    # All event logs modified before this time will be omitted.
    expiration = time.time() - 30 * 60

    def _source_file_filter(source_file):
        stat = os.stat(source_file)
        return stat.st_ino not in monitor_files and stat.st_mtime > expiration

    @async_loop_forever(scan_interval_seconds)
    async def _scan_event_log_files():
        if incremental_only:
            whence = os.SEEK_SET if initialized.is_set() else os.SEEK_END
        else:
            whence = os.SEEK_SET
        # Remove non exist files.
        remove_inos = []
        for ino, tail_file in monitor_files.items():
            if not tail_file.is_valid():
                remove_inos.append(ino)
        for ino in remove_inos:
            try:
                monitor_files[ino].cancel()
            except Exception:
                logger.exception(
                    "Cancel TailFile task of removed file failed.")
            monitor_files.pop(ino, None)
        # Scan event files.
        source_files = await loop.run_in_executor(None, _get_source_files,
                                                  event_dir, source_types,
                                                  _source_file_filter)
        # Open files.
        open_files = await asyncio.gather(*[
            loop.run_in_executor(None, _open_file, filename, os.O_RDONLY)
            for filename in list(itertools.chain(*source_files.values()))
        ])
        # Create TailFile for new files, TailFile close fd.
        for file_info in open_files:
            assert file_info.ino not in monitor_files
            try:
                tail_file = TailFile(
                    file_info.fd, callback, whence=whence, closefd=True)
                monitor_files[file_info.ino] = tail_file
            except Exception:
                logger.exception("TailFile is failed on ino %s.",
                                 file_info.ino)
        # Set initialized.
        if not initialized.is_set():
            initialized_files.update(monitor_files)
            initialized.set()

    task = create_task(_scan_event_log_files())

    async def _waiting_initialized():
        await initialized.wait()
        return MonitorInfo(task, initialized_files, monitor_files)

    return await _waiting_initialized()


ReadFileResult = collections.namedtuple(
    "ReadFileResult", ["fid", "size", "mtime", "position", "lines"])


def _read_file(file, pos, whence, n_bytes=20480, closefd=True):
    with open(file, "rb", closefd=closefd) as f:
        # The ino may be 0 on Windows.
        stat = os.stat(f.fileno())
        fid = stat.st_ino or file
        f.seek(pos, whence)
        data = f.read(n_bytes)
        last_new_line = data.rfind(b"\n")
        if last_new_line == -1:
            return ReadFileResult(fid, stat.st_size, stat.st_mtime, pos, [])
        data_len = len(data)
        data = data[:last_new_line + 1]
        last_line_len = data_len - len(data)
        data = data.decode("utf-8")
        lines = data.splitlines()
        # The last line may be broken.
        return ReadFileResult(fid, stat.st_size, stat.st_mtime,
                              f.tell() - last_line_len, lines)


def monitor_events2(event_dir,
                    callback,
                    scan_interval_seconds=event_collector_consts.
                    SCAN_EVENT_DIR_INTERVAL_SECONDS,
                    incremental_only=True,
                    start_mtime=time.time() - 30 * 60,
                    source_types=None,
                    loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    monitor_files = {}
    initialized = asyncio.Event()

    logger.info(
        "Monitor events logs modified after %s on %s %s, "
        "the source types are %s.", start_mtime, event_dir, "incremental only"
        if incremental_only else "all", "all"
        if source_types is None else source_types)

    MonitorFile = collections.namedtuple("MonitorFile",
                                         ["size", "mtime", "position"])

    def _source_file_filter(source_file):
        stat = os.stat(source_file)
        return stat.st_mtime > start_mtime

    def _read_file_with_cache(file, pos, whence):
        assert isinstance(file, str), \
            f"File should be a str, but a {type(file)}({file}) found"
        fd = os.open(file, os.O_RDONLY)
        try:
            stat = os.stat(fd)
            fid = stat.st_ino or file
            monitor_file = monitor_files.get(fid)
            if monitor_file:
                if (monitor_file.size == stat.st_size
                        and monitor_file.mtime == stat.st_mtime):
                    logger.debug(
                        "Skip reading the file because "
                        "there is no change: %s", file)
                    return
                position = monitor_file.position
                whence = os.SEEK_SET
            else:
                logger.info("Found new event log file: %s", file)
                position = pos
            return _read_file(fd, position, whence, closefd=False)
        except Exception:
            logger.exception("Read event file failed: %s", file)
        finally:
            os.close(fd)

    @async_loop_forever(scan_interval_seconds)
    async def _scan_event_log_files():
        if incremental_only:
            whence = os.SEEK_SET if initialized.is_set() else os.SEEK_END
        else:
            whence = os.SEEK_SET
        # Scan event files.
        source_files = await loop.run_in_executor(None, _get_source_files,
                                                  event_dir, source_types,
                                                  _source_file_filter)

        # Limit concurrent read to avoid fd exhaustion.
        semaphore = asyncio.Semaphore(
            event_collector_consts.CONCURRENT_READ_LIMIT)

        async def _concurrent_coro(filename):
            async with semaphore:
                return await loop.run_in_executor(None, _read_file_with_cache,
                                                  filename, 0, whence)

        # Read files.
        all_read_results = await asyncio.gather(*[
            _concurrent_coro(filename)
            for filename in list(itertools.chain(*source_files.values()))
        ])
        for read_result in all_read_results:
            if read_result is not None:
                try:
                    monitor_file = MonitorFile(read_result.size,
                                               read_result.mtime,
                                               read_result.position)
                    monitor_files[read_result.fid] = monitor_file
                    callback(read_result.lines)
                except Exception:
                    logger.exception("Process read event result failed: %s",
                                     read_result)
        initialized.set()

    return create_task(_scan_event_log_files())
