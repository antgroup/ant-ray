import abc
import logging
import datetime
import cProfile
import io
import os
import pstats
import glob
try:
    import yappi
except ImportError:
    yappi = None

logger = logging.getLogger(__name__)


def get_profile_file(timestamp: datetime.datetime = None):
    try:
        import ray.worker
        _global_node = ray.worker._global_node
        log_dir = _global_node.get_logs_dir_path()
    except (ImportError, AttributeError):
        log_dir = os.path.dirname(__file__)

    out_dir = os.path.join(log_dir or ".", "perf")
    os.makedirs(out_dir, exist_ok=True)

    # Lower resolution.
    timestamp = timestamp.replace(second=0, microsecond=0)
    for p in glob.glob(os.path.join(out_dir, f"{os.getpid()}_*")):
        try:
            name = os.path.basename(p)
            name = os.path.splitext(name)[0]
            delta = timestamp - datetime.datetime.fromisoformat(
                name.split("_")[1])
            if delta.total_seconds() < 60 and os.stat(p).st_size == 0:
                return p
        except Exception:
            logger.exception("Get profile file failed, the timestamp is %s",
                             timestamp)
    return os.path.join(out_dir, f"{os.getpid()}_{timestamp.isoformat()}.prof")


class _Profiler(metaclass=abc.ABCMeta):
    def __init__(self):
        self._profile_time = None

    def start(self):
        self._profile_time = t = datetime.datetime.now()
        logger.info("Started profiling at %s by %s.", t, type(self).__name__)

    @abc.abstractmethod
    def stop(self):
        pass

    def _dump_stats(self, stats: pstats.Stats):
        if self._profile_time is None:
            logger.error("Profiling time is None.")
        else:
            profile_time = self._profile_time
            self._profile_time = None
            stats_file = get_profile_file(profile_time)
            try:
                stats.dump_stats(stats_file)
            finally:
                stats.stream = stream = io.StringIO()
                stats.sort_stats(pstats.SortKey.CUMULATIVE) \
                    .print_stats(20)
                logger.info(
                    "Profiling (%s) stopped.\n"
                    "The full result will be dumped into %s.\n"
                    "%s", profile_time, stats_file, stream.getvalue())


class _cProfile(_Profiler):
    def __init__(self):
        super().__init__()
        self._profile = None

    def start(self):
        super().start()
        self._profile = cProfile.Profile()
        self._profile.enable()

    def stop(self):
        if not self._profile:
            return
        self._profile.disable()
        stats = pstats.Stats(self._profile)
        self._dump_stats(stats)
        self._profile = None


class _yappi(_Profiler):
    def start(self):
        super().start()
        yappi.start()

    def stop(self):
        if not yappi.is_running():
            return
        yappi.stop()
        func_stats = yappi.get_func_stats()
        stats = yappi.convert2pstats(func_stats)
        self._dump_stats(stats)


Profiler = _yappi() if yappi is not None else _cProfile()
