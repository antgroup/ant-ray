import ray.worker
import logging
from ray.core.generated import event_pb2

logger = logging.getLogger(__name__)


class EventSeverity:
    @staticmethod
    def is_valid(severity):
        if (isinstance(severity, int)
                and severity <= event_pb2._EVENT_SEVERITY.values[-1].number
                and severity >= 0):
            return True
        else:
            return False

    INFO = event_pb2.Event.Severity.INFO
    WARNING = event_pb2.Event.Severity.WARNING
    ERROR = event_pb2.Event.Severity.ERROR
    FATAL = event_pb2.Event.Severity.FATAL


def report_event(severity, label, message):
    if not EventSeverity.is_valid(severity):
        logger.error(
            "the first argument for the report_event function must be the"
            " EventSeverity")
        return

    severity = event_pb2._EVENT_SEVERITY.values_by_number[severity].name

    if ray.worker.global_worker.mode == ray.worker.LOCAL_MODE:
        logger.info(
            "Python worker don't support report_event function in the Local"
            " Mode.")
    else:
        ray.worker.global_worker.core_worker.report_event(
            severity, label, message)
