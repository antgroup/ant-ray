from ray.ray_constants import env_integer
from ray.core.generated import event_pb2

PER_JOB_EVENTS_COUNT_LIMIT = 500
SCAN_EVENT_DIR_INTERVAL_SECONDS = 30
EVENT_AGENT_REPORT_INTERVAL_SECONDS = 1
EVENT_AGENT_RETRY_INTERVAL_SECONDS = 2
EVENT_AGENT_CACHE_SIZE = 10240
EVENT_AGENT_RECOVER_BYTES = 1024 * 100
EVENT_KEYS = {
    "timestamp", "eventId", "jobId", "nodeId", "taskId", "sourceType",
    "sourceHostname", "sourcePid", "severity", "label", "message"
}
EVENT_HEAD_PERSISTENT_LOGGER_COUNT = env_integer(
    "EVENT_HEAD_PERSISTENT_LOGGER_COUNT", 50)
EVENT_SOURCE_PERSISTENCE = event_pb2.Event.SourceType.Name(
    event_pb2.Event.PERSISTENCE)
EVENT_SOURCE_NON_PERSISTENCE = list(
    set(event_pb2.Event.SourceType.keys()) - {EVENT_SOURCE_PERSISTENCE})
CONCURRENT_READ_LIMIT = 50
