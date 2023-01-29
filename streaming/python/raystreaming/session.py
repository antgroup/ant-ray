import enum


class JobStatus(enum.Enum):
    """
    Keep this in sync with
     `com.alipay.streaming.runtime.master.joblifecycle.JobStatus`
    """
    # The initial state of a job.
    SUBMITTING = 0
    # Job has been submitted and is long running.
    RUNNING = 1
    # Job is finished.
    FINISHED = 2
    # Job submission failed.
    SUBMITTING_FAILED = 3
    # Unknown state only when trying to transfer an error string to JobStatus.
    UNKNOWN = 4
