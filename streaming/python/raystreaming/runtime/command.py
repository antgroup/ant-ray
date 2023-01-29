from raystreaming.generated.common_pb2 import \
    UnitedDistributedControlMessage as ControlMessage


class BaseWorkerCmd:
    """
    base worker cmd
    """

    def __init__(self, actor_id):
        self.from_actor_id = actor_id


class WorkerCommitReport(BaseWorkerCmd):
    """
    worker commit report
    """

    def __init__(self, actor_id, commit_checkpoint_id):
        super().__init__(actor_id)
        self.commit_checkpoint_id = commit_checkpoint_id


class WorkerRollbackRequest(BaseWorkerCmd):
    """
    worker rollback request
    """

    def __init__(self, actor_id, exception_msg):
        super().__init__(actor_id)
        self.__exception_msg = exception_msg

    def exception_msg(self):
        return self.__exception_msg


class WorkerReceivePartialBarrierReport(BaseWorkerCmd):
    """
    worker receive partial barrier report
    """

    def __init__(self, actor_id, global_checkpoint_id, partial_checkpoint_id):
        super().__init__(actor_id)
        self.global_checkpoint_id = global_checkpoint_id
        self.partial_checkpoint_id = partial_checkpoint_id


class EndOfDataReport(BaseWorkerCmd):
    """
    worker report the end of data stream
    """

    def __init__(self, actor_id):
        super().__init__(actor_id)


class WorkerUnitedDistributedControllerCommitReport(BaseWorkerCmd):
    """
    Report to `JobMaster` that the current worker
    has already committed the specific UDC.
    """

    def __init__(self, actor_id, control_msg: ControlMessage):
        super().__init__(actor_id)
        self.control_message = control_msg
