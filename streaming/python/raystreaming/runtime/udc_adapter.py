from abc import ABC, abstractmethod
from raystreaming.generated.common_pb2 import \
    UnitedDistributedControlMessage as ControlMessage
from raystreaming.runtime.command import \
    WorkerUnitedDistributedControllerCommitReport
from ray.actor import ActorHandle
import ray
from raystreaming.runtime.remote_call import RemoteCallMst
import logging

logger = logging.getLogger(__name__)


class UnitedDistributedControllerActorAdapter(ABC):
    """
    UnitedDistributedControllerActorAdapter is the operator interface
    of UDC, which is responsible for executing specific actions for
    the different phases.
    """

    def __init__(self):
        self.master_actor: ActorHandle = None

    def set_master_actor(self, master_actor: ActorHandle):
        self.master_actor = master_actor

    def get_master_actor(self):
        return self.master_actor

    @abstractmethod
    def on_prepare(self, msg: ControlMessage) -> bool:
        """
        Prepare phase, which will be invoked once the specific
        UDC controller generated a control message.
        Note: This is a sync operation!

        Args:
            msg: The control message generated from the specific
            UDC controller.

        Returns:
            A flag that indicates whether the prepared action
            was successful or not.
        """
        pass

    @abstractmethod
    def on_commit(self):
        """Commit phase, which will be invoked by the UDC lifetime manager
            after all the preparation phases are successfully done.
            Note: This is a async operation! User should invoke the
            `commit_callback` to notify JobMaster that commit has done.
        """
        pass

    @abstractmethod
    def on_disposed(self) -> bool:
        """Dispose phase, which will be invoked by the UDC lifetime manager
            after all the committed phases
            are successfully done.

        Returns:
            A flag that indicates whether the prepared action
            had been taken or not.
        """
        pass

    @abstractmethod
    def on_cancel(self) -> bool:
        """Dispose phase, which will be invoked by the UDC lifetime manager
            after all the committed phases are successfully done.

        Returns:
            A flag that indicates whether the prepared action
            was successful or not.
        """
        pass

    def __commit_to_master(self, control_msg: ControlMessage):
        assert self.get_master_actor() is not None, \
            "Master actor handler can't be null when committing to JobMaster!"
        actor_id = ray.get_runtime_context().actor_id
        report = WorkerUnitedDistributedControllerCommitReport(
            actor_id.binary(), control_msg)
        RemoteCallMst.report_job_worker_udc_commit(self.get_master_actor(),
                                                   report)

    def __commit_callback_bool(self, commit_result: bool) -> None:
        control_msg = ControlMessage()
        control_msg.commit_result = commit_result
        self.__commit_to_master(control_msg)

    def __commit_callback_control_message(self, control_msg: ControlMessage):
        self.__commit_to_master(control_msg)

    def commit_callback(self, msg):
        logger.info("Commit callback was invoked actively.")
        if isinstance(msg, ControlMessage):
            self.__commit_callback_control_message(msg)
        elif isinstance(msg, bool):
            self.__commit_callback_bool(msg)
        else:
            raise TypeError("The return type of the 'on_commit' function \
                            must be either 'bool' or 'ControlMessage'")
