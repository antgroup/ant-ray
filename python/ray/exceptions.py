import os
from traceback import format_exception

from typing import Union

import ray.cloudpickle as pickle
from ray.core.generated.common_pb2 import RayException, Language, PYTHON
from ray.core.generated.common_pb2 import ActorDiedErrorContext
from ray._raylet import ActorID
import colorama
import setproctitle


class RayError(Exception):
    """Super class of all ray exception types."""

    def to_bytes(self):
        # Extract exc_info from exception object.
        exc_info = (type(self), self, self.__traceback__)
        formatted_exception_string = "\n".join(format_exception(*exc_info))
        return RayException(
            language=PYTHON,
            serialized_exception=pickle.dumps(self),
            formatted_exception_string=formatted_exception_string
        ).SerializeToString()

    @staticmethod
    def from_bytes(b):
        ray_exception = RayException()
        ray_exception.ParseFromString(b)
        return RayError.from_ray_exception(ray_exception)

    @staticmethod
    def from_ray_exception(ray_exception):
        if ray_exception.language == PYTHON:
            try:
                return pickle.loads(ray_exception.serialized_exception)
            except Exception as e:
                msg = "Failed to unpickle serialized exception"
                raise RuntimeError(msg) from e
        else:
            return CrossLanguageError(ray_exception)


class CrossLanguageError(RayError):
    """Raised from another language."""

    def __init__(self, ray_exception):
        super().__init__("An exception raised from {}:\n{}".format(
            Language.Name(ray_exception.language),
            ray_exception.formatted_exception_string))


class TaskCancelledError(RayError):
    """Raised when this task is cancelled.

    Attributes:
        task_id (TaskID): The TaskID of the function that was directly
            cancelled.
    """

    def __init__(self, task_id=None):
        self.task_id = task_id

    def __str__(self):
        if self.task_id is None:
            return "This task or its dependency was cancelled by"
        return "Task: " + str(self.task_id) + " was cancelled"


class RayTaskError(RayError):
    """Indicates that a task threw an exception during execution.

    If a task throws an exception during execution, a RayTaskError is stored in
    the object store for each of the task's outputs. When an object is
    retrieved from the object store, the Python method that retrieved it checks
    to see if the object is a RayTaskError and if it is then an exception is
    thrown propagating the error message.
    """

    def __init__(self,
                 function_name,
                 traceback_str,
                 cause,
                 proctitle=None,
                 pid=None,
                 ip=None):
        """Initialize a RayTaskError."""
        import ray
        # BaseException implements a __reduce__ method that returns
        # a tuple with the type and the value of self.args.
        # https://stackoverflow.com/a/49715949/2213289
        self.args = (function_name, traceback_str, cause, proctitle, pid, ip)
        if proctitle:
            self.proctitle = proctitle
        else:
            self.proctitle = setproctitle.getproctitle()
        self.pid = pid or os.getpid()
        self.ip = ip or ray.util.get_node_ip_address()
        self.function_name = function_name
        self.traceback_str = traceback_str
        # TODO(edoakes): should we handle non-serializable exception objects?
        self.cause = cause
        assert traceback_str is not None

    def as_instanceof_cause(self):
        """Returns an exception that is an instance of the cause's class.

        The returned exception will inherit from both RayTaskError and the
        cause class and will contain all of the attributes of the cause
        exception.
        """

        cause_cls = self.cause.__class__
        if issubclass(RayTaskError, cause_cls):
            return self  # already satisfied

        if issubclass(cause_cls, RayError):
            return self  # don't try to wrap ray internal errors

        error_msg = str(self)

        class cls(RayTaskError, cause_cls):
            def __init__(self, cause):
                self.cause = cause
                # BaseException implements a __reduce__ method that returns
                # a tuple with the type and the value of self.args.
                # https://stackoverflow.com/a/49715949/2213289
                self.args = (cause, )

            def __getattr__(self, name):
                return getattr(self.cause, name)

            def __str__(self):
                return error_msg

        name = f"RayTaskError({cause_cls.__name__})"
        cls.__name__ = name
        cls.__qualname__ = name

        return cls(self.cause)

    def __str__(self):
        """Format a RayTaskError as a string."""
        lines = self.traceback_str.strip().split("\n")
        out = []
        in_worker = False
        for line in lines:
            if line.startswith("Traceback "):
                out.append(f"{colorama.Fore.CYAN}"
                           f"{self.proctitle}"
                           f"{colorama.Fore.RESET} "
                           f"(pid={self.pid}, ip={self.ip})")
            elif in_worker:
                in_worker = False
            elif "ray/worker.py" in line or "ray/function_manager.py" in line:
                in_worker = True
            else:
                out.append(line)
        return "\n".join(out)


class WorkerCrashedError(RayError):
    """Indicates that the worker died unexpectedly while executing a task."""

    def __init__(self, error_msg=None) -> None:
        self.error_msg = error_msg

    def __str__(self):
        if self.error_msg is None:
            return (
                "The worker died unexpectedly while executing this task. "
                "Check python-core-worker-*.log files for more information.")
        return self.error_msg


class RayActorError(RayError):
    """Indicates that the actor died unexpectedly before finishing a task.

    This exception could happen either because the actor process dies while
    executing a task, or because a task is submitted to a dead actor.

    If the actor is dead because of an exception thrown in its creation tasks,
    RayActorError will contain the creation_task_error, which is used to
    reconstruct the exception on the caller side.

    cause: The cause of the actor error. `RayTaskError` type means
        the actor has died because of an exception within `__init__`.
        `ActorDiedErrorContext` means the actor has died because of
        unexepected system error. None means the cause is not known.
        Theoretically, this should not happen,
        but it is there as a safety check.
    """

    def __init__(self,
                 cause: Union[RayTaskError, ActorDiedErrorContext] = None):
        # -- If the actor has failed in the middle of __init__, this is set. --
        self._actor_init_failed = False
        # -- The base actor error message. --
        self.base_error_msg = (
            "The actor died unexpectedly before finishing this task.")

        if not cause:
            self.error_msg = self.base_error_msg
        elif isinstance(cause, RayTaskError):
            self._actor_init_failed = True
            self.error_msg = ("The actor died because of an error"
                              " raised in its creation task, "
                              f"{cause.__str__()}")
        else:
            # Inidicating system-level actor failures.
            assert isinstance(cause, ActorDiedErrorContext)
            error_msg_lines = [self.base_error_msg]
            error_msg_lines.append(f"\tclass_name: {cause.class_name}")
            error_msg_lines.append(
                f"\tactor_id: {ActorID(cause.actor_id).hex()}")
            # Below items are optional fields.
            if cause.pid != 0:
                error_msg_lines.append(f"\tpid: {cause.pid}")
            if cause.name != "":
                error_msg_lines.append(f"\tname: {cause.name}")
            if cause.ray_namespace != "":
                error_msg_lines.append(f"\tnamespace: {cause.ray_namespace}")
            if cause.node_ip_address != "":
                error_msg_lines.append(f"\tip: {cause.node_ip_address}")
            error_msg_lines.append(cause.error_message)
            if cause.never_started:
                error_msg_lines.append(
                    "The actor never ran - it was cancelled before it started running."  # noqa
                )
            self.error_msg = "\n".join(error_msg_lines)

    @property
    def actor_init_failed(self) -> bool:
        return self._actor_init_failed

    def __str__(self) -> str:
        return self.error_msg

    @staticmethod
    def from_task_error(task_error: RayTaskError):
        return RayActorError(task_error)


class RaySystemError(RayError):
    """Indicates that Ray encountered a system error.

    This exception can be thrown when the raylet is killed.
    """

    def __init__(self, client_exc, traceback_str=None):
        self.client_exc = client_exc
        self.traceback_str = traceback_str

    def __str__(self):
        error_msg = f"System error: {self.client_exc}"
        if self.traceback_str:
            error_msg += f"\ntraceback: {self.traceback_str}"
        return error_msg


class NotImplementError(RayError):
    """Indicates that the interface is not implemented or unsupported yet."""
    pass


class ObjectStoreFullError(RayError):
    """Indicates that the object store is full.

    This is raised if the attempt to store the object fails
    because the object store is full even after multiple retries.
    """

    def __str__(self):
        return super(ObjectStoreFullError, self).__str__() + (
            "\n"
            "The local object store is full of objects that are still in "
            "scope and cannot be evicted. Tip: Use the `ray memory` command "
            "to list active objects in the cluster.")


class ObjectLostError(RayError):
    """Indicates that an object has been lost due to node failure.

    Attributes:
        object_ref_hex: Hex ID of the object.
    """

    def __init__(self, object_ref_hex):
        self.object_ref_hex = object_ref_hex

    def __str__(self):
        return (f"Object {self.object_ref_hex} is lost due to node failure.")


class GetTimeoutError(RayError):
    """Indicates that a call to the worker timed out."""
    pass


class PlasmaObjectNotAvailable(RayError):
    """Called when an object was not available within the given timeout."""
    pass


class AsyncioActorExit(RayError):
    """Raised when an asyncio actor intentionally exits via exit_actor()."""
    pass


class RuntimeEnvSetupError(RayError):
    """Raised when a runtime environment fails to be set up.

    params:
        error_message: The error message that explains
            why runtime env setup has failed.
    """

    def __init__(self, error_message: str = None):
        self.error_message = error_message

    def __str__(self):
        msgs = ["Failed to set up runtime environment."]
        if self.error_message:
            msgs.append(self.error_message)
        return "\n".join(msgs)


class TaskUnschedulableError(RayError):
    """Raised when the task cannot be scheduled.
    One example is that the node specified through
    NodeAffinitySchedulingStrategy is dead.
    """

    def __init__(self, error_message: str):
        self.error_message = error_message

    def __str__(self):
        return f"The task is not schedulable: {self.error_message}"


class ActorUnschedulableError(RayError):
    """Raised when the actor cannot be scheduled.
    One example is that the node specified through
    NodeAffinitySchedulingStrategy is dead.
    """

    def __init__(self, error_message: str):
        self.error_message = error_message

    def __str__(self):
        return f"The actor is not schedulable: {self.error_message}"


RAY_EXCEPTION_TYPES = [
    PlasmaObjectNotAvailable,
    RayError,
    RayTaskError,
    WorkerCrashedError,
    RayActorError,
    ObjectStoreFullError,
    ObjectLostError,
    GetTimeoutError,
    AsyncioActorExit,
    RuntimeEnvSetupError,
    TaskUnschedulableError,
    ActorUnschedulableError,
]
