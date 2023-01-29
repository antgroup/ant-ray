import enum
import json
import logging
import threading
import time
import traceback
import sys
from queue import Queue

import ray
import pickle
import raystreaming.runtime.processor as processor
from ray.actor import ActorHandle
from raystreaming.config import Config
from raystreaming.constants import StreamingConstants
from raystreaming.generated import remote_call_pb2
from raystreaming.utils import EnvUtil
from raystreaming.runtime.injector import ExceptionInjector
from raystreaming.runtime.command import (
    WorkerCommitReport,
    WorkerRollbackRequest,
    WorkerReceivePartialBarrierReport,
)
from raystreaming.runtime.control_message import (
    ControlMessageType,
    ControlMessage,
)
from raystreaming.runtime.failover import Barrier, PartialBarrier
from raystreaming.runtime.graph import ExecutionVertexContext
from raystreaming.runtime.metrics.worker_metrics import WorkerMetrics
from raystreaming.runtime.remote_call import CallResult, RemoteCallMst
from raystreaming.runtime.state_backend import StateBackendFactory
from raystreaming.runtime.task import SourceStreamTask, \
    OneInputStreamTask, TwoInputStreamTask, MultiInputStreamTask
from raystreaming.runtime.transfer import channel_id_bytes_to_str
from raystreaming.utils import read_control_message

logger = logging.getLogger(__name__)

# special flag to indicate this actor not ready
_NOT_READY_FLAG_ = b" " * 4


@ray.remote(max_restarts=-1)
class MockJobMaster:
    def request_rollback(self, rollback_commit):
        pass


class JobWorkerInstance:
    """A streaming job worker is used to execute user-defined function.
     """
    pass
    """
     Attributes:
         master_actor: Optional[ActorHandle], interact with `JobMaster`
         worker_context: Optional[remote_call_pb2.PythonJobWorkerContext]
         execution_vertex_context: Optional[ExecutionVertexContext]
         __need_rollback: bool"""

    def __init__(self):
        logger.info("Create job worker.")
        self.worker_context = None
        self.execution_vertex_context = None
        self.config = None
        self.task_id = None
        self.task = None
        self.rollback_cnt = 0
        self.stream_processor = None
        self.__state = WorkerState()
        self.__need_rollback = True
        self.mail_box = Queue()
        self.initial_state_lock = threading.Lock()
        self.worker_metric = None
        self.was_reconstructed = ray.get_runtime_context(
        ).was_current_actor_reconstructed

    def set_annotated_methods(self, **methods):
        """This method run before `init`"""
        self.annotated_methods = methods
        logger.info(f"set annotated methods {methods}")

    def init(self, worker_config_str):
        if self.__state.get_type() != StateType.NONE:
            logger.info("Skip duplicated init for py worker.")
            return True
        self.worker_config = json.loads(worker_config_str)
        self.state_backend = StateBackendFactory.get_state_backend(
            self.worker_config.get(
                StreamingConstants.CP_STATE_BACKEND_TYPE,
                StreamingConstants.CP_STATE_BACKEND_DEFAULT))
        self.state_backend.init(self.worker_config)

        if not self.maybe_load_context():
            return False

        logger.info("Job worker init success.")
        self.__state.set_type(StateType.INIT)
        return True

    def maybe_load_context(self):
        if self.was_reconstructed:
            try:
                logger.info("Loading the job worker immutable context "
                            "when restarting the actor.")
                immutable_context_bytes = self.__load_immutable_context()
                if immutable_context_bytes is not None and \
                        immutable_context_bytes.__len__() > 0:
                    immutable_context = pickle.loads(immutable_context_bytes)
                    self.worker_context = JobWorkerContext()
                    self.worker_context.init_immutable_context(
                        immutable_context)
                    self.request_rollback(
                        "Request to rollback when the actor is restarted.")
                else:
                    logger.error(
                        "The loaded job worker immutable context is None.")
                    return False
            except Exception:
                logger.error("Loading the job worker immutable context error.")
                return False
        return True

    def register_context(self, ctx_bytes):
        logger.info("Start to register context.")

        is_first_register = self.execution_vertex_context is None
        new_context = JobWorkerContext(ctx_bytes)
        new_vertex_context = new_context.get_execution_vertex_context()

        if not is_first_register:
            if new_context.is_changed:
                logger.info("Update worker context for workerId: {}.".format(
                    new_context.worker_id))
                return self.update_context(new_context)
            else:
                logger.warn(
                    "Job worker context is already exists and not changed. "
                    "workerId: {}.".format(new_context.worker_id))
                return False

        self.worker_context = new_context
        self.execution_vertex_context = new_vertex_context
        self.config = self.execution_vertex_context.config

        # init metric client
        if not self.worker_metric:
            self.worker_metric = WorkerMetrics(
                self.config[Config.STREAMING_JOB_NAME],
                self.execution_vertex_context.execution_job_vertex_name,
                self.worker_context.worker_id,
                self.execution_vertex_context.actor_id.hex(), self.config)
            self.worker_metric.open(self.execution_vertex_context.vertex_type)

        if not self.was_reconstructed and is_first_register:
            logger.info(
                "Saving the job worker context when first registering context."
            )
            self.save_global_context(0)
            self.__save_immutable_context()
            logger.info("Saved the job worker context.")

        logger.info("Finish to register context: {}.".format(
            self.worker_context))
        return True

    def apply_new_context(self):
        if self.worker_context.is_changed():
            self.reset_context_to_unchanged()
            self.save_global_context(self.task.last_checkpoint_id)
            logger.info("Applied the new context.")
            return True

        logger.warning("Skip applying the new context "
                       "for context is not changed.")
        return False

    def insert_control_message(self, message):
        if message.message_type == ControlMessageType.OPERATOR_COMMAND:
            logger.info("Got an operator command, call operator directly.")
            return self.forward_command(message)
        logger.info("Insert a new control message {}".format(
            message.message_type))
        self.mail_box.put(message)

    def check_processor(func):
        def wrapper(self, *args, **kwargs):
            if self.stream_processor is None:
                raise RuntimeError("This processor is null.")
            return func(self, *args, **kwargs)

        return wrapper

    @check_processor
    def forward_command(self, message):
        return self.stream_processor.forward_command(message)

    # United Distributed Controller function start.

    @read_control_message
    @check_processor
    def udc_prepare(self, udc_msg):
        logger.info("worker udc prepare.")
        return self.stream_processor \
            .get_operator().on_prepare(udc_msg)

    @check_processor
    def udc_commit(self):
        return self.stream_processor \
            .get_operator().on_commit()

    @check_processor
    def udc_disposed(self):
        return self.stream_processor \
            .get_operator().on_disposed()

    @check_processor
    def udc_cancel(self):
        return self.stream_processor \
            .get_operator().on_cancel()

    # United Distributed Controller function end.

    def update_context_internal(self, new_context):
        logger.info("Worker {} update context internal.".format(
            new_context.worker_id))
        self.worker_context = new_context
        self.execution_vertex_context = \
            new_context.get_execution_vertex_context()

    def update_context(self, new_context):
        if self.worker_context is not None and new_context.is_changed:
            logger.info("Insert update context control message into mailbox.")
            control_message = ControlMessage(ControlMessageType.UPDATE_CONTEXT,
                                             new_context)
            self.insert_control_message(control_message)
        return True

    def create_stream_task(self, checkpoint_id):
        # use vertex id as task id
        self.task_id = self.execution_vertex_context.get_vertex_id()

        # build and get processor from operator
        operator = self.execution_vertex_context.stream_operator
        self.stream_processor = processor.build_processor(operator)

        if isinstance(self.stream_processor, processor.SourceProcessor):
            return SourceStreamTask(self.task_id, self.stream_processor, self,
                                    checkpoint_id)
        elif isinstance(self.stream_processor, processor.OneInputProcessor):
            return OneInputStreamTask(self.task_id, self.stream_processor,
                                      self, checkpoint_id)
        elif isinstance(self.stream_processor, processor.TwoInputProcessor):
            assert len(
                self.execution_vertex_context.right_input_vertex_ids) == 1
            return TwoInputStreamTask(
                self.task_id, self.stream_processor, self, checkpoint_id,
                self.execution_vertex_context.left_input_vertex_id,
                self.execution_vertex_context.right_input_vertex_ids[0])
        elif isinstance(self.stream_processor, processor.MultiInputProcessor):
            assert self.execution_vertex_context. \
                       right_input_vertex_ids is not None, \
                       "Right input vertex ids can't be null " \
                       "when constructing multiple input processor."
            return MultiInputStreamTask(
                self.task_id, self.stream_processor, self, checkpoint_id,
                self.execution_vertex_context.right_input_vertex_ids)
        else:
            raise Exception(f"Unsupported processor type: "
                            f"{self.stream_processor}")

    def rollback(self, checkpoint_id_bytes, partial_checkpoint_id_bytes):
        if not hasattr(self, "worker_config"):
            msg = "Worker isn't been inited. Try to wait a second and call"
            " again."
            logger.info(msg)
            return self.__gen_call_result(CallResult.fail(msg=msg))

        checkpoint_id_pb = remote_call_pb2.CheckpointId()
        checkpoint_id_pb.ParseFromString(checkpoint_id_bytes)
        checkpoint_id = checkpoint_id_pb.checkpoint_id

        # TODO
        partial_checkpoint_id = 0

        if not self.was_reconstructed:
            self.save_global_context(checkpoint_id)

        logger.info("Start rollback, checkpoint_id={}".format(checkpoint_id))
        # skip useless rollback
        self.initial_state_lock.acquire()
        try:
            if self.task is not None and self.task.thread.is_alive(
            ) and checkpoint_id == self.task.last_checkpoint_id and \
                    self.task.is_initial_state:
                logger.info(
                    "Task is already in initial state, skip this rollback.")
                return self.__gen_call_result(
                    CallResult.skipped(
                        "Task is already in initial state, skip this rollback."
                    ))
        finally:
            self.initial_state_lock.release()

        self.rollback_cnt += 1
        # restart task
        try:
            if self.worker_context.ctx_bytes is None:
                load_context_result = self.load_and_register_context(
                    checkpoint_id, partial_checkpoint_id)
                if not load_context_result:
                    logger.error("Failed to load context when rollback.")
                    return False

            if self.task is not None:
                # make sure the runner is closed
                logger.info("Start to cancel stream task.")
                self.task.cancel_task()
                del self.task
                logger.info("Stream task canceled.")
            self.task = self.create_stream_task(checkpoint_id)
            q_recover_info = self.task.recover(self.was_reconstructed
                                               or self.rollback_cnt > 1)
            self.__state.set_type(StateType.RUNNING)
            self.__need_rollback = False
            logger.info(
                "Rollback success, checkpoint is {}, qRecoverInfo is {}.".
                format(checkpoint_id, q_recover_info))
            return self.__gen_call_result(CallResult.success(q_recover_info))
        except Exception:
            actor_id = self.execution_vertex_context.actor_id.hex()
            ray.report_event(
                ray.EventSeverity.ERROR, "ROLLBACK",
                f"Python worker rollback failed, actor id is {actor_id},"
                f" checkpoint id is {checkpoint_id},"
                f" exception is:\n{traceback.format_exc()}")
            logger.exception("Rollback has exception.")
            return self.__gen_call_result(
                CallResult.fail(msg=traceback.format_exc()))

    def on_reader_message(self, *buffers):
        """Called by upstream queue writer to send data message to downstream
        queue reader.
        """
        if self.task is None or self.task.reader is None:
            logger.info("task is None, skip reader transfer")
            return
        self.task.reader.on_reader_message(*buffers)

    def on_reader_message_sync(self, buffer: bytes):
        """Called by upstream queue writer to send control message to
         downstream downstream queue reader.
        """
        if self.task is None or self.task.reader is None:
            logger.info("task is None, skip reader transfer")
            return _NOT_READY_FLAG_
        return self.task.reader.on_reader_message_sync(buffer)

    def on_writer_message(self, buffer: bytes):
        """Called by downstream queue reader to send notify message to
        upstream queue writer.
        """
        if self.task is None or self.task.writer is None:
            logger.info("task is None, skip writer transfer")
            return
        self.task.writer.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        """Called by downstream queue reader to send control message to
        upstream queue writer.
        """
        if self.task is None or self.task.writer is None:
            return _NOT_READY_FLAG_
        return self.task.writer.on_writer_message_sync(buffer)

    def health_check(self):
        logger.debug("health check.")
        return self.get_worker_runtime_info()

    def task_health_check(self):
        logger.debug("Task health check.")
        return True if self.task is not None and \
                       self.stream_processor is not None and \
                       self.__state.get_type() == StateType.RUNNING else False

    def destroy(self):
        logger.info("Destroying python job worker.")
        if self.task:
            self.task.cancel_task()
        if self.stream_processor:
            self.stream_processor.close()
        return True

    def shutdown(self):
        logger.info("Python worker shutdown.")
        self.destroy()
        sys.exit(0)

    def shutdown_without_reconstruction(self):
        logger.info("Python worker shutdown without reconstruction.")
        self.destroy()
        ray.actor.exit_actor()

    def notify_checkpoint_timeout(self, checkpoint_id_bytes):
        checkpoint_id = self.__parse_to_checkpoint_id(checkpoint_id_bytes)
        # TODO(lingxuan.zlx): remove timeout state.
        return self.__clear_global_context(checkpoint_id)

    def commit(self, barrier_bytes):
        barrier_pb = remote_call_pb2.Barrier()
        barrier_pb.ParseFromString(barrier_bytes)
        barrier = Barrier(barrier_pb.id)
        logger.info("Receive trigger, barrier is {}.".format(barrier))

        if self.task is not None:
            self.task.commit_trigger(barrier)
        return True

    def commit_barrier(self, checkpoint_id: int):
        logger.info("Worker {} commit barrier, checkpoint id {}.".format(
            self.execution_vertex_context.exe_vertex_name, checkpoint_id))
        report = WorkerCommitReport(
            self.execution_vertex_context.actor_id.binary(), checkpoint_id)
        RemoteCallMst.report_job_worker_commit(self.master_actor, report)

    def commit_partial_barrier(self, checkpoint_id, partial_checkpoint_id):
        logger.info("Worker {} commit partial barrier: {}.".format(
            self.execution_vertex_context.exe_vertex_name,
            str(checkpoint_id) + "-" + str(partial_checkpoint_id)))
        report = WorkerReceivePartialBarrierReport(
            self.execution_vertex_context.actor_id.binary(), checkpoint_id,
            partial_checkpoint_id)
        RemoteCallMst.report_job_worker_partial_barrier(
            self.master_actor, report)

    def clear_expired_cp(self, state_checkpoint_id_bytes,
                         queue_checkpoint_id_bytes,
                         partial_checkpoint_id_bytes):
        state_checkpoint_id = self.__parse_to_checkpoint_id(
            state_checkpoint_id_bytes)
        queue_checkpoint_id = self.__parse_to_checkpoint_id(
            queue_checkpoint_id_bytes)
        logger.info("Start to clear expired checkpoint, checkpoint_id={}"
                    ", queue_checkpoint_id={}, exe_vertex_name={}.".format(
                        state_checkpoint_id, queue_checkpoint_id,
                        self.execution_vertex_context.exe_vertex_name))

        ret = self.__clear_expired_cp_state(state_checkpoint_id) \
            if state_checkpoint_id > 0 else True
        ret &= self.__clear_expired_queue_msg(state_checkpoint_id,
                                              queue_checkpoint_id)
        ret &= self.__clear_global_context(state_checkpoint_id)
        logger.info(
            "Clear expired checkpoint done, result={}, checkpoint_id={}"
            ", queue_checkpoint_id={}, exe_vertex_name={}.".format(
                ret, state_checkpoint_id, queue_checkpoint_id,
                self.execution_vertex_context.exe_vertex_name))
        return ret

    def clear_partial_checkpoint(self, checkpoint_id_bytes,
                                 partial_checkpoint_id_bytes):
        self.reset_context_to_unchanged()

        global_checkpoint_id = self.__parse_to_checkpoint_id(
            checkpoint_id_bytes)
        partial_checkpoint_id = self.__parse_to_checkpoint_id(
            partial_checkpoint_id_bytes)
        logger.info("Start to clear partial checkpoint for task.")
        if self.task:
            return self.task.clear_partial_checkpoint(global_checkpoint_id,
                                                      partial_checkpoint_id)
        return True

    def reset_context_to_unchanged(self):
        logger.info("Reset job worker context to unchanged.")
        self.worker_context.mark_as_unchanged()
        self.worker_context.set_role_in_changed_sub_dag(
            remote_call_pb2.OperatorType.TRANSFORM)

    def suspend(self):
        logger.info("Start to suspend the worker.")
        if self.task:
            self.task.suspend()
        return True

    def resume(self):
        logger.info("Start to resume the worker.")
        if self.task:
            self.task.resume()
        return True

    def broadcast_partial_barrier(self, partial_barrier_bytes):
        partial_barrier_pb = remote_call_pb2.PartialBarrier()
        partial_barrier_pb.ParseFromString(partial_barrier_bytes)
        partial_barrier = PartialBarrier(
            partial_barrier_pb.global_checkpoint_id,
            partial_barrier_pb.partial_checkpoint_id)
        logger.info("Received a partial barrier: {}.".format(partial_barrier))
        message = ControlMessage(ControlMessageType.BROADCAST_PARTIAL_BARRIER,
                                 partial_barrier)
        self.insert_control_message(message)
        return True

    def is_ready_rescaling(self):
        if self.task is None:
            logger.error("This task is null.")
            return False
        if self.stream_processor is None:
            logger.error("This processor is null.")
            return False
        return self.stream_processor \
            .get_operator().is_ready_rescaling()

    def fetch_metrics(self):
        # NOTE(lingxuan.zlx): master might fetch worker metrics before
        # its state has been created.
        if self.worker_metric is not None:
            return self.worker_metric.query_metrics().SerializeToString()

    def check_if_need_rollback(self):
        return self.__need_rollback

    def inject_exception(self, exception_injector_bytes):
        injector_pb = remote_call_pb2.ExecptionInjector()
        injector_pb.ParseFromString(exception_injector_bytes)
        injector_exception = ExceptionInjector(
            injector_pb.inject_exception_type)

        message = ControlMessage(ControlMessageType.EXCEPTION_INJECTION,
                                 injector_exception)
        self.insert_control_message(message)
        return True

    def load_and_register_context(self, global_checkpoint_id,
                                  partial_checkpoint_id):
        try:
            context_bytes = self.__load_global_context(global_checkpoint_id)
            if context_bytes is not None and context_bytes.__len__() > 0:
                old_context = pickle.loads(context_bytes)
                return self.register_context(old_context.context_bytes)
            else:
                logger.error(
                    "Job worker context loading from backend is null.")
        except Exception as e:
            logger.error(
                "Loading job worker context from backend error, {}".format(e))
        return False

    def __save_immutable_context(self):
        if self.worker_context is not None:
            immutable_context = ImmutableContext(
                self.worker_context.master_actor, self.worker_context.actor_id)
            immutable_context_bytes = pickle.dumps(immutable_context)
            immutable_context_key = self.__gen_immutable_context_key()
            logger.info("Saving the worker immutable context, key is {}, "
                        "bytes length is {}.".format(
                            immutable_context_key,
                            immutable_context_bytes.__len__()))
            try:
                self.state_backend.put(immutable_context_key,
                                       immutable_context_bytes)
            except BaseException as e:
                logger.error(
                    "Failed to save worker immutable context. {}".format(e))
                return False
            return True
        else:
            logger.error(
                "Failed to save worker immutable context by empty context.")
            return False

    def save_global_context(self, checkpoint_id):
        if self.worker_context is not None:
            context_bytes = pickle.dumps(self.worker_context)
            global_context_key = self.__gen_global_context_key(checkpoint_id)
            logger.info("Saving the worker global context, key is {}, "
                        "bytes length is {}.".format(global_context_key,
                                                     context_bytes.__len__()))
            try:
                self.state_backend.put(global_context_key, context_bytes)
            except BaseException as e:
                logger.error("Failed to save worker global context, "
                             "checkpoint_id: {}. {}".format(checkpoint_id, e))
                return False
            return True
        else:
            logger.error(
                "Failed to save worker global context for empty context, "
                "checkpoint_id: {}.".format(checkpoint_id))
            return False

    def __clear_global_context(self, checkpoint_id):
        """ Remove global context related after checkpoint exipred.
        """
        if hasattr(self, "state_backend") and self.state_backend is not None:
            global_context_key = self.__gen_global_context_key(checkpoint_id)
            logger.info("Removing the worker global context, key is {}, "
                        .format(global_context_key))
            try:
                self.state_backend.remove(global_context_key)
            except BaseException as e:
                logger.error("Failed to remove worker global context, "
                             "checkpoint_id: {}. {}".format(checkpoint_id, e))
                return False
            return True
        else:
            logger.error(
                "Failed to clear worker global context for empty context, "
                "checkpoint_id: {}.".format(checkpoint_id))
            return False

    def save_partial_context(self, global_checkpoint_id,
                             partial_checkpoint_id):
        # TODO(lingxuan.zlx):  save partial context for rescale rollback.
        return True

    def clear_partial_context(self, global_checkpoint_id,
                              partial_checkpoint_id):
        # TODO(lingxuan.zlx):  clear partial context after rescaling finished.
        return True

    def __load_immutable_context(self):
        context_key = self.__gen_immutable_context_key()
        logger.info(
            "Loading the job worker immutable context, key is {}.".format(
                context_key))
        return self.state_backend.get(context_key)

    def __load_global_context(self, global_checkpoint_id):
        context_key = self.__gen_global_context_key(global_checkpoint_id)
        logger.info("Loading the job worker global context, "
                    "global checkpoint id is {}, "
                    "key is {}.".format(global_checkpoint_id, context_key))
        return self.state_backend.get(context_key)

    def rescale_rollback(self, partial_checkpoint_id_bytes):
        partial_checkpoint_id_pb = remote_call_pb2.CheckpointId()
        partial_checkpoint_id_pb.ParseFromString(partial_checkpoint_id_bytes)
        partial_checkpoint_id = partial_checkpoint_id_pb.checkpoint_id
        message = ControlMessage(ControlMessageType.RESCALE_ROLLBACK,
                                 partial_checkpoint_id)
        self.insert_control_message(message)
        return True

    def request_rollback(self, exception_msg="Python exception."):
        logger.info("Request rollback, actor id: {}.".format(
            self.worker_context.get_actor_id()))

        self.__need_rollback = True

        request_ret = False
        for i in range(StreamingConstants.REQUEST_ROLLBACK_RETRY_TIMES):
            logger.info("request rollback {} time".format(i))
            try:
                request_ret = RemoteCallMst.request_job_worker_rollback(
                    self.worker_context.get_master_actor(),
                    WorkerRollbackRequest(
                        self.worker_context.get_actor_id().binary(),
                        "Exception msg=%s, retry time=%d." % (exception_msg,
                                                              i)))
            except Exception:
                logger.exception("Unexpected error when rollback")
            logger.info("request rollback {} time, ret={}".format(
                i, request_ret))
            if not request_ret:
                logger.warning(
                    "Request rollback return false, maybe it's invalid"
                    " request, try to sleep 1s.")
                time.sleep(1)
            else:
                break
        if not request_ret:
            logger.warning("Request failed after retry {} times, \
                now worker shutdown without reconstruction.".format(
                StreamingConstants.REQUEST_ROLLBACK_RETRY_TIMES))
            self.shutdown_without_reconstruction()

        self.__state.set_type(StateType.WAIT_ROLLBACK)

    def __clear_expired_cp_state(self, checkpoint_id):
        if self.__need_rollback:
            logger.warning(
                "Need rollback, skip clear_expired_cp_state, checkpoint id: {}"
                .format(checkpoint_id))
            return False

        logger.info("Clear expired checkpoint state, cp id is {}.".format(
            checkpoint_id))

        if self.task is not None:
            self.task.clear_expired_cp_state(checkpoint_id)
        return True

    def __clear_expired_queue_msg(self, state_cp_id, queue_cp_id):
        if self.__need_rollback:
            logger.warning(
                "Need rollback, skip clear_expired_queue_msg"
                ", state checkpoint id: {} queue checkpoint id: {}".format(
                    state_cp_id, queue_cp_id))
            return False

        logger.info("Clear expired queue msg, state checkpoint id: {} queue"
                    " checkpoint id is {}.".format(state_cp_id, queue_cp_id))

        if self.task is not None:
            self.task.clear_expired_queue_msg(state_cp_id, queue_cp_id)
        return True

    def __parse_to_checkpoint_id(self, checkpoint_id_bytes):
        checkpoint_id_pb = remote_call_pb2.CheckpointId()
        checkpoint_id_pb.ParseFromString(checkpoint_id_bytes)
        return checkpoint_id_pb.checkpoint_id

    def __add_query_metric(self, metric_result_pb, metric_category,
                           metric_name, metric_value, host, job_name, op_name,
                           pid, id, worker_name):
        dump = metric_result_pb.dumps.add()
        dump.category = metric_category
        dump.name = metric_name
        dump.value = str(metric_value)
        dump.query_scope.host = host
        dump.query_scope.job_name = job_name
        dump.query_scope.op_name = op_name
        dump.query_scope.pid = str(pid)
        dump.query_scope.worker_id.id = id
        dump.query_scope.worker_name = worker_name

    def __gen_call_result(self, call_result):
        call_result_pb = remote_call_pb2.CallResult()

        call_result_pb.success = call_result.success
        call_result_pb.result_code = call_result.result_code.value
        if call_result.result_msg is not None:
            call_result_pb.result_msg = call_result.result_msg

        if call_result.result_obj is not None:
            q_recover_info = call_result.result_obj
            for q, status in q_recover_info.get_creation_status().items():
                call_result_pb.result_obj.creation_status[
                    channel_id_bytes_to_str(q)] = status.value

        return call_result_pb.SerializeToString()

    def _gen_unique_key(self, key_prefix):
        return key_prefix \
               + str(self.worker_config.get(StreamingConstants.JOB_NAME)) \
               + "_" + str(self.worker_config.get(
                StreamingConstants.WORKER_ID))

    def __gen_context_key_prefix(self):
        return self._gen_unique_key(StreamingConstants.JOB_WORKER_CONTEXT_KEY)

    def __gen_immutable_context_key(self):
        """
        Generates the job worker context key.
        :return: jobworker_context_{job_name}_{worker_id}
        """
        return self.__gen_context_key_prefix()

    def __gen_global_context_key(self, checkpoint_id):
        """
        Generates the job worker global context key.
        :param checkpoint_id:
        :return: jobworker_context_{job_name}_{worker_id}_
        global_{global_checkpoint_id}
        """
        return "{}{}{}".format(self.__gen_context_key_prefix(),
                               StreamingConstants.GLOBAL_CONTEXT_DELIMITER,
                               checkpoint_id)

    def __gen_partial_context_key(self, global_checkpoint_id,
                                  partial_checkpoint_id):
        """
        Generates the job worker partial context key.
        :param global_checkpoint_id:
        :param partial_checkpoint_id:
        :return: jobworker_context_{job_name}_{worker_id}_
        {global_checkpoint_id}_partial_{partial_checkpoint_id}
        """
        return "{}{}{}{}{}".format(
            self.__gen_context_key_prefix(),
            StreamingConstants.CONTEXT_DELIMITER, global_checkpoint_id,
            StreamingConstants.PARTIAL_CONTEXT_DELIMITER,
            partial_checkpoint_id)

    def get_worker_runtime_info(self):
        worker_runtime_info = WorkerRuntimeInfo()
        try:
            info_result_pb = remote_call_pb2.WorkerRuntimeInfo()

            info_result_pb.healthy = True if self.__state.get_type(
            ) != StateType.NONE else False

            if not info_result_pb.healthy:
                logger.info(
                    "Get worker runtime info: {}.".format(worker_runtime_info))

            info_result_pb.cluster_name = worker_runtime_info.get_cluster_name
            info_result_pb.idcName = worker_runtime_info.get_idc_name
            info_result_pb.pid = worker_runtime_info.get_pid
            info_result_pb.hostname = worker_runtime_info.get_hostname
            info_result_pb.ip_address = worker_runtime_info.get_ip_address

            return info_result_pb.SerializeToString()
        except Exception as e:
            logger.warning(
                "Get worker runtime info occurs an exception: {}".format(e))
            return remote_call_pb2.WorkerRuntimeInfo().SerializeToString()

    def fetch_profiling_infos(self):
        if self.task is None:
            return "{}"
        return self.task.fetch_profiling_infos()

    def close_metric(self):
        self.worker_metric.close()

    @property
    def master_actor(self):
        return self.worker_context.master_actor


@ray.remote(max_restarts=-1)
class JobWorker(JobWorkerInstance):
    pass


class WorkerRuntimeInfo:
    """
    worker runtime info
    """

    def __init__(self):
        self.__cluster_name = EnvUtil.get_cluster_name()
        self.__idc_name = EnvUtil.get_idc_name()
        self.__pid = str(EnvUtil.get_pid())
        self.__hostname = EnvUtil.get_hostname()
        self.__ip_address = EnvUtil.get_ip_address()

    @property
    def get_cluster_name(self):
        return self.__cluster_name

    @property
    def get_idc_name(self):
        return self.__idc_name

    @property
    def get_pid(self):
        return self.__pid

    @property
    def get_hostname(self):
        return self.__hostname

    @property
    def get_ip_address(self):
        return self.__ip_address

    def __repr__(self):
        return "WorkerRuntimeInfo([{0},{1},{2},{3},{4}])" \
            .format(self.__cluster_name, self.__idc_name, self.__pid,
                    self.__hostname, self.__ip_address)


class WorkerState:
    """
    worker state
    """

    def __init__(self):
        self.__type = StateType.NONE

    def set_type(self, type):
        self.__type = type

    def get_type(self):
        return self.__type


class StateType(enum.Enum):
    """
    state type
    """

    # None stands worker has not been initiliazed.
    NONE = 0
    INIT = 1
    RUNNING = 2
    WAIT_ROLLBACK = 3


class ImmutableContext:
    """
    ImmutableContext is the static context of `JobWorker`.
    """

    def __init__(self, master_actor, actor_id):
        self.__master_actor = master_actor
        self.__actor_id = actor_id

    def get_master_actor(self):
        return self.__master_actor

    def get_actor_id(self):
        return self.__actor_id


class JobWorkerContext:
    """
    Global context of `JobWorker`.
    """

    def __init__(self, ctx_bytes=None):
        self.ctx_bytes = None
        self.immutable_context = None
        if ctx_bytes is not None:
            ctx_pb = remote_call_pb2.PythonJobWorkerContext()
            ctx_pb.ParseFromString(ctx_bytes)

            self.ctx_bytes = ctx_bytes
            self.worker_id = ctx_pb.worker_id
            self.master_actor = ActorHandle._deserialization_helper(
                ctx_pb.master_actor)
            self.execution_vertex_context = \
                ExecutionVertexContext(ctx_pb.execution_vertex_context)
            self.is_changed = ctx_pb.is_changed
            self.role_in_changed_sub_dag = ctx_pb.role_in_changed_sub_dag

            self.actor_id = ray.runtime_context.get_runtime_context().actor_id
            self.immutable_context = ImmutableContext(self.master_actor,
                                                      self.actor_id)

    def __getstate__(self):
        return {"ctx_bytes": self.ctx_bytes}

    def __setstate__(self, state):
        self.__init__(state.get("ctx_bytes"))

    def __str__(self):
        return "JobWorkerContext [worker_id:%s, actor_id:%s]" \
               % (self.worker_id, self.actor_id)

    @property
    def context_bytes(self):
        return self.ctx_bytes

    def get_master_actor(self):
        return self.immutable_context.get_master_actor()

    def get_actor_id(self):
        return self.immutable_context.get_actor_id()

    def init_immutable_context(self, immutable_context):
        self.immutable_context = ImmutableContext(
            immutable_context.get_master_actor(),
            immutable_context.get_actor_id())

    def get_execution_vertex_context(self):
        return self.execution_vertex_context

    def get_role_in_changed_sub_dag(self):
        return self.role_in_changed_sub_dag

    def set_role_in_changed_sub_dag(self, role_in_changed_sub_dag):
        self.role_in_changed_sub_dag = role_in_changed_sub_dag

    def is_changed(self):
        return self.is_changed

    def mark_as_changed(self):
        self.is_changed = True

    def mark_as_unchanged(self):
        self.is_changed = False
