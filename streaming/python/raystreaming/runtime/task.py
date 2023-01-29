import logging
import os
import pickle
import threading
import time
import typing
import json
from abc import ABC, abstractmethod
from typing import Optional
import traceback

import ray
from raystreaming.collector import OutputCollector
from raystreaming.config import Config
from raystreaming.constants import StreamingConstants
from raystreaming.context import RuntimeContextImpl
from raystreaming.generated import remote_call_pb2
from raystreaming.runtime import serialization
from raystreaming.runtime.command import EndOfDataReport
from raystreaming.runtime.control_message import ControlMessageType
from raystreaming.runtime.failover import (Barrier, PartialBarrier,
                                           OpCheckpointInfo)
from raystreaming.runtime.remote_call import RemoteCallMst
from raystreaming.runtime.serialization import (
    PythonSerializer,
    CrossLangSerializer,
)
from raystreaming.runtime.transfer import (
    BarrierType,
    ChannelID,
    DataMessage,
    CheckpointBarrier,
    DataReader,
    DataWriter,
    NativeConfigKeys,
    ChannelInterruptException,
    QueueRecoverInfo,
)
from raystreaming.partition import (
    RoundRobinPartition, DynamicRebalancePartition, DynamicRebalanceParam)
from raystreaming.exception import EndOfDataException
from raystreaming.state.utils import convert_operator_state_config

if typing.TYPE_CHECKING:
    from raystreaming.runtime.processor import Processor

logger = logging.getLogger(__name__)


class StreamTask(ABC):
    """Base class for all streaming tasks. Each task runs a processor."""

    def __init__(self, task_id: int, processor: "Processor", worker,
                 last_checkpoint_id: int):
        self.worker_context = worker.worker_context
        self.vertex_context = worker.execution_vertex_context
        self.task_id = task_id
        self.processor = processor
        # worker must be actor instance if it's TidProcessor
        self.worker = worker
        self.config: dict = worker.config
        self.reader: Optional[DataReader] = None
        self.writer: Optional[DataWriter] = None
        self.is_initial_state = True
        self.last_checkpoint_id: int = last_checkpoint_id
        self.thread = threading.Thread(target=self.run, daemon=True)

        self.__outdated_partial_checkpoint_set = set()
        self.suspended: bool = False

        self.metric = self.worker.worker_metric
        self.consumed_count = 0
        self.sample_frequency = int(
            worker.worker_config[Config.METRIC_WORKER_SAMPLE_FREQUENCY])

        # In exactly once mode, task should be suspended after failover,
        # and be resumed together after all task has finished their rollback.
        if worker.worker_config[StreamingConstants.RELIABILITY_LEVEL]\
                == StreamingConstants.EXACTLY_ONCE:
            logger.info(
                "Suspend task in exactly once mode, wait for resume call from"
                " JobMaster.")
            self.suspended = True
        self.running = False
        self.stopped = False

    def build_reader_serializer(self):
        self.read_timeout_millis = \
            int(self.config.get(Config.READ_TIMEOUT_MS,
                                Config.DEFAULT_READ_TIMEOUT_MS))
        self.python_serializer = PythonSerializer()
        self.cross_lang_serializer = CrossLangSerializer()

    def do_checkpoint(self, checkpoint_id: int, input_points):
        logger.info("Start do checkpoint, cp id {}, inputPoints {}.".format(
            checkpoint_id, input_points))

        self.metric.cp_timer.start_duration()
        output_points = None
        if self.writer is not None:
            output_points = self.writer.get_output_checkpoints()

        operator_checkpoint = self.processor.save_checkpoint(checkpoint_id)
        op_checkpoint_info = OpCheckpointInfo(
            operator_checkpoint, input_points, output_points, checkpoint_id)
        self.__save_cp_state_and_report(op_checkpoint_info, checkpoint_id)

        barrier_pb = remote_call_pb2.Barrier()
        barrier_pb.id = checkpoint_id
        byte_buffer = barrier_pb.SerializeToString()
        if self.writer is not None:
            self.metric.inc_produce_barrier_count()
            self.writer.broadcast_barrier(checkpoint_id, byte_buffer)
        logger.info("Operator checkpoint {} finish.".format(checkpoint_id))
        self.metric.cp_timer.observe_duration()

    def __save_cp_state_and_report(self, op_checkpoint_info, checkpoint_id):
        logger.info(
            "Start to save cp state and report, checkpoint id is {}.".format(
                checkpoint_id))
        self.__save_cp(op_checkpoint_info, checkpoint_id)
        self.worker.commit_barrier(checkpoint_id)
        self.last_checkpoint_id = checkpoint_id

    def __save_cp(self, op_checkpoint_info, checkpoint_id):
        logger.info("save operator cp, op_checkpoint_info={}".format(
            op_checkpoint_info))
        cp_bytes = pickle.dumps(op_checkpoint_info)
        self.worker.state_backend.put(
            self.__gen_op_checkpoint_key(checkpoint_id), cp_bytes)
        self.worker.save_global_context(checkpoint_id)

    def handle_end_of_data(self):
        logger.info("Handle end of data.")
        self.worker.close_metric()
        if self.writer is not None:
            self.writer.broadcast_end_of_data_barrier()
        self.processor.close()
        report = EndOfDataReport(self.vertex_context.actor_id.binary())
        RemoteCallMst.report_job_worker_end_of_data(self.worker.master_actor,
                                                    report)

    def clear_expired_cp_state(self, checkpoint_id):
        cp_key = self.__gen_op_checkpoint_key(checkpoint_id)
        try:
            self.worker.state_backend.remove(cp_key)
            self.processor.delete_checkpoint(checkpoint_id)
        except BaseException:
            logger.exception("Failed to remote key %s from state backend.",
                             cp_key)

    def clear_expired_queue_msg(self, state_cp_id, queue_cp_id):
        # get operator checkpoint
        if self.reader is not None:
            self.reader.clear_checkpoint(queue_cp_id)
        if self.writer is not None:
            self.writer.clear_checkpoint(state_cp_id, queue_cp_id)

    def request_rollback(self, exception_msg: str):
        self.worker.request_rollback(exception_msg)

    def __gen_op_checkpoint_key(self, checkpoint_id):
        op_checkpoint_key = StreamingConstants \
            .JOB_WORKER_OP_CHECKPOINT_PREFIX_KEY + str(
                self.vertex_context.job_name) + "_" + str(
                    self.vertex_context.exe_vertex_name) + "_" + str(
                        checkpoint_id)
        logger.info(
            "Generate op checkpoint key {}. ".format(op_checkpoint_key))
        return op_checkpoint_key

    def __gen_output_actor_queues(self):
        execution_vertex_context = self.vertex_context
        build_time = execution_vertex_context.build_time
        output_actors_map = {}
        output_cyclic_map = {}
        job_vertex_id_to_exec = {}
        for edge in execution_vertex_context.output_execution_edges:
            target_task_id = edge.target_execution_vertex_id
            target_vertex = execution_vertex_context.\
                get_vertex_by_execution_vertex_id(target_task_id)
            target_job_vertex_id = target_vertex.execution_job_vertex_id
            if target_job_vertex_id not in job_vertex_id_to_exec:
                job_vertex_id_to_exec[target_job_vertex_id] = [], []
            exec_edges, chanel_names = job_vertex_id_to_exec.get(
                target_job_vertex_id)
            exec_edges.append(edge)
            target_actor = target_vertex.worker_actor
            channel_name = ChannelID.gen_id(self.task_id, target_task_id,
                                            build_time)
            chanel_names.append(channel_name)
            output_actors_map[channel_name] = target_actor
            output_cyclic_map[channel_name] = edge.cyclic
        return output_actors_map, output_cyclic_map, job_vertex_id_to_exec

    def __gen_input_actor_queues(self):
        execution_vertex_context = self.vertex_context
        build_time = execution_vertex_context.build_time
        input_actor_map = {}
        input_cyclic_map = {}
        for edge in execution_vertex_context.input_execution_edges:
            source_task_id = edge.source_execution_vertex_id
            source_actor = execution_vertex_context\
                .get_source_actor_by_execution_vertex_id(source_task_id)
            channel_name = ChannelID.gen_id(source_task_id, self.task_id,
                                            build_time)
            input_actor_map[channel_name] = source_actor
            input_cyclic_map[channel_name] = edge.cyclic
        return input_actor_map, input_cyclic_map

    def to_bool(self, str):
        return str in (True, "true", "True", "1")

    def recover(self, is_recover):
        logger.info("Stream task {}.".format("begin recover" if is_recover else
                                             "first start begin"))

        self.prepare_task(is_recover)
        ret = self.start()

        logger.info("Stream task {}.".format("recover end" if is_recover else
                                             "first start end"))
        return ret

    def prepare_task(self, is_recover):
        task_conf = dict(self.worker.config)
        channel_size = int(
            self.worker.config.get(Config.CHANNEL_SIZE,
                                   Config.CHANNEL_SIZE_DEFAULT))
        enable_dynamic_rebalance = self.worker.config.get(
            Config.ENABLE_DYNAMIC_REBALANCE,
            Config.ENABLE_DYNAMIC_REBALANCE_DEFAULT)
        enable_dynamic_rebalance = self.to_bool(enable_dynamic_rebalance)
        # Only if reliability level is exactly once, "Dynamic rebalance"
        # can replace Round-robin.
        reliability_level = self.worker.worker_config[
            StreamingConstants.RELIABILITY_LEVEL]
        if enable_dynamic_rebalance:
            enable_dynamic_rebalance = \
                reliability_level == StreamingConstants.EXACTLY_ONCE \
                or reliability_level == StreamingConstants.AT_LEAST_ONCE
        dynamic_rebalance_batch_size = int(
            self.worker.config.get(
                Config.DYNAMIC_REBALANCE_BATCH_SIZE,
                Config.DYNAMIC_REBALANCE_BATCH_SIZE_DEFAULT))
        streaming_ring_buffer_capacity = int(
            self.worker.config.get(
                NativeConfigKeys.STREAMING_RING_BUFFER_CAPACITY))
        task_conf[Config.CHANNEL_SIZE] = channel_size
        task_conf[Config.CHANNEL_TYPE] = self.worker.config \
            .get(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL)
        task_conf[
            NativeConfigKeys.PLASMA_STORE_PATH] = \
            ray.worker.global_worker.node.plasma_store_socket_name

        execution_vertex_context = self.worker.execution_vertex_context

        # get operator checkpoint
        op_checkpoint_info = OpCheckpointInfo()
        if is_recover:
            loaded_bytes = self.worker.state_backend.get(
                self.__gen_op_checkpoint_key(self.last_checkpoint_id))

            # when use memory state, if actor throw exception, will miss state
            if loaded_bytes:
                logger.info("Operator recover from checkpoint state.")
                op_checkpoint_info = pickle.loads(loaded_bytes)
                logger.info(
                    "Use state to recover queue link, op_checkpoint={}".format(
                        op_checkpoint_info))
            else:
                logger.warning(
                    "Loaded bytes empty, last checkpoint {}.".format(
                        self.last_checkpoint_id))

        # writers
        collectors = []
        output_actors_map, output_cyclic_map, job_vertex_id_to_exec = \
            self.__gen_output_actor_queues()
        output_queue_ids = []
        if len(output_actors_map) > 0:
            channel_str_ids = list(output_actors_map.keys())
            target_actors = list(output_actors_map.values())
            logger.info("Create DataWriter channel_ids {}, target_actors {}"
                        ", output_points={}, output_cyclic_map={}.".format(
                            channel_str_ids, target_actors,
                            op_checkpoint_info.output_points,
                            output_cyclic_map))
            self.writer = DataWriter()
            self.writer.init(task_conf, output_actors_map, output_cyclic_map,
                             op_checkpoint_info.output_points,
                             self.last_checkpoint_id, self.metric,
                             self.sample_frequency)
            logger.info("Create DataWriter succeed channel_ids {}, "
                        "target_actors {}.".format(channel_str_ids,
                                                   target_actors))
            # Note(loushang.ls) We need to keep the order
            # for the `split` operator.
            job_vertex_ids = list(job_vertex_id_to_exec.keys())
            job_vertex_ids.sort()
            for job_vertex_id in job_vertex_ids:
                (exec_edges,
                 channel_names) = job_vertex_id_to_exec[job_vertex_id]
                output_queue_ids += channel_names
                partition_func = exec_edges[0].partition
                edge_src_operator_id = exec_edges[0].original_src_operator_id
                if isinstance(
                        partition_func,
                        RoundRobinPartition) and enable_dynamic_rebalance:
                    logger.info("Dynamic rebalance created.")
                    dynamic_rebalance_param = DynamicRebalanceParam()
                    dynamic_rebalance_param.set_data_writer(self.writer)
                    dynamic_rebalance_param.set_output_queue_names(
                        channel_names)
                    dynamic_rebalance_param.set_buffer_capacity(
                        streaming_ring_buffer_capacity)
                    dynamic_rebalance_param.set_batch_size(
                        dynamic_rebalance_batch_size)
                    partition_func = DynamicRebalancePartition(
                        dynamic_rebalance_param)
                collectors.append(
                    OutputCollector(self.writer, channel_names, target_actors,
                                    partition_func, edge_src_operator_id,
                                    job_vertex_id))
            logger.info(f"Created collectors {collectors}")

        # readers
        input_actor_map, input_cyclic_map = self.__gen_input_actor_queues()
        if len(input_actor_map) > 0:
            channel_str_ids = list(input_actor_map.keys())
            from_actors = list(input_actor_map.values())
            logger.info("Create DataReader, channels {}, input_actors {}"
                        ", input_points={}, input_cyclic_map={}.".format(
                            channel_str_ids, from_actors,
                            op_checkpoint_info.input_points, input_cyclic_map))
            self.reader = DataReader()
            self.reader.init(task_conf, input_actor_map, input_cyclic_map,
                             op_checkpoint_info.input_points,
                             self.last_checkpoint_id)

            self.build_reader_serializer()

            def exit_handler():
                # Make DataReader stop read data when MockQueue destructor
                # gets called to avoid crash
                self.cancel_task()

            import atexit
            atexit.register(exit_handler)

        # TO update backpressure ratio metrics.
        def update_backpressure_ratio():
            if self.writer is not None:
                self.metric.report_bp_ratio_metric(
                    output_queue_ids,
                    self.writer.get_backpressure_ratio_by_list(
                        output_queue_ids))

        # Register backpressure ratio metric function, and report
        # backpressure ratio on time.
        self.metric.set_output_queue_ids(output_queue_ids)
        self.metric.init_backpressure_ratio_metric()
        self.metric.register_bp_ratio_func(update_backpressure_ratio)

        # Create updated job config in converted state operator or sort of
        # related changes.
        updated_job_config = self.__setup_job_config(task_conf)
        updated_job_config.update(task_conf)

        # create runtime context
        runtime_context = RuntimeContextImpl(
            self.worker.task_id,
            execution_vertex_context.execution_vertex.execution_vertex_index,
            execution_vertex_context.get_parallelism(),
            self.metric,
            self.worker.master_actor,
            self.worker.state_backend,
            config=task_conf,
            job_config=updated_job_config)
        logger.info("Open Processor: {}.".format(self.processor))

        # open processor
        self.processor.open(collectors, runtime_context)

        # load processor checkpoint
        logger.info("Stream task loading checkpoint: {}.".format(
            self.last_checkpoint_id))
        try:
            self.processor.load_checkpoint(self.last_checkpoint_id)
        except Exception as e:
            logger.error(
                "Failed to load checkpoint for stream task: {} {}. {}".format(
                    self.vertex_context.exe_vertex_name, e,
                    traceback.format_exc()))

        # immediately save cp. In case of FO in cp 0, or use old cp in multi
        # node FO.
        self.__save_cp(op_checkpoint_info, self.last_checkpoint_id)

    def __setup_job_config(self, job_config):
        return convert_operator_state_config(job_config)

    def start(self):
        recover_info = QueueRecoverInfo()
        if self.reader is not None:
            recover_info = self.reader.get_queue_recover_info()

        self.running = True
        self.thread.start()

        logger.info("Start operator success.")
        return recover_info

    @abstractmethod
    def run(self):
        pass

    def execute_control_task_from_mail_box(self):
        while not self.worker.mail_box.empty():
            logger.info("Job worker [{}] handle control message.".format(
                self.vertex_context.exe_vertex_name))
            message = self.worker.mail_box.get()
            self.handle_control_message(message)

    def handle_control_message(self, message):
        logger.info("Handle control message: {}.".format(message.message_type))
        if message.message_type is ControlMessageType.UPDATE_CONTEXT:
            self.update_context(message.message)
        elif message.message_type is ControlMessageType.CLEAR_PARTIAL_BARRIER:
            self.clear_partial_checkpoint(message.message[0],
                                          message.message[1])
        elif message.message_type is ControlMessageType.SUSPEND:
            self.suspend()
        elif message.message_type is ControlMessageType.RESUME:
            self.resume()
        elif message.message_type is ControlMessageType.EXCEPTION_INJECTION:
            message.message.execute()
        elif message.message_type is ControlMessageType.RESCALE_ROLLBACK:
            self.rescale_rollback(message.message)
        elif message.message_type is \
                ControlMessageType.BROADCAST_PARTIAL_BARRIER:
            self.handle_broadcast_partial_barrier(message.message)
        else:
            logger.info("No such message handler")

    def cancel_task(self):
        task_running_status = self.running
        # Set task running is false before stopping other transfers.
        self.running = False
        logger.info("Start to cancel task.")
        if self.writer:
            logger.info("Data writer stopping.")
            self.writer.stop()
            logger.info("Data writer stopped.")
        if self.reader:
            logger.info("Data reader stopping.")
            self.reader.stop()
            logger.info("Data reader stopped.")
        # Waiting task thread exit if task status is already running.
        slept_time = 0
        count = 0
        while task_running_status and not self.stopped:
            count += 1
            duration = 0.1 * count
            slept_time += duration
            time.sleep(duration)
            if slept_time > 5:
                logger.warning(
                    "Can't cancel task in %s seconds, exit process "
                    "now.", slept_time)
                os._exit(1)

    @abstractmethod
    def commit_trigger(self, barrier: Barrier) -> bool:
        pass

    def suspend(self):
        logger.info("Suspend stream task.")
        self.suspended = True

    def do_report_partial_checkpoint(self, checkpoint_id,
                                     partial_checkpoint_id):
        if partial_checkpoint_id in self.__outdated_partial_checkpoint_set:
            logger.info("Skip report outdated partial checkpoint {}.".format(
                partial_checkpoint_id))
            return
        # TODO(lingxuan.zlx): save checkpoint for partial barrier.
        self.worker.commit_partial_barrier(checkpoint_id,
                                           partial_checkpoint_id)
        self.worker.suspend()

    def read_and_process(self) -> bool:
        if self.reader is None:
            return True
        self.worker.initial_state_lock.acquire()
        try:
            item = self.reader.read(self.read_timeout_millis)
            self.is_initial_state = False
        finally:
            self.worker.initial_state_lock.release()
        # Continue to pull data from transfer if pulled data is None or empty
        # message.
        if item is None or item.body is None:
            return True

        continue_for_process = True
        if isinstance(item, DataMessage):
            # calculate tps and time cost
            is_sample = (self.consumed_count % self.sample_frequency) == 0
            if is_sample:
                self.metric.start_consume_msg_timer()
                self.consumed_count = 0

            # resolve record from bytes
            msg_data = item.body
            type_id = msg_data[0]
            if type_id == serialization.PYTHON_TYPE_ID:
                msg = self.python_serializer.deserialize(msg_data[1:])
            else:
                msg = self.cross_lang_serializer.deserialize(msg_data[1:])

            # calculate latency
            if is_sample:
                self.metric.start_latency_timer(msg.get_trace_timestamp())
                self.metric.observe_latency_timer()
            msg.set_trace_timestamp(int(round(time.time() * 1000)))

            # process record
            self.processor.process(msg)

            # calculate tps and time cost
            self.consumed_count += 1
            self.metric.mark_consume_msg_meter()
            if is_sample:
                self.metric.observe_consume_msg_timer()
        elif isinstance(item, CheckpointBarrier):
            logger.info("Got barrier: {}".format(item))
            if item.barrier_type == BarrierType.EndOfDataBarrier:
                logger.info(
                    "InputStreamTask got EndOfDataBarrier, trigger end of"
                    " data and then stop processing")
                self.processor.on_finish()
                self.handle_end_of_data()
                continue_for_process = False
            else:
                if item.is_partial_barrier:
                    partial_barrier = PartialBarrier(item.checkpoint_id,
                                                     item.partial_barrier_id)
                    self.handle_broadcast_partial_barrier(partial_barrier)
                else:
                    self.metric.inc_consume_barrier_count()
                    logger.info("Start to do checkpoint {}.".format(
                        item.checkpoint_id))
                    input_points = item.get_input_checkpoints()
                    self.do_checkpoint(item.checkpoint_id, input_points)
                    logger.info("Do checkpoint {} success.".format(
                        item.checkpoint_id))
                    self.metric.inc_cp_success()
        else:
            raise RuntimeError("Unknown item type!")

        return continue_for_process

    def resume(self):
        logger.info("Resume stream task.")
        self.suspended = False

    def update_context(self, new_context):
        logger.info("Update vertex context: {}.".format(new_context))

        self.worker.update_context_internal(new_context)
        # TODO: convert worker context to input queue dict
        self.worker_context = self.worker.worker_context
        self.vertex_context = self.worker.execution_vertex_context

        try:
            if self.reader:
                input_actor_queues, _ = self.__gen_input_actor_queues()
                self.reader.rescale(input_actor_queues)
            if self.writer:
                output_actor_queues, _, _ = self.__gen_output_actor_queues()
                self.writer.rescale(output_actor_queues)
        except Exception as e:
            logger.error("Failed to update context for worker: {}. {}".format(
                self.vertex_context.exe_vertex_name, e))
            return False

        logger.info("Worker {} update context succeeded.".format(
            self.vertex_context.exe_vertex_name))

        if bool(
                self.vertex_context.config.get(
                    Config.UPDATE_CONTEXT_OVERRIDE_ENABLE)):
            logger.info(
                "Worker {} override context for context-update.".format(
                    self.vertex_context.exe_vertex_name))
            self.worker.save_global_context(self.last_checkpoint_id)
        return True

    def handle_broadcast_partial_barrier(self, partial_barrier):
        if not self.worker_context.is_changed:
            logger.warn(
                f"Skip unchanged worker for broadcasting partial barrier {partial_barrier}."  # noqa: E501
            )
            return
        logger.info(
            "Handle broadcast partial barrier message: {} in worker: {}.".
            format(partial_barrier, self.vertex_context.exe_vertex_name))
        if partial_barrier.partial_checkpoint_id in \
                self.__outdated_partial_checkpoint_set:
            logger.warn("Skip report outdated partial checkpoint {}.".format(
                partial_barrier.partial_checkpoint_id))
            return

        # TODO(lingxuan.zlx): Skip outdataed partial barrier.
        if self.vertex_context.worker_state is not \
                remote_call_pb2.ExecutionVertexState.TO_ADD:
            # TODO(lingxuan.zlx): Flush state
            pass
        op_in_sub_dag = self.worker_context.role_in_changed_sub_dag
        if not (op_in_sub_dag in [remote_call_pb2.OperatorType.SOURCE_AND_SINK,
                remote_call_pb2.OperatorType.SOURCE]) \
            or self.vertex_context.worker_state is \
                remote_call_pb2.ExecutionVertexState.TO_DEL:
            self.suspend()
        else:
            logger.info(
                "Worker {}:{} continue after received partial barrier.".format(
                    self.vertex_context.execution_job_vertex_id,
                    self.vertex_context.execution_job_vertex_name))

        if self.worker_context.is_changed and \
                self.vertex_context.worker_state is \
                remote_call_pb2.ExecutionVertexState.TO_ADD:
            # TODO(lingxuan.zlx): processor close state.
            pass

        # commit partial barrier
        self.worker.commit_partial_barrier(
            partial_barrier.global_checkpoint_id,
            partial_barrier.partial_checkpoint_id)

        # broadcast partial barrier
        if op_in_sub_dag is not remote_call_pb2.OperatorType.SINK:
            self.broadcast_partial_barrier(partial_barrier)

    def broadcast_partial_barrier(self, partial_barrier):
        logger.info("Broadcast partial barrier to downstream.")
        if self.writer:
            barrier_pb = remote_call_pb2.PartialBarrier()
            barrier_pb.global_checkpoint_id = partial_barrier \
                .global_checkpoint_id
            barrier_pb.partial_checkpoint_id = partial_barrier \
                .partial_checkpoint_id
            byte_buffer = barrier_pb.SerializeToString()
            self.writer.broadcast_partial_barrier(
                partial_barrier.global_checkpoint_id,
                partial_barrier.partial_checkpoint_id, byte_buffer)

    def clear_partial_checkpoint(self, global_checkpoint_id,
                                 partial_checkpoint_id):
        logger.info("Clear partial checkpoint {},{} in task.".format(
            global_checkpoint_id, partial_checkpoint_id))
        if self.writer:
            self.writer.clear_partial_checkpoint(global_checkpoint_id,
                                                 partial_checkpoint_id)
        if self.reader:
            self.reader.clear_partial_checkpoint(global_checkpoint_id,
                                                 partial_checkpoint_id)
        # TODO(lingxuan.zlx): producer clear partial checkpoint
        if self.vertex_context.worker_state is not \
                remote_call_pb2.ExecutionVertexState.TO_DEL:
            logger.info("Finish to clear partial checkpoint, "
                        "now resume all consumers.")
            self.resume()
        else:
            logger.info("No need to resume a deleting consumer.")
        self.__outdated_partial_checkpoint_set.clear()
        return self.worker.clear_partial_context(global_checkpoint_id,
                                                 partial_checkpoint_id)

    def rescale_rollback(self, partial_checkpoint_id):
        logger.info("Rescale rollback in task, outdated partial checkpoint "
                    "id {}.".format(partial_checkpoint_id))
        self.__outdated_partial_checkpoint_set.add(partial_checkpoint_id)
        if self.reader:
            self.reader.rescale_rollback()

    def fetch_profiling_infos(self):
        obj = json.loads("{}")

        if self.writer:
            obj["producer"] = self.writer.fetch_profiling_infos()
        if self.reader:
            obj["consumer"] = self.reader.fetch_profiling_infos()
        # There is no is_rollback in python worker, so just return false.
        obj["rollbacking"] = "false"
        logger.info("type of obj is: {}.".format(type(obj)))
        return json.dumps(obj)


class InputStreamTask(StreamTask):
    """Base class for stream tasks that execute a
    :class:`runtime.processor.OneInputProcessor` or
    :class:`runtime.processor.TwoInputProcessor` """

    def commit_trigger(self, barrier):
        raise RuntimeError(
            "commit_trigger is only supported in SourceStreamTask.")

    def __init__(self, task_id, processor_instance, worker,
                 last_checkpoint_id):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)

    def run(self):
        logger.info("Input task thread start.")
        try:
            while self.running:
                self.execute_control_task_from_mail_box()
                if self.suspended:
                    time.sleep(0.1)
                    continue
                if not self.read_and_process():
                    break
        except ChannelInterruptException:
            logger.info("queue has stopped.")
        except BaseException as e:
            logger.exception(
                "Last success checkpointId={}, now occur error: {}.".format(
                    self.last_checkpoint_id, e))
            self.request_rollback(traceback.format_exc())
        logger.info("Input task thread exit.")
        self.stopped = True


class OneInputStreamTask(InputStreamTask):
    """A stream task for executing :class:`runtime.processor.OneInputProcessor`
    """

    def __init__(self, task_id, processor_instance, worker,
                 last_checkpoint_id):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)


class TwoInputStreamTask(InputStreamTask):
    """A stream task for executing :class:`runtime.processor.TwoInputProcessor`
    """

    def __init__(self, task_id, processor_instance, worker, last_checkpoint_id,
                 left_stream, right_stream):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)
        self.processor.left_stream = left_stream
        self.processor.right_stream = right_stream


class MultiInputStreamTask(InputStreamTask):
    """A stream task for executing :class:`runtime.processor.MultiInputProcessor`
    """

    def __init__(self, task_id, processor_instance, worker, last_checkpoint_id,
                 right_input_vertex_ids):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)
        self.processor.set_upstream_runtime_context(right_input_vertex_ids)


class SourceStreamTask(StreamTask):
    """A stream task for executing :class:`runtime.processor.SourceProcessor`
    """

    def __init__(self, task_id: int, processor_instance, worker,
                 last_checkpoint_id):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)
        self.__pending_barrier: Optional[Barrier] = None

    def run(self):
        logger.info("Source task thread start.")
        # NOTE(lingxuan.zlx): source stream task will break the reading loop
        # after receiving EndOfDataException, but no pending global barrier
        # can be passed and transporting to downstreams who will always be
        # suspended and waiting for alignment of the comming global barrier.
        # To address this issue, we don't need break this whole task runnable
        # thread and record its status in `end_of_fetcher`.
        end_of_fetcher = False
        try:
            while self.running:
                self.execute_control_task_from_mail_box()
                if self.suspended:
                    time.sleep(0.1)
                    continue
                try:
                    if not end_of_fetcher:
                        self.processor.fetch(self.last_checkpoint_id + 1)
                    else:
                        # Sleep a while to reduce spin-loop.
                        time.sleep(0.1)
                except EndOfDataException:
                    logger.info(
                        "SourceStreamTask catch EndOfDataException, trigger"
                        " end of data and then stop processing.")
                    self.processor.on_finish()
                    self.handle_end_of_data()
                    end_of_fetcher = True
                # check checkpoint
                if self.__pending_barrier is not None:
                    # source fetcher only have outputPoints
                    barrier = self.__pending_barrier
                    logger.info("Start to do checkpoint {}.".format(
                        barrier.id))
                    self.do_checkpoint(barrier.id, barrier)
                    logger.info("Finish to do checkpoint {}.".format(
                        barrier.id))
                    self.__pending_barrier = None

                if not self.read_and_process():
                    break

        except ChannelInterruptException:
            logger.info("queue has stopped.")
        except BaseException as e:
            logger.error(
                "Last success checkpointId={}, now occur error: {} {}.".format(
                    self.last_checkpoint_id, e, traceback.format_exc()))
            self.request_rollback(traceback.format_exc())

        logger.info("Source fetcher thread exit.")
        self.stopped = True

    def commit_trigger(self, barrier):
        if self.__pending_barrier is not None:
            logger.warning(
                "Last barrier is not broadcast now, skip this barrier trigger."
            )
            return False

        self.__pending_barrier = barrier
        return True
