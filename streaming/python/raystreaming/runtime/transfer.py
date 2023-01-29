import logging
import random
import typing
from abc import ABC, abstractmethod
from enum import Enum
from queue import Queue

import ray
from ray._raylet import JavaFunctionDescriptor
from ray._raylet import Language
from ray._raylet import PythonFunctionDescriptor
from ray.actor import ActorHandle
from raystreaming import _streaming
from raystreaming.config import ConfigHelper
from raystreaming.constants import StreamingConstants
from raystreaming.generated import streaming_pb2
from raystreaming.utils import EnvUtil
from raystreaming.handler import generate_template

logger = logging.getLogger(__name__)

CHANNEL_ID_LEN = ray.ObjectID.nil().size()


class ChannelID:
    """
    ChannelID is used to identify a transfer channel between
     a upstream worker and downstream worker.
    """

    def __init__(self, channel_id_str: str):
        """
        Args:
            channel_id_str: string representation of channel id
        """
        self.channel_id_str = channel_id_str
        self.object_id = ray.ObjectID(channel_id_str_to_bytes(channel_id_str))

    def __eq__(self, other):
        if other is None:
            return False
        if type(other) is ChannelID:
            return self.channel_id_str == other.channel_id_str
        else:
            return False

    def __hash__(self):
        return hash(self.channel_id_str)

    def __repr__(self):
        return self.channel_id_str

    @staticmethod
    def gen_random_id():
        """Generate a random channel id string
        """
        res = ""
        for i in range(CHANNEL_ID_LEN * 2):
            res += str(chr(random.randint(0, 5) + ord("A")))
        return res

    @staticmethod
    def gen_random_bytes_id():
        """Generate a random channel id string
        """
        return channel_id_str_to_bytes(ChannelID.gen_random_id())

    @staticmethod
    def gen_id(from_index, to_index, ts):
        """Generate channel id, which is 28 character"""
        channel_id = bytearray(CHANNEL_ID_LEN)
        for i in range(11, 7, -1):
            channel_id[i] = ts & 0xff
            ts >>= 8
        channel_id[CHANNEL_ID_LEN - 4] = (from_index & 0xffff) >> 8
        channel_id[CHANNEL_ID_LEN - 3] = (from_index & 0xff)
        channel_id[CHANNEL_ID_LEN - 2] = (to_index & 0xffff) >> 8
        channel_id[CHANNEL_ID_LEN - 1] = (to_index & 0xff)
        return channel_id_bytes_to_str(bytes(channel_id))


def channel_id_str_to_bytes(channel_id_str):
    """
    Args:
        channel_id_str: string representation of channel id

    Returns:
        bytes representation of channel id
    """
    assert type(channel_id_str) in [str, bytes]
    if isinstance(channel_id_str, bytes):
        return channel_id_str
    qid_bytes = bytes.fromhex(channel_id_str)
    assert len(qid_bytes) == CHANNEL_ID_LEN
    return qid_bytes


def channel_id_bytes_to_str(id_bytes):
    """
    Args:
        id_bytes: bytes representation of channel id

    Returns:
        string representation of channel id
    """
    assert type(id_bytes) in [str, bytes]
    if isinstance(id_bytes, str):
        return id_bytes
    return bytes.hex(id_bytes)


class Message(ABC):
    @property
    @abstractmethod
    def body(self):
        """Message data"""
        pass

    @property
    @abstractmethod
    def timestamp(self):
        """Get timestamp when item is written by upstream DataWriter
        """
        pass

    @property
    @abstractmethod
    def channel_id(self):
        """Get string id of channel where data is coming from"""
        pass

    @property
    @abstractmethod
    def message_id(self):
        """Get message id of the message"""
        pass


class DataMessage(Message):
    """
    DataMessage represents data between upstream and downstream operator.
    """

    def __init__(self,
                 body,
                 timestamp,
                 message_id,
                 channel_id,
                 is_empty_message=False):
        self.__body = body
        self.__timestamp = timestamp
        self.__channel_id = channel_id
        self.__message_id = message_id
        self.__is_empty_message = is_empty_message

    def __len__(self):
        return len(self.__body)

    @property
    def body(self):
        return self.__body

    @property
    def timestamp(self):
        return self.__timestamp

    @property
    def channel_id(self):
        return self.__channel_id

    @property
    def message_id(self):
        return self.__message_id

    @property
    def is_empty_message(self):
        """Whether this message is an empty message.
        Upstream DataWriter will send an empty message when this is no data
         in specified interval.
        """
        return self.__is_empty_message


class BarrierType(Enum):
    GlobalBarrier = 0
    PartialBarrier = 1
    EndOfDataBarrier = 2


class CheckpointBarrier(Message):
    """
    CheckpointBarrier separates the records in the data stream into the set of
     records that goes into the current snapshot, and the records that go into
     the next snapshot. Each barrier carries the ID of the snapshot whose
     records it pushed in front of it.
    """

    def __init__(self, barrier_data, timestamp, message_id, channel_id,
                 offsets, barrier_id, partial_barrier_id, barrier_type):
        self.__barrier_data = barrier_data
        self.__timestamp = timestamp
        self.__message_id = message_id
        self.__channel_id = channel_id
        self.checkpoint_id = barrier_id
        self.offsets = offsets
        self.partial_barrier_id = partial_barrier_id
        self.barrier_type = BarrierType(barrier_type)

    @property
    def body(self):
        return self.__barrier_data

    @property
    def timestamp(self):
        return self.__timestamp

    @property
    def channel_id(self):
        return self.__channel_id

    @property
    def message_id(self):
        return self.__message_id

    def get_input_checkpoints(self):
        return self.offsets

    @property
    def is_partial_barrier(self):
        return self.barrier_type is BarrierType.PartialBarrier

    @property
    def is_global_barrier(self):
        return self.barrier_type is BarrierType.GlobalBarrier

    def __str__(self):
        if self.is_global_barrier:
            return "Barrier(Checkpoint id : {})".format(self.checkpoint_id)
        elif self.is_partial_barrier:
            return "PartialBarrier(checkpoint id : {}, " \
                   "partial checkpoint id : {})".format(
                       self.checkpoint_id, self.partial_barrier_id)
        elif self.barrier_type == BarrierType.EndOfDataBarrier:
            return "EndOfDataBarrier"
        else:
            logger.warn("Unsupported barrier type: {}".format(
                self.barrier_type))


class ChannelCreationParametersBuilder:
    """
    wrap initial parameters needed by a streaming queue
    """

    @staticmethod
    def build_input_queue_parameters(
            queue_ids_dict: typing.Mapping[str, ActorHandle],
            cyclic_map: typing.Mapping[str, bool]):
        return ChannelCreationParametersBuilder.build_parameters(
            queue_ids_dict, cyclic_map, True)

    @staticmethod
    def build_output_queue_parameters(
            queue_ids_dict: typing.Mapping[str, ActorHandle],
            cyclic_map: typing.Mapping[str, bool]):
        return ChannelCreationParametersBuilder.build_parameters(
            queue_ids_dict, cyclic_map, False)

    @staticmethod
    def build_parameters(queue_ids_dict: typing.Mapping[str, ActorHandle],
                         cyclic_map: typing.Mapping[str, bool],
                         for_input: bool):
        """
        queue_ids_dict: queue id dict
        """
        parameters = []
        if for_input:
            py_async_func_name, py_sync_func_name = \
                generate_template.format("on_writer_message"), \
                generate_template.format("on_writer_message_sync")
        else:
            py_async_func_name, py_sync_func_name = \
                generate_template.format("on_reader_message"), \
                generate_template.format("on_reader_message_sync")
        for channel_name, actor_handle in queue_ids_dict.items():
            creation_desc = actor_handle. \
                _ray_actor_creation_function_descriptor
            if actor_handle._ray_actor_language == Language.PYTHON:
                async_func = PythonFunctionDescriptor(
                    creation_desc.module_name, py_async_func_name,
                    creation_desc.class_name.encode())
                sync_func = PythonFunctionDescriptor(
                    creation_desc.module_name, py_sync_func_name,
                    creation_desc.class_name.encode())
            else:
                # signature can be omitted if there are noe overloaded methods.
                if for_input:
                    async_func = JavaFunctionDescriptor(
                        creation_desc.class_name,
                        generate_template.format("onWriterMessage"), "")
                    sync_func = JavaFunctionDescriptor(
                        creation_desc.class_name,
                        generate_template.format("onWriterMessageSync"), "")
                else:
                    async_func = JavaFunctionDescriptor(
                        creation_desc.class_name,
                        generate_template.format("onReaderMessage"), "")
                    sync_func = JavaFunctionDescriptor(
                        creation_desc.class_name,
                        generate_template.format("onReaderMessageSync"), "")
            parameter = _streaming.ChannelCreationParameter(
                actor_handle._ray_actor_id, async_func, sync_func,
                cyclic_map[channel_name])
            parameters.append(parameter)
        return parameters


class DataWriter:
    """Data Writer is a wrapper of streaming c++ DataWriter, which sends data
     to downstream workers
    """

    def __init__(self):
        self._configuration = {}
        self._output_queue_ids = []
        self._output_checkpoints = {}
        self._writer = None
        self.write_count = 0
        self.metric = None
        self.sample_frequency = 2000

    def init(self, conf,
             output_queues: typing.Mapping[str, ray.actor.ActorHandle],
             cyclic_map: typing.Mapping[str, bool],
             output_points: typing.Mapping[bytes, int], checkpoint_id, metric,
             sample_frequency):
        self.set_configuration(conf)
        self.load_checkpoint(output_points, checkpoint_id)
        output_queue_ids_list = [
            channel_id_str_to_bytes(qid_str)
            for qid_str in output_queues.keys()
        ]
        creation_parameters = ChannelCreationParametersBuilder \
            .build_output_queue_parameters(output_queues, cyclic_map)
        logger.info("Start init DataWriter, queues:{}, message ids: {}".format(
            output_queue_ids_list, self._output_checkpoints))

        self._output_queue_ids = output_queue_ids_list
        channel_size = int(
            self._configuration.get(NativeConfigKeys.QUEUE_SIZE))
        config_bytes = _to_native_conf(self._configuration)
        py_msg_ids = DataWriter.get_offset_list(output_queue_ids_list,
                                                self._output_checkpoints)
        self._writer = _streaming.DataWriter \
            .create(output_queue_ids_list,
                    creation_parameters, channel_size, py_msg_ids,
                    config_bytes)
        self.metric = metric
        self.sample_frequency = sample_frequency
        logger.info("init DataWriter succeed")

    @staticmethod
    def get_offset_list(queue_ids, points):
        res = []
        for qid in queue_ids:
            if qid in points:
                res.append(points[qid])
            else:
                res.append(0)
        return res

    def write(self, channel_id: ChannelID, item: bytes):
        """Write data into native channel
        Args:
            channel_id: channel id
            item: bytes data
        Returns:
            msg_id
        """
        assert type(item) == bytes

        is_sample = (self.write_count % self.sample_frequency) == 0
        if is_sample:
            self.metric.start_produce_msg_timer()
            self.write_count = 0

        msg_id = self._writer.write(channel_id.object_id, item)

        self.write_count += 1
        self.metric.mark_produce_msg_meter()
        if is_sample:
            self.metric.observe_produce_msg_timer()

        return msg_id

    def set_configuration(self, conf):
        for (k, v) in conf.items():
            self._configuration[k] = v

    def load_checkpoint(self, output_points, checkpoint_id):
        if output_points is not None:
            self._output_checkpoints = output_points
        self._configuration[NativeConfigKeys.CHECKPOINT_ID] = checkpoint_id

    def broadcast_barrier(self, checkpoint_id, attach):
        self._writer.broadcast_barrier(checkpoint_id, checkpoint_id, attach)

    def broadcast_partial_barrier(self, global_checkpoint_id,
                                  partial_barrier_id, attach):
        self._writer.broadcast_partial_barrier(global_checkpoint_id,
                                               partial_barrier_id, attach)

    def get_output_checkpoints(self):
        return self._writer.get_output_seq_id(self._output_queue_ids)

    def get_backpressure_ratio(self):
        return self._writer.get_backpressure_ratio(self._output_queue_ids)

    def get_backpressure_ratio_by_list(self, channel_names):
        """The return is a dict whose key is the channel name
        with binary format.
        """
        output_queue_ids_list = [
            channel_id_str_to_bytes(qid_str) for qid_str in channel_names
        ]
        return self._writer.get_backpressure_ratio(output_queue_ids_list)

    def clear_checkpoint(self, state_cp_id, queue_cp_id):
        logger.info("producer start to clear checkpoint"
                    ", state_cp_id={} queue_cp_id={}".format(
                        state_cp_id, queue_cp_id))
        self._writer.clear_checkpoint(state_cp_id, queue_cp_id)

    def on_writer_message(self, buffer: bytes):
        self._writer.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        return self._writer.on_writer_message_sync(buffer).to_pybytes()

    def stop(self):
        logger.info("stopping channel writer.")
        if self._writer is not None:
            self._writer.stop()
            # destruct DataWriter
            self._writer = None

    def close(self):
        logger.info("closing channel writer.")

    def broadcast_end_of_data_barrier(self):
        logger.info("transfer.py broadcast_end_of_data_barrier.")
        self._writer.broadcast_end_of_data_barrier()

    def rescale(self, output_q_ids_dict):
        output_queue_ids = [
            channel_id_str_to_bytes(qid_str)
            for qid_str in output_q_ids_dict.keys()
        ]
        # TODO(lingxuan.zlx): Make queue diff out of reader.
        queue_added = set(output_queue_ids) - set(self._output_queue_ids)
        queue_deleted = set(self._output_queue_ids) - set(output_queue_ids)
        logger.info(
            "rescale input queue ids {}, queue added {}, queue_deleted {}".
            format(output_queue_ids, queue_added, queue_deleted))
        to_collect_ids = list(set(output_queue_ids).union(queue_deleted))
        logger.info(
            "Writer to collect partial set : {}.".format(to_collect_ids))
        # TODO(lingxuan.zlx): We'd better select all cyclic edges from queue
        # id vector. But for now, we assume all queues are noncyclic.
        cyclic_map = {queue_id: False for queue_id in output_q_ids_dict.keys()}

        initial_parameters = ChannelCreationParametersBuilder \
            .build_output_queue_parameters(output_q_ids_dict, cyclic_map)
        self._writer.rescale(output_queue_ids, to_collect_ids,
                             initial_parameters)
        self._output_queue_ids = output_queue_ids

    def clear_partial_checkpoint(self, global_checkpoint_id,
                                 partial_checkpoint_id):
        logger.info("Clear writer partial checkpoint.")
        self._writer.clear_partial_checkpoint(global_checkpoint_id,
                                              partial_checkpoint_id)

    def fetch_profiling_infos(self):
        return self._writer.fetch_profiling_infos()


class DataReader:
    """Data Reader is wrapper of streaming c++ DataReader, which read data
    from channels of upstream workers
    """

    def __init__(self):
        self._configuration = {}
        self._queue = Queue()
        self._input_checkpoints = {}
        self._input_queue_ids = []
        self._queues_creation_status = None

    def init(self, conf, input_queues: typing.Mapping[str, ActorHandle],
             cyclic_map: typing.Mapping[str, bool],
             input_points: typing.Mapping[bytes, int], checkpoint_id):
        self.set_configuration(conf)
        self.load_checkpoint(input_points, checkpoint_id)
        assert len(input_queues) > 0
        input_queue_ids = \
            [channel_id_str_to_bytes(qid_str)
             for qid_str in input_queues.keys()]
        msg_id_list = []
        for qid in input_queue_ids:
            if qid in self._input_checkpoints:
                msg_id_list.append(self._input_checkpoints[qid])
            else:
                msg_id_list.append(0)

        timer_interval = -1
        if NativeConfigKeys.TIMER_INTERVAL_MS in self._configuration:
            timer_interval = int(
                self._configuration.get(NativeConfigKeys.TIMER_INTERVAL_MS))

        initial_parameters = ChannelCreationParametersBuilder \
            .build_input_queue_parameters(input_queues, cyclic_map)
        self._input_queue_ids = input_queue_ids
        logger.info("Create native data reader, input_queues={}, channels={},"
                    " msg_ids={}, conf={}".format(input_queues,
                                                  input_queue_ids, msg_id_list,
                                                  self._configuration))
        reader, queues_creation_status = _streaming.DataReader.create(
            input_queue_ids, initial_parameters, msg_id_list, timer_interval,
            _to_native_conf(self._configuration))
        self._reader = reader
        self._queues_creation_status = queues_creation_status
        creation_status = {}
        for q, status in queues_creation_status.items():
            creation_status[q] = QueueCreationStatus(status)
        logger.info("DataReader init success, queues_creation_status: %s",
                    creation_status)
        return QueueRecoverInfo(queue_creation_status_map=creation_status)

    def set_configuration(self, conf):
        for (k, v) in conf.items():
            self._configuration[k] = v

    def load_checkpoint(self, input_points, checkpoint_id):
        if input_points is not None:
            self._input_checkpoints = input_points
        self._configuration[NativeConfigKeys.CHECKPOINT_ID] = checkpoint_id

    def get_queue_recover_info(self):
        if self._queues_creation_status is not None:
            creation_status = {}
            for q, status in self._queues_creation_status.items():
                creation_status[q] = QueueCreationStatus(status)
            return QueueRecoverInfo(queue_creation_status_map=creation_status)
        logger.error("Can not get queue info when reader hasn't initialized.")
        return None

    def read(self, timeout_millis):
        """Read data from channel
        Args:
            timeout_millis: timeout millis when there is no data in channel
             for this duration
        Returns:
            channel item
        """
        if self._queue.empty():
            messages = self._reader.read(timeout_millis)
            for message in messages:
                self._queue.put(message)

        if self._queue.empty():
            return None
        return self._queue.get()

    def rescale(self, input_q_ids_dict):
        input_queue_ids = [
            channel_id_str_to_bytes(qid_str)
            for qid_str in input_q_ids_dict.keys()
        ]
        # TODO(lingxuan.zlx): Make queue diff out of reader.
        queue_added = set(input_queue_ids) - set(self._input_queue_ids)
        queue_deleted = set(self._input_queue_ids) - set(input_queue_ids)
        logger.info(
            "rescale input queue ids {}, queue added {}, queue_deleted {}".
            format(input_queue_ids, queue_added, queue_deleted))
        to_collect_ids = list(set(input_queue_ids).union(queue_deleted))
        logger.info("to collect partial set : {}.".format(to_collect_ids))
        # TODO(lingxuan.zlx): We'd better select all cyclic edges from queue
        # id vector.
        # But for now, we assume all queues are noncyclic.
        cyclic_map = {queue_id: False for queue_id in input_q_ids_dict.keys()}

        initial_parameters = ChannelCreationParametersBuilder \
            .build_input_queue_parameters(input_q_ids_dict, cyclic_map)
        self._reader.rescale(input_queue_ids, to_collect_ids,
                             initial_parameters)
        self._input_queue_ids = input_queue_ids

    def clear_checkpoint(self, queue_cp_id):
        self._reader.clear_checkpoint(queue_cp_id)

    def clear_partial_checkpoint(self, global_checkpoint_id,
                                 partial_checkpoint_id):
        logger.info("Clear reader partial checkpoint.")
        self._reader.clear_partial_checkpoint(global_checkpoint_id,
                                              partial_checkpoint_id)

    def rescale_rollback(self):
        logger.info("rescale rollback")
        self._reader.rescale_rollback()

    def stop(self):
        logger.info("stopping Data Reader.")
        if self._reader is not None:
            self._reader.stop()
            # destruct DataReader
            self._reader = None

    def close(self):
        logger.info("closing Data Reader.")

    def on_reader_message(self, *buffers):
        self._reader.on_reader_message(*buffers)

    def on_reader_message_sync(self, buffer):
        return self._reader.on_reader_message_sync(buffer).to_pybytes()

    def fetch_profiling_infos(self):
        return self._reader.fetch_profiling_infos()


class QueueRecoverInfo:
    def __init__(self, queue_creation_status_map=None):
        if queue_creation_status_map is None:
            queue_creation_status_map = {}
        self.__queue_creation_status_map = queue_creation_status_map

    def get_creation_status(self):
        return self.__queue_creation_status_map

    def get_data_lost_queues(self):
        data_lost_queues = set()
        # NOTE(lingxuan.zlx): See more details from java side.
        for (q, status) in self.__queue_creation_status_map.items():
            if (status in [
                    QueueCreationStatus.DataLost, QueueCreationStatus.Timeout
            ]):
                data_lost_queues.add(q)
        return data_lost_queues

    def __str__(self):
        return "QueueRecoverInfo [dataLostQueues=%s]" \
               % (self.get_data_lost_queues())


class QueueCreationStatus(Enum):
    FreshStarted = 0
    PullOk = 1
    Timeout = 2
    DataLost = 3


class ChannelInitException(Exception):
    def __init__(self, msg):
        self.msg = msg


class ChannelInterruptException(Exception):
    def __init__(self, msg=None):
        self.msg = msg


class OperatorType(Enum):
    """
    operator type
    """

    SOURCE = 1
    TRANSFORM = 2
    SINK = 3


class ReliabilityLevel(Enum):
    """
    reliability level
    """

    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2
    EXACTLY_SAME = 3


class NativeConfigKeys:
    PLASMA_STORE_PATH = "storePath"
    LOCAL_SCHEDULER_CONNECTION_PTR = "localSchedulerConnectionPtr"
    QUEUE_SIZE = "queueSize"
    TIMER_INTERVAL_MS = "timerIntervalMs"
    IS_RECREATE = "isRecreate"
    QUEUE_TYPE = "queue_type"
    """
     ray runtime keys
    """
    RAY_RUNTIME_OBJECT_STORE_ADDRESS = "object_store_address"
    RAY_RUNTIME_RAYLET_SOCKET_NAME = "raylet_socket_name"
    RAY_RUNTIME_TASK_JOB_ID = "current_job_id"
    """
     protobuf keys
    """

    # string config
    STREAMING_PERSISTENCE_PATH = "StreamingPersistencePath"
    STREAMING_JOB_NAME = "StreamingJobName"
    STREAMING_OP_NAME = "StreamingOpName"
    STREAMING_WORKER_NAME = "StreamingWorkerName"
    STREAMING_LOG_PATH = "StreamingLogPath"
    RAYLET_SOCKET_NAME = "RayletSocketName"
    ELASTICBUFFER_FILE_DIRECTORY = "ElasticBufferFileDirectory"

    # uint config
    STREAMING_LOG_LEVEL = "StreamingLogLevel"
    STREAMING_RING_BUFFER_CAPACITY = "StreamingRingBufferCapacity"
    STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = "StreamingEmptyMessageTimeInterval"  # noqa: E501
    STREAMING_RECONSTRUCT_OBJECTS_TIME_OUT_PER_MB = "StreamingReconstructObjectsTimeOutPerMb"  # noqa: E501
    STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES = "StreamingReconstructObjectsRetryTimes"  # noqa: E501
    CHECKPOINT_ID = "checkpoint_id"
    TASK_JOB_ID = "task_job_id"
    STREAMING_WRITER_CONSUMED_STEP = "StreamingWriterConsumedStep"
    STREAMING_READER_CONSUMED_STEP = "StreamingReaderConsumedStep"
    STREAMING_READER_CONSUMED_STEP_UPDATER = "StreamingReaderConsumedStepUpdater"  # noqa: E501
    STREAMING_FLOW_CONTROL_SEQ_ID_SCALE = "StreamingFlowControlSeqIdScale"
    RAY_PIPE_BATCH_INTERVAL = "RayPipeBatchInterval"
    STREAMING_FLOW_CONTROL = "StreamingFlowControl"
    HTTP_PROFILER_ENABLE = "HttpProfilerEnable"
    STREAMING_READER_ROUND_ROBIN_SLEEP_TIME_US = "StreamingReaderRoundRobinSleepTimeUs"  # noqa: E501
    ELASTICBUFFER_ENABLE = "ElasticBufferEnable"
    ELASTICBUFFER_MAX_SAVE_BUFFER_SIZE = "ElasticBufferMaxSaveBufferSize"
    ELASTICBUFFER_FLUSH_BUFFER_SIZE = "ElasticBufferFlushBufferSize"
    ELASTICBUFFER_FILE_CACHE_SIZE = "ElasticBufferFileCacheSize"
    ELASTICBUFFER_MAX_FILE_NUM = "ElasticBufferMaxFileNum"


def _to_native_conf(conf):
    streaming_config = streaming_pb2.StreamingConfig()
    streaming_config.buffer_pool_size = ConfigHelper.get_buffer_pool_size(conf)
    streaming_config.buffer_pool_min_buffer_size = \
        ConfigHelper.get_buffer_pool_min_buffer_size(conf)

    if NativeConfigKeys.STREAMING_RING_BUFFER_CAPACITY in conf:
        streaming_config.ring_buffer_capacity = \
            int(conf[NativeConfigKeys.STREAMING_RING_BUFFER_CAPACITY])

    if NativeConfigKeys.STREAMING_LOG_LEVEL in conf:
        streaming_config.log_level = int(
            conf[NativeConfigKeys.STREAMING_LOG_LEVEL])

    if NativeConfigKeys.STREAMING_LOG_PATH in conf:
        streaming_config.log_path = conf[NativeConfigKeys.STREAMING_LOG_PATH]

    if NativeConfigKeys.STREAMING_EMPTY_MESSAGE_TIME_INTERVAL in conf:
        streaming_config.empty_message_interval = \
            int(conf[NativeConfigKeys.STREAMING_EMPTY_MESSAGE_TIME_INTERVAL])

    if NativeConfigKeys.PLASMA_STORE_PATH in conf:
        streaming_config.plasma_socket_path = \
            conf[NativeConfigKeys.PLASMA_STORE_PATH]

    if NativeConfigKeys.QUEUE_TYPE in conf or StreamingConstants.QUEUE_TYPE \
            in conf:
        queue_type = conf[NativeConfigKeys.QUEUE_TYPE] \
            if conf[NativeConfigKeys.QUEUE_TYPE] in conf else \
            conf[StreamingConstants.QUEUE_TYPE]
        streaming_config.queue_type = streaming_pb2.TransferQueueType.Value(
            queue_type.upper())

    if NativeConfigKeys.CHECKPOINT_ID in conf:
        streaming_config.checkpoint_id = int(
            conf[NativeConfigKeys.CHECKPOINT_ID])

    if NativeConfigKeys.STREAMING_JOB_NAME in conf:
        streaming_config.job_name = conf[NativeConfigKeys.STREAMING_JOB_NAME]

    if NativeConfigKeys.STREAMING_OP_NAME in conf:
        streaming_config.op_name = conf[NativeConfigKeys.STREAMING_OP_NAME]

    if StreamingConstants.OPERATOR_TYPE in conf:
        streaming_config.role = \
            streaming_pb2.NodeType.Value(
                conf[StreamingConstants.OPERATOR_TYPE])

    if StreamingConstants.RELIABILITY_LEVEL in conf or \
            StreamingConstants.RELIABILITY_LEVEL_COMPATIBLE in conf:
        streaming_config.reliability_level = \
            streaming_pb2.ReliabilityLevel.Value(
                conf[StreamingConstants.RELIABILITY_LEVEL]
                if StreamingConstants.RELIABILITY_LEVEL in conf
                else conf[StreamingConstants.RELIABILITY_LEVEL_COMPATIBLE]
            )

    if NativeConfigKeys.ELASTICBUFFER_ENABLE in conf:
        streaming_config.elastic_buffer_enable = True \
            if conf[NativeConfigKeys.ELASTICBUFFER_ENABLE] in \
            ["True", True, 1] else False

    if NativeConfigKeys.HTTP_PROFILER_ENABLE in conf:
        streaming_config.http_profiler_enable = True \
            if conf[NativeConfigKeys.HTTP_PROFILER_ENABLE] in \
            ["True", True, 1] else False

    if NativeConfigKeys.STREAMING_READER_ROUND_ROBIN_SLEEP_TIME_US in conf:
        streaming_config.reader_round_robin_sleep_time_us = \
            int(conf[
                NativeConfigKeys.STREAMING_READER_ROUND_ROBIN_SLEEP_TIME_US])

    streaming_config.persistence_cluster_name = \
        ConfigHelper.get_cp_pangu_cluster_name(conf)
    streaming_config.persistence_path = ConfigHelper.get_cp_pangu_root_dir(
        conf)

    if NativeConfigKeys.TASK_JOB_ID in conf:
        streaming_config.job_id = conf[NativeConfigKeys.TASK_JOB_ID]

    # Use string name key first.
    if NativeConfigKeys.STREAMING_FLOW_CONTROL in conf:
        if int(conf[NativeConfigKeys.STREAMING_FLOW_CONTROL]) in \
                streaming_pb2.FlowControlType.values():
            streaming_config.flow_control_config.flow_control_type = \
                int(conf[NativeConfigKeys.STREAMING_FLOW_CONTROL])

    if NativeConfigKeys.STREAMING_WORKER_NAME in conf:
        streaming_config.worker_name = conf[
            NativeConfigKeys.STREAMING_WORKER_NAME]

    if NativeConfigKeys.STREAMING_WRITER_CONSUMED_STEP in conf:
        streaming_config.flow_control_config.writer_consumed_step = \
            int(conf[NativeConfigKeys.STREAMING_WRITER_CONSUMED_STEP])

    if NativeConfigKeys.STREAMING_READER_CONSUMED_STEP in conf:
        streaming_config.flow_control_config.reader_consumed_step = \
            int(conf[NativeConfigKeys.STREAMING_READER_CONSUMED_STEP])

    if NativeConfigKeys.STREAMING_FLOW_CONTROL_SEQ_ID_SCALE in conf:
        streaming_config.flow_control_config.flow_control_seq_id_scale = \
            int(conf[NativeConfigKeys.STREAMING_FLOW_CONTROL_SEQ_ID_SCALE])

    if NativeConfigKeys.STREAMING_READER_CONSUMED_STEP_UPDATER in conf:
        streaming_config.flow_control_config.reader_step_updater = \
            int(conf[NativeConfigKeys.STREAMING_READER_CONSUMED_STEP_UPDATER])

    if streaming_config.elastic_buffer_enable:
        esbufferfold = "es_buffer/"
        if NativeConfigKeys.ELASTICBUFFER_FILE_CACHE_SIZE in conf:
            streaming_config.elastic_buffer_config.file_cache_size = \
                int(conf[NativeConfigKeys.ELASTICBUFFER_FILE_CACHE_SIZE])
        else:
            streaming_config.elastic_buffer_config.file_cache_size = 1048576
        if NativeConfigKeys.ELASTICBUFFER_FLUSH_BUFFER_SIZE in conf:
            streaming_config.elastic_buffer_config.flush_buffer_size = \
                int(conf[NativeConfigKeys.ELASTICBUFFER_FLUSH_BUFFER_SIZE])
        else:
            streaming_config.elastic_buffer_config.flush_buffer_size = 800
        if NativeConfigKeys.ELASTICBUFFER_MAX_FILE_NUM in conf:
            streaming_config.elastic_buffer_config.max_file_num = \
                int(conf[NativeConfigKeys.ELASTICBUFFER_MAX_FILE_NUM])
        else:
            streaming_config.elastic_buffer_config.max_file_num = 100
        if NativeConfigKeys.ELASTICBUFFER_MAX_SAVE_BUFFER_SIZE in conf:
            streaming_config.elastic_buffer_config.max_save_buffer_size = \
                int(conf[NativeConfigKeys.ELASTICBUFFER_MAX_SAVE_BUFFER_SIZE])
        else:
            streaming_config.elastic_buffer_config.max_save_buffer_size = 1000
        if NativeConfigKeys.ELASTICBUFFER_FILE_DIRECTORY in conf:
            streaming_config.elastic_buffer_config.file_directory = \
                conf[NativeConfigKeys.ELASTICBUFFER_FILE_DIRECTORY]
        else:
            streaming_config.elastic_buffer_config.file_directory = \
                "{}/{}".format(EnvUtil.get_working_dir(), esbufferfold)
    logger.info("Streaming config in protobuf : {}".format(streaming_config))
    return streaming_config.SerializeToString()
