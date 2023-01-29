# flake8: noqa

import logging

cimport raystreaming.includes.libstreaming as libstreaming
from cython.operator cimport dereference, postincrement
from libc.stdint cimport *
from libcpp.list cimport list as c_list
from libcpp.memory cimport shared_ptr, make_shared, dynamic_pointer_cast
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map as c_unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp cimport bool as c_bool
from ray._raylet cimport (
    Buffer,
    ActorID,
    ObjectRef,
    FunctionDescriptor,
)
from ray._raylet import JavaFunctionDescriptor
from ray.includes.common cimport (
    CRayFunction,
    LANGUAGE_PYTHON,
    LANGUAGE_JAVA,
    CBuffer
)
from ray.includes.unique_ids cimport (
    CObjectID
)
from raystreaming.includes.libstreaming cimport (
    CStreamingStatus,
    CStreamingMessage,
    CStreamingMessageBundle,
    CDataBundle,
    CDataWriter,
    CDataReader,
    CLocalMemoryBuffer,
    CChannelCreationParameter,
    CTransferCreationStatus,
    CConsumerChannelInfo,
    CStreamingBarrierHeader,
    CStreamingLogLevel,
    CRuntimeContext,
    set_streaming_log_config,
    kBarrierHeaderSize
)

channel_logger = logging.getLogger(__name__)


cdef class ChannelCreationParameter:
    cdef:
        CChannelCreationParameter parameter

    def __cinit__(self, ActorID actor_id,
                  FunctionDescriptor async_func,
                  FunctionDescriptor sync_func,
                  c_bool cyclic):
        cdef:
            shared_ptr[CRayFunction] async_func_ptr
            shared_ptr[CRayFunction] sync_func_ptr
        self.parameter = CChannelCreationParameter()
        self.parameter.actor_id = (<ActorID>actor_id).data
        if isinstance(async_func, JavaFunctionDescriptor):
            self.parameter.async_function =\
                make_shared[CRayFunction](LANGUAGE_JAVA, async_func.descriptor)
        else:
            self.parameter.async_function =\
                make_shared[CRayFunction](LANGUAGE_PYTHON, async_func.descriptor)
        if isinstance(sync_func, JavaFunctionDescriptor):
            self.parameter.sync_function =\
                make_shared[CRayFunction](LANGUAGE_JAVA, sync_func.descriptor)
        else:
            self.parameter.sync_function =\
                make_shared[CRayFunction](LANGUAGE_PYTHON, sync_func.descriptor)
        self.parameter.cyclic = cyclic


cdef class DataWriter:
    cdef:
        CDataWriter *writer

    def __init__(self):
        raise Exception("use create() to create DataWriter")

    @staticmethod
    def create(list py_output_channels: list[bytes],
               list channel_init_params: list[ChannelCreationParameter],
               uint64_t queue_size,
               list py_msg_ids: list[int],
               bytes config_bytes):
        assert len(py_output_channels) == len(channel_init_params)
        assert len(py_msg_ids) == len(py_msg_ids)
        cdef:
            c_vector[CObjectID] channel_ids = bytes_list_to_qid_vec(py_output_channels)
            c_vector[CChannelCreationParameter] init_param_vec
            ChannelCreationParameter param
            c_vector[uint64_t] msg_ids
            CDataWriter *c_writer
            shared_ptr[CRuntimeContext] c_runtime_context = make_shared[CRuntimeContext]()
            cdef const unsigned char[:] config_data
        for channel_init_param in channel_init_params:
            param = channel_init_param
            init_param_vec.push_back((<ChannelCreationParameter>param).parameter)
        for py_msg_id in py_msg_ids:
            msg_ids.push_back(<uint64_t>py_msg_id)
        if config_bytes:
            config_data = config_bytes
            channel_logger.info("DataWriter load config, config bytes size: %s", config_data.nbytes)
            c_runtime_context.get().SetConfig(<uint8_t *>(&config_data[0]), <size_t>config_data.nbytes)
        cdef:
            CStreamingStatus writer_init_status;
            c_vector[uint64_t] queue_size_vec
        for i in range(channel_ids.size()):
            queue_size_vec.push_back(queue_size)
        cdef CStreamingStatus status
        c_writer = new CDataWriter(c_runtime_context)
        with nogil:
            status = c_writer.Init(channel_ids, init_param_vec, msg_ids, queue_size_vec)
        if <uint32_t>status != <uint32_t> libstreaming.StatusOK:
            msg = "initialize writer failed, status={}".format(<uint32_t>status)
            channel_logger.error(msg)
            del c_writer
            import raystreaming.runtime.transfer as transfer
            raise transfer.ChannelInitException(msg)

        c_writer.Run()
        channel_logger.info("create native writer succeed")
        cdef DataWriter writer = DataWriter.__new__(DataWriter)
        writer.writer = c_writer
        return writer

    def __dealloc__(self):
        if self.writer != NULL:
            del self.writer
            channel_logger.info("deleted DataWriter")
            self.writer = NULL

    def write(self, ObjectRef qid, const unsigned char[:] value):
        """support zero-copy bytes, bytearray, array of unsigned char"""
        cdef:
            CObjectID native_id = qid.data
            uint64_t msg_id
            uint8_t *data = <uint8_t *>(&value[0])
            uint32_t size = value.nbytes
        with nogil:
            msg_id = self.writer.WriteMessageToBufferRing(native_id, data, size)
        return msg_id

    def broadcast_barrier(self,
                          uint64_t checkpoint_id,
                          uint64_t barrier_id,
                          const unsigned char[:] data):
        cdef size_t size = <size_t>data.nbytes
        with nogil:
            self.writer.BroadcastBarrier(
                checkpoint_id, barrier_id,
                <uint8_t *>(&data[0]), size)

    def broadcast_partial_barrier(self,
                          uint64_t global_barier_id,
                          uint64_t partial_barrier_id,
                          const unsigned char[:] data):
        cdef size_t size = <size_t>data.nbytes
        with nogil:
            self.writer.BroadcastPartialBarrier(
                global_barier_id, partial_barrier_id,
                <uint8_t *>(&data[0]), size)

    def get_output_seq_id(self, py_output_channels: list[bytes]):
        cdef:
            c_vector[CObjectID] channel_ids = bytes_list_to_qid_vec(py_output_channels)
            c_vector[uint64_t] result
        self.writer.GetChannelOffset(channel_ids, result)
        output_seq_ids_map = {}
        for i in range(channel_ids.size()):
            k = channel_ids[i].Binary()
            v = result[i]
            output_seq_ids_map[k] = v
        return output_seq_ids_map

    def get_backpressure_ratio(self, py_output_channels: list[bytes]):
        cdef:
            c_vector[CObjectID] channel_ids = bytes_list_to_qid_vec(py_output_channels)
            c_vector[double] result
        self.writer.GetChannelSetBackPressureRatio(channel_ids, result)
        backpressure_ratio_map = {}
        for i in range(channel_ids.size()):
            k = channel_ids[i].Binary()
            v = result[i]
            backpressure_ratio_map[k] = v
        return backpressure_ratio_map


    def clear_checkpoint(self, uint64_t state_cp_id, uint64_t queue_cp_id):
        self.writer.ClearCheckpoint(state_cp_id, queue_cp_id)

    def on_writer_message(self, const unsigned char[:] value):
        cdef:
            c_vector[shared_ptr[CLocalMemoryBuffer]] buffers
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
        buffers.push_back(local_buf)
        with nogil:
            self.writer.OnMessage(buffers)

    def on_writer_message_sync(self, const unsigned char[:] value):
        cdef:
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
            shared_ptr[CLocalMemoryBuffer] result_buffer
        with nogil:
            result_buffer = self.writer.OnMessageSync(local_buf)
        return Buffer.make(dynamic_pointer_cast[CBuffer, CLocalMemoryBuffer](result_buffer))

    def stop(self):
        with nogil:
            self.writer.Stop()
        channel_logger.info("stopped DataWriter")

    def broadcast_end_of_data_barrier(self):
        with nogil:
            self.writer.BroadcastEndOfDataBarrier()

    def rescale(self, list py_output_queues, py_collect_ids, rescale_channel_params):
        cdef:
            c_vector[CObjectID] queue_id_vec = bytes_list_to_qid_vec(py_output_queues)
            c_vector[CObjectID] collect_id_vec = bytes_list_to_qid_vec(py_collect_ids)
            c_vector[CChannelCreationParameter] rescale_channel_param_vec
        for param in rescale_channel_params:
            rescale_channel_param_vec.push_back((<ChannelCreationParameter>param).parameter)
        self.writer.Rescale(queue_id_vec, collect_id_vec, rescale_channel_param_vec)

    def clear_partial_checkpoint(self,
                                 uint64_t global_checkpoint_id,
                                 uint64_t partial_checkpoint_id):
        self.writer.ClearPartialCheckpoint(
            global_checkpoint_id, partial_checkpoint_id)
        channel_logger.info("Clear wrtier partial checkpoint done.")

    def fetch_profiling_infos(self):
        cdef c_string debug_infos = b"{}"
        with nogil:
            if self.writer:
                debug_infos = self.writer.GetProfilingInfo()

        return str(debug_infos)

cdef class DataReader:
    cdef:
        CDataReader *reader
        readonly bytes meta
        readonly bytes data

    def __init__(self):
        raise Exception("use create() to create DataReader")

    @staticmethod
    def create(list py_input_queues,
               list channel_init_params: list[ChannelCreationParameter],
               list py_msg_ids,
               int timer_interval,
               bytes config_bytes):
        assert len(py_input_queues) == len(channel_init_params)
        assert len(py_input_queues) == len(py_msg_ids)
        cdef:
            c_vector[CObjectID] queue_id_vec = bytes_list_to_qid_vec(py_input_queues)
            c_vector[CChannelCreationParameter] init_param_vec
            c_vector[uint64_t] msg_ids
            cdef const unsigned char[:] config_data
            CDataReader *c_reader
            shared_ptr[CRuntimeContext] c_runtime_context = make_shared[CRuntimeContext]()
        for channel_init_param in channel_init_params:
            param = channel_init_param
            init_param_vec.push_back((<ChannelCreationParameter>param).parameter)
        for py_msg_id in py_msg_ids:
            msg_ids.push_back(<uint64_t>py_msg_id)
        cdef c_vector[CTransferCreationStatus] creation_status
        if config_bytes:
            config_data = config_bytes
            channel_logger.info("DataReader load config, config bytes size: %s", config_data.nbytes)
            c_runtime_context.get().SetConfig(<uint8_t *>(&(config_data[0])), config_data.nbytes)
        c_reader = new CDataReader(c_runtime_context)
        with nogil:
            c_reader.Init(queue_id_vec, init_param_vec, msg_ids, timer_interval, creation_status)

        creation_status_map = {}
        if not creation_status.empty():
            for i in range(queue_id_vec.size()):
                k = queue_id_vec[i].Binary()
                v = <uint64_t>creation_status[i]
                creation_status_map[k] = v
        channel_logger.info("create native reader succeed")
        cdef DataReader reader = DataReader.__new__(DataReader)
        reader.reader = c_reader
        return reader, creation_status_map

    def __dealloc__(self):
        if self.reader != NULL:
            del self.reader
            channel_logger.info("deleted DataReader")
            self.reader = NULL

    def read(self, uint32_t timeout_millis):
        cdef:
            shared_ptr[CDataBundle] bundle
            CStreamingStatus status
        with nogil:
            status = self.reader.GetBundle(timeout_millis, bundle)
        if <uint32_t> status != <uint32_t> libstreaming.StatusOK:
            if <uint32_t> status == <uint32_t> libstreaming.StatusInterrupted:
                # avoid cyclic import
                import raystreaming.runtime.transfer as transfer
                raise transfer.ChannelInterruptException("reader interrupted")
            elif <uint32_t> status == <uint32_t> libstreaming.StatusInitQueueFailed:
                raise Exception("init channel failed")
            elif <uint32_t> status == <uint32_t> libstreaming.StatusGetBundleTimeOut:
                return []
            else:
                raise Exception("no such status " + str(<uint32_t>status))
        cdef:
            uint32_t msg_nums
            CObjectID queue_id = bundle.get().c_from
            c_list[shared_ptr[CStreamingMessage]] msg_list
            list msgs = []
            uint64_t timestamp
            uint64_t msg_id
            c_unordered_map[CObjectID, CConsumerChannelInfo] *offset_map = NULL
            shared_ptr[CStreamingMessage] barrier
            CStreamingBarrierHeader barrier_header
            c_unordered_map[CObjectID, CConsumerChannelInfo].iterator it

        cdef uint32_t bundle_type = <uint32_t>(bundle.get().meta.get().GetBundleType())
        # avoid cyclic import
        from raystreaming.runtime.transfer import DataMessage
        if bundle_type == <uint32_t> libstreaming.BundleTypeBundle:
            msg_nums = bundle.get().meta.get().GetMessageListSize()
            CStreamingMessageBundle.GetMessageListFromRawData(
                bundle.get().DataBuffer(),
                bundle.get().DataSize(),
                msg_nums,
                msg_list)
            timestamp = bundle.get().meta.get().GetMessageBundleTs()
            for msg in msg_list:
                msg_bytes = msg.get().Payload()[:msg.get().PayloadSize()]
                qid_bytes = queue_id.Binary()
                msg_id = msg.get().GetMessageId()
                msgs.append(
                    DataMessage(msg_bytes, timestamp, msg_id, qid_bytes))
            return msgs
        elif bundle_type == <uint32_t> libstreaming.BundleTypeEmpty:
            timestamp = bundle.get().meta.get().GetMessageBundleTs()
            msg_id = bundle.get().meta.get().GetLastMessageId()
            return [DataMessage(None, timestamp, msg_id, queue_id.Binary(), True)]
        elif bundle.get().meta.get().IsBarrier():
            py_offset_map = {}
            self.reader.GetOffsetInfo(offset_map)
            it = offset_map.begin()
            while it != offset_map.end():
                queue_id_bytes = dereference(it).first.Binary()
                current_message_id = dereference(it).second.current_message_id
                py_offset_map[queue_id_bytes] = current_message_id
                postincrement(it)
            msg_nums = bundle.get().meta.get().GetMessageListSize()
            CStreamingMessageBundle.GetMessageListFromRawData(
                bundle.get().DataBuffer(),
                bundle.get().DataSize(),
                msg_nums,
                msg_list)
            timestamp = bundle.get().meta.get().GetMessageBundleTs()
            barrier = msg_list.front()
            msg_id = barrier.get().GetMessageId()
            CStreamingMessage.GetBarrierIdFromRawData(barrier.get().Payload(), &barrier_header)
            barrier_id = barrier_header.barrier_id
            barrier_data = (barrier.get().Payload() + kBarrierHeaderSize)[
                           :barrier.get().PayloadSize() - kBarrierHeaderSize]
            partial_barrier_id = barrier_header.partial_barrier_id
            barrier_type = <uint64_t> barrier_header.barrier_type
            py_queue_id = queue_id.Binary()
            from raystreaming.runtime.transfer import CheckpointBarrier
            return [CheckpointBarrier(
                barrier_data, timestamp, msg_id, py_queue_id, py_offset_map,
                barrier_id, partial_barrier_id, barrier_type)]
        else:
            raise Exception("Unsupported bundle type {}".format(bundle_type))

    def clear_checkpoint(self, uint64_t queue_cp_id):
        channel_logger.info("clear checkpoint done[noop].")

    def rescale(self, list py_input_queues, py_collect_ids, rescale_channel_params):
        cdef:
            c_vector[CObjectID] queue_id_vec = bytes_list_to_qid_vec(py_input_queues)
            c_vector[CObjectID] collect_id_vec = bytes_list_to_qid_vec(py_collect_ids)
            c_vector[CChannelCreationParameter] rescale_channel_param_vec
        for param in rescale_channel_params:
            rescale_channel_param_vec.push_back((<ChannelCreationParameter>param).parameter)
        self.reader.Rescale(queue_id_vec, collect_id_vec, rescale_channel_param_vec)

    def rescale_rollback(self):
        self.reader.RescaleRollback()

    def clear_partial_checkpoint(self,
                                 uint64_t global_checkpoint_id,
                                 uint64_t partial_checkpoint_id):
        self.reader.ClearPartialCheckpoint(
            global_checkpoint_id, partial_checkpoint_id)
        channel_logger.info("clear partial checkpoint done.")

    def on_reader_message(self,
                          const unsigned char[:] item_meta_buffer,
                          const unsigned char[:] bundle_meta_buffer,
                          const unsigned char[:] bundle_data_buffer):
        cdef:
            c_vector[shared_ptr[CLocalMemoryBuffer]] buffers
            shared_ptr[CLocalMemoryBuffer] native_item_meta_buffer = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(
                    &item_meta_buffer[0]), <size_t>item_meta_buffer.nbytes, True)
            shared_ptr[CLocalMemoryBuffer] native_bundle_meta_buffer = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(
                    &bundle_meta_buffer[0]), <size_t>bundle_meta_buffer.nbytes, True)
            shared_ptr[CLocalMemoryBuffer] native_bundle_data_buffer = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(
                    &bundle_data_buffer[0]), <size_t>bundle_data_buffer.nbytes, True)
        buffers.push_back(native_item_meta_buffer)
        buffers.push_back(native_bundle_meta_buffer)
        buffers.push_back(native_bundle_data_buffer)
        with nogil:
            self.reader.OnMessage(buffers)

    def on_reader_message_sync(self, const unsigned char[:] value):
        cdef:
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
            shared_ptr[CLocalMemoryBuffer] result_buffer
        with nogil:
            result_buffer = self.reader.OnMessageSync(local_buf)
        return Buffer.make(dynamic_pointer_cast[CBuffer, CLocalMemoryBuffer](result_buffer))

    def stop(self):
        with nogil:
            self.reader.Stop()
        channel_logger.info("stopped DataReader")


    def fetch_profiling_infos(self):
        cdef c_string debug_infos = b"{}"
        with nogil:
            if self.reader:
                debug_infos = self.reader.GetProfilingInfo()
        
        return str(debug_infos)

cdef c_vector[CObjectID] bytes_list_to_qid_vec(list py_queue_ids) except *:
    assert len(py_queue_ids) >= 0
    cdef:
        c_vector[CObjectID] queue_id_vec
        c_string q_id_data
    for q_id in py_queue_ids:
        q_id_data = q_id
        assert q_id_data.size() == CObjectID.Size(), f"{q_id_data.size()}, {CObjectID.Size()}"
        obj_id = CObjectID.FromBinary(q_id_data)
        queue_id_vec.push_back(obj_id)
    return queue_id_vec


def set_log_config(int log_level):
    cdef c_string app_name = b"streaming"
    set_streaming_log_config(app_name, <CStreamingLogLevel>(log_level))
