# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# flake8: noqa

from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector as c_vector
from libcpp.unordered_map cimport unordered_map as c_unordered_map
from libcpp.list cimport list as c_list
from cpython cimport PyObject
cimport cpython

cdef inline object PyObject_to_object(PyObject* o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result

from ray.includes.common cimport (
    CLanguage,
    CRayObject,
    CRayStatus,
    CRayFunction
)

from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CTaskID,
    CObjectID,
)


cdef extern from "common/buffer.h" nogil:
    cdef cppclass CLocalMemoryBuffer "ray::streaming::LocalMemoryBuffer":
        CLocalMemoryBuffer(uint8_t *data, size_t size, c_bool copy)
        uint8_t *Data() const
        uint64_t Size() const

    cdef cppclass CMemoryBuffer "ray::streaming::MemoryBuffer":
        uint8_t *Data() const
        uint64_t Size() const


cdef extern from "logging.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingLogLevel "ray::streaming::StreamingLogLevel":
        pass

cdef extern from "streaming.h" namespace "ray::streaming" nogil:
    cdef void set_streaming_log_config(
            const c_string app_name,
            const CStreamingLogLevel log_level)

cdef extern from "common/status.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingStatus "ray::streaming::StreamingStatus":
        pass
    cdef CStreamingStatus StatusOK "ray::streaming::StreamingStatus::OK"
    cdef CStreamingStatus StatusQueueIdNotFound "ray::streaming::StreamingStatus::QueueIdNotFound"
    cdef CStreamingStatus StatusEmptyRingBuffer "ray::streaming::StreamingStatus::EmptyRingBuffer"
    cdef CStreamingStatus StatusFullChannel "ray::streaming::StreamingStatus::FullChannel"
    cdef CStreamingStatus StatusNoSuchItem "ray::streaming::StreamingStatus::NoSuchItem"
    cdef CStreamingStatus StatusInitQueueFailed "ray::streaming::StreamingStatus::InitQueueFailed"
    cdef CStreamingStatus StatusGetBundleTimeOut "ray::streaming::StreamingStatus::GetBundleTimeOut"
    cdef CStreamingStatus StatusSkipSendEmptyMessage "ray::streaming::StreamingStatus::SkipSendEmptyMessage"
    cdef CStreamingStatus StatusInterrupted "ray::streaming::StreamingStatus::Interrupted"
    cdef CStreamingStatus StatusOutOfMemory "ray::streaming::StreamingStatus::OutOfMemory"
    cdef CStreamingStatus StatusInvalid "ray::streaming::StreamingStatus::Invalid"
    cdef CStreamingStatus StatusUnknownError "ray::streaming::StreamingStatus::UnknownError"
    cdef CStreamingStatus StatusTailStatus "ray::streaming::StreamingStatus::TailStatus"

cdef extern from "runtime_context.h" namespace "ray::streaming" nogil:
    cdef cppclass CRuntimeContext "ray::streaming::RuntimeContext":
        void SetConfig(const uint8_t *, uint32_t size)


cdef extern from "streaming_config.h" namespace "ray::streaming" nogil:
    cdef cppclass CTransferQueueType "ray::streaming::TransferQueueType":
        pass
    cdef CTransferQueueType CTransferQueueTypeMockQueue "ray::streaming::TransferQueueType::MOCK_QUEUE"
    cdef CTransferQueueType CTransferQueueTypeStreamingQueue "ray::streaming::TransferQueueType::STREAMING_QUEUE"

    cdef cppclass CStreamingQueueCreationType "ray::streaming::StreamingQueueCreationType":
        pass
    cdef CStreamingQueueCreationType CStreamingQueueCreationTypeRecreate "ray::streaming::StreamingQueueCreationType::RECREATE"
    cdef CStreamingQueueCreationType CStreamingQueueCreationTypeReconstruct "ray::streaming::CTransferQueueType::RECONSTRUCT"
    cdef CStreamingQueueCreationType CStreamingQueueCreationTypeReconstructAndClear "ray::streaming::CTransferQueueType::RECREATE_AND_CLEAR"


cdef extern from "message/message.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageType "ray::streaming::StreamingMessageType":
        pass
    cdef CStreamingMessageType MessageTypeBarrier "ray::streaming::StreamingMessageType::Barrier"
    cdef CStreamingMessageType MessageTypeMessage "ray::streaming::StreamingMessageType::Message"
    cdef cppclass CStreamingMessage "ray::streaming::StreamingMessage":
        inline uint8_t *Payload() const
        inline uint32_t PayloadSize() const
        inline CStreamingMessageType GetMessageType() const
        inline uint64_t GetMessageId() const
        @staticmethod
        inline void GetBarrierIdFromRawData(const uint8_t *data,
                                            CStreamingBarrierHeader *barrier_header)
    cdef struct CStreamingBarrierHeader "ray::streaming::StreamingBarrierHeader":
        CStreamingBarrierType barrier_type;
        uint64_t barrier_id;
        uint64_t partial_barrier_id;
        inline c_bool IsGlobalBarrier();
        inline c_bool IsPartialBarrier();
    cdef cppclass CStreamingBarrierType "ray::streaming::StreamingBarrierType":
        pass
    cdef CStreamingBarrierType CGlobalBarrier "ray::streaming::StreamingBarrierType::GlobalBarrier"
    cdef CStreamingBarrierType CPartialBarrier "ray::streaming::StreamingBarrierType::PartialBarrier"
    cdef uint32_t kMessageHeaderSize;
    cdef uint32_t kBarrierHeaderSize;

cdef extern from "message/message_bundle.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageBundleType "ray::streaming::StreamingMessageBundleType":
        pass
    cdef CStreamingMessageBundleType BundleTypeEmpty "ray::streaming::StreamingMessageBundleType::Empty"
    cdef CStreamingMessageBundleType BundleTypeBarrier "ray::streaming::StreamingMessageBundleType::Barrier"
    cdef CStreamingMessageBundleType BundleTypeBundle "ray::streaming::StreamingMessageBundleType::Bundle"

    cdef cppclass CStreamingMessageBundleMeta "ray::streaming::StreamingMessageBundleMeta":
        CStreamingMessageBundleMeta()
        inline uint64_t GetMessageBundleTs() const
        inline uint64_t GetLastMessageId() const
        inline uint32_t GetMessageListSize() const
        inline CStreamingMessageBundleType GetBundleType() const
        inline c_bool IsBarrier()
        inline c_bool IsBundle()

    ctypedef shared_ptr[CStreamingMessageBundleMeta] CStreamingMessageBundleMetaPtr
    uint32_t kMessageBundleHeaderSize "ray::streaming::kMessageBundleHeaderSize"
    cdef cppclass CStreamingMessageBundle "ray::streaming::StreamingMessageBundle"(CStreamingMessageBundleMeta):
         @staticmethod
         void GetMessageListFromRawData(const uint8_t *data, uint32_t size, uint32_t msg_nums,
                                        c_list[shared_ptr[CStreamingMessage]] &msg_list);

    cdef cppclass CDataBundle "ray::streaming::StreamingReaderBundle":
        inline uint8_t *DataBuffer()
        inline uint64_t DataSize()
        inline uint8_t *MetaBuffer()
        CObjectID c_from "from"
        uint64_t bundle_id
        uint64_t last_barrier_id
        CStreamingMessageBundleMetaPtr meta
        uint64_t timestamp_push


cdef extern from "channel.h" namespace "ray::streaming" nogil:
    struct CChannelCreationParameter "ray::streaming::StreamingQueueInitialParameter":
        CActorID actor_id;
        shared_ptr[CRayFunction] async_function;
        shared_ptr[CRayFunction] sync_function;
        c_bool cyclic;

    struct CStreamingQueueInfo "ray::streaming::StreamingQueueInfo":
        uint64_t first_seq_id;
        uint64_t last_seq_id;
        uint64_t target_seq_id;
        uint64_t consumed_seq_id;
        uint64_t unconsumed_bytes;

    struct CConsumerChannelInfo "ray::streaming::ConsumerChannelInfo":
        CObjectID channel_id;
        uint64_t current_message_id;
        uint64_t current_seq_id;
        uint64_t barrier_id;
        uint64_t partial_barrier_id;
        CStreamingQueueInfo queue_info;
        uint64_t last_queue_item_delay;
        uint64_t last_queue_item_latency;
        uint64_t last_queue_target_diff;
        uint64_t get_queue_item_times;
        uint64_t notify_cnt;
        uint64_t max_notified_msg_id;
        CChannelCreationParameter parameter;
        uint64_t last_queue_item_read_latency;
        uint64_t last_queue_item_read_latency_large_than_10ms;
        uint64_t last_queue_item_read_latency_large_than_5ms;
        uint64_t last_bundle_merger_latency;
        uint64_t last_bundle_merger_latency_large_than_10ms;
        uint64_t last_bundle_merger_latency_large_than_5ms;


    cdef cppclass CTransferCreationStatus "ray::streaming::TransferCreationStatus":
        pass
    cdef CTransferCreationStatus CTransferCreationStatusFreshStarted "ray::streaming::TransferCreationStatus::FreshStarted"
    cdef CTransferCreationStatus CTransferCreationStatusPullOk "ray::streaming::TransferCreationStatus::PullOk"
    cdef CTransferCreationStatus CTransferCreationStatusTimeout "ray::streaming::TransferCreationStatus::Timeout"
    cdef CTransferCreationStatus CTransferCreationStatusDataLost "ray::streaming::TransferCreationStatus::DataLost"
    cdef CTransferCreationStatus CTransferCreationStatusFailed "ray::streaming::TransferCreationStatus::Failed"
    cdef CTransferCreationStatus CTransferCreationStatusInvalid "ray::streaming::TransferCreationStatus::Invalid"


cdef extern from "buffer_pool/buffer_pool.h" namespace "ray::streaming" nogil:
    cdef cppclass CBufferPool "ray::streaming::BufferPool":
        CStreamingStatus GetBuffer(CMemoryBuffer *buffer);
        CStreamingStatus GetBuffer(uint64_t min_size, CMemoryBuffer *buffer);
        CStreamingStatus GetBufferBlockedTimeout(uint64_t min_size, CMemoryBuffer *buffer, uint64_t timeout);


cdef extern from "rescale/rescale.h" namespace "ray::streaming" nogil:
    cdef cppclass CScalable "ray::streaming::Scalable":
        void Rescale(const c_vector[CObjectID] &id_vec,
                     const c_vector[CObjectID] &target_id_vec,
                     const c_vector[CChannelCreationParameter] &init_params)
        void RescaleRollback()


cdef extern from "data_reader.h" namespace "ray::streaming" nogil:
    cdef cppclass CDataReader "ray::streaming::DataReader"(CScalable):
        CDataReader(shared_ptr[CRuntimeContext] &runtime_context)

        void Stop()

        void Init(const c_vector[CObjectID] &input_ids,
                  const c_vector[CChannelCreationParameter] &init_params,
                  const c_vector[uint64_t] &streaming_msg_ids, int64_t timer_interval,
                  c_vector[CTransferCreationStatus] &creation_status);

        void Init(const c_vector[CObjectID] &input_ids,
                 const c_vector[CChannelCreationParameter] &init_params,
                 int64_t timer_interval, c_bool is_rescale=False);

        CStreamingStatus GetBundle(const uint32_t timeout_ms,
                                   shared_ptr[CDataBundle] &message);

        void GetOffsetInfo(c_unordered_map[CObjectID, CConsumerChannelInfo] *&offset_map);

        void NotifyConsumedItem(CConsumerChannelInfo &channel_info, uint64_t offset);

        void NotifyConsumed(shared_ptr[CDataBundle] &message);

        void ClearPartialCheckpoint(uint64_t global_barrier_id, uint64_t partial_barrier_id);

        void OnMessage(c_vector[shared_ptr[CLocalMemoryBuffer]] buffers);

        shared_ptr[CLocalMemoryBuffer] OnMessageSync(shared_ptr[CLocalMemoryBuffer] buffer);

        c_string GetProfilingInfo();


cdef extern from "data_writer.h" namespace "ray::streaming" nogil:
    cdef cppclass CDataWriter "ray::streaming::DataWriter"(CScalable):
        CDataWriter(shared_ptr[CRuntimeContext] &runtime_context)

        CStreamingStatus Init(const c_vector[CObjectID] &channel_ids,
                              const c_vector[CChannelCreationParameter] &init_params,
                              const c_vector[uint64_t] &message_ids,
                              const c_vector[uint64_t] &queue_size_vec);

        long WriteMessageToBufferRing(
                const CObjectID &q_id, uint8_t *data, uint32_t data_size)

        void GetChannelOffset(const c_vector[CObjectID] &q_id_vec,
                              c_vector[uint64_t] &result);


        void GetChannelSetBackPressureRatio(const c_vector[CObjectID] &channel_id_vec,
                                           c_vector[double] &result);

        void BroadcastBarrier(uint64_t checkpoint_id, uint64_t barrier_id, const uint8_t *data,
                              uint32_t data_size);

        void BroadcastPartialBarrier(uint64_t global_barrier_id, uint64_t partial_barrier_id,
                                     const uint8_t *data, uint32_t data_size);

        void ClearCheckpoint(uint64_t state_cp_id, uint64_t queue_cp_id);
        void ClearPartialCheckpoint(uint64_t global_barrier_id, uint64_t partial_barrier_id);

        void Run();

        void Stop();

        shared_ptr[CBufferPool] GetBufferPool(const CObjectID &qid);

        void OnMessage(c_vector[shared_ptr[CLocalMemoryBuffer]] buffers);

        shared_ptr[CLocalMemoryBuffer] OnMessageSync(shared_ptr[CLocalMemoryBuffer] buffer);

        void BroadcastEndOfDataBarrier();

        c_string GetProfilingInfo();
