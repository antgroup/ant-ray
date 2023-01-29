#pragma once
#ifndef RAY_STREAMING_WRITER_H
#define RAY_STREAMING_WRITER_H

#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "buffer_pool/buffer_pool.h"
#include "channel/channel.h"
#include "config.h"
#include "event_service.h"
#include "flow_control/flow_control.h"
#include "http_profiler.h"
#include "message/message_bundle.h"
#include "queue/receiver.h"
#include "reliability/barrier_helper.h"
#include "reliability_helper.h"
#include "rescale/rescale.h"
#include "runtime_context.h"
#include "wal.h"

namespace ray {
namespace streaming {
class ReliabilityHelper;
class ExactlySameHelper;
class StreamingFlowControl;

class DataWriter : public Scalable, public DirectCallReceiver {
 public:
  // ExactlySameHelper need some writer private properties or methods
  // to put bundle meta info into storage outside.
  friend class ExactlySameHelper;

  // For http profiler access inner fields.
  friend class HttpProfiler;
  friend class MockWriter;

  DataWriter(std::shared_ptr<RuntimeContext> &runtime_context);

  virtual ~DataWriter();

  /*!
   * @brief streaming writer client initialization without raylet/local scheduler
   * @param queue_id_vec queue id vector
   * @param channel_message_id_vec channel msg id is related with message checkpoint
   * @param queue_size queue size (memory size not length)
   * @param abnormal_queues reamaining queue id vector
   */
  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::vector<StreamingQueueInitialParameter> &init_params,
                       const std::vector<uint64_t> &channel_message_id_vec,
                       const std::vector<uint64_t> &queue_size_vec);

  /*!
   * @brief new writer client in raylet
   * @param queue_id_vec
   * @param channel_msg_id_vec
   * @param queue_size
   * @param abnormal_queues reamaining queue id vector
   * @param create_types_vec channel creation type.
   */

  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::vector<StreamingQueueInitialParameter> &init_params,
                       const std::vector<uint64_t> &channel_msg_id_vec,
                       const std::vector<uint64_t> &queue_size_vec,
                       const std::vector<QueueCreationType> &create_types_vec);
  /*!
   * @brief To increase throughout, we merge a lot of message to a message bundle and no
   * message will be pushed into queue directly util daemon thread does this action.
   * Additionally, writing will block when buffer ring is full.
   *
   * Note: data should be a buffer in buffer pool, so bundle can be zero-copy.
   * @param q_id
   * @param data complete message including message header
   * @param data_size message size including message header
   * @param message_type
   * @return message message id which will be set into `data` and be returned.
   */
  uint64_t WriteMessageToBufferRing(
      const ObjectID &q_id, uint8_t *data, uint32_t data_size,
      StreamingMessageType message_type = StreamingMessageType::Message,
      bool use_disposable_buffer = false);

  /// \param q_id the queue which write message to.
  /// \param buffer the memory buffer holds use data.
  /// \param message_type message type
  /// \param in_buffer_pool whether the buffer is allocated in buffer pool or not.
  uint64_t WriteMessageToBufferRing(const ObjectID &q_id, MemoryBuffer buffer,
                                    StreamingMessageType message_type);

  /*!
   * @brief replay all queue from checkpoint, it's useful under FO
   * @param q_id_vec queue id vector
   * @param result offset vector
   * @return
   */
  void GetChannelOffset(const std::vector<ObjectID> &q_id_vec,
                        std::vector<uint64_t> &result);

  /*!
   * @brief fetch all channels back pressure ratio
   * @param channel_id_vec channel id vector
   * @param result backpressure ratio vector
   * @return
   */
  void GetChannelSetBackPressureRatio(const std::vector<ObjectID> &channel_id_vec,
                                      std::vector<double> &result);

  /*!
   * @brief send barrier to all channel
   * Note: there are user define data in barrier bundle
   * @param checkpoint_id : Its value is up to checkpoint operation. Actually it's useless
   * but logging.
   * @param barrier_id
   * @param data
   * @param data_size
   */
  void BroadcastBarrier(uint64_t checkpoint_id, uint64_t barrier_id, const uint8_t *data,
                        uint32_t data_size);

  /*!
   * @brief send UDC msg to all downstream channel.
   * @param udc_msg_id : The UDC msg id used to align.
   * @param data
   * @param data_size
   */
  void BroadcastUDCMsg(uint64_t udc_msg_id, const uint8_t *data, uint32_t data_size);

  /*
   * @brief broadcast partial barrier to all marked downstream.
   * @param global_barrier_id
   * @param partial_barrier_id
   * @param data
   * @param data_size
   */
  void BroadcastPartialBarrier(uint64_t global_barrier_id, uint64_t partial_barrier_id,
                               const uint8_t *data, uint32_t data_size);
  /*!
   * @brief To relieve stress from large source/input data, we define a new function
   * clear_check_point
   * in producer/writer class. Worker can invoke this function if and only if
   * notify_consumed each item
   * flag is passed in reader/consumer, which means writer's producing became more
   * rhythmical and reader
   * can't walk on old way anymore.
   * @param state_cp_id: operator state checkpoint id
   * @param barrier_id: user-defined numerical checkpoint id
   */
  void ClearCheckpoint(uint64_t state_cp_id, uint64_t queue_cp_id);
  void ClearPartialCheckpoint(uint64_t global_barrier_id, uint64_t partial_barrier_id);

  void Run();

  void Stop();

  std::shared_ptr<BufferPool> GetBufferPool(const ObjectID &qid);
  StreamingStatus GetMemoryBuffer(const ObjectID &qid, uint64_t min_size,
                                  MemoryBuffer &buffer, bool use_disposable_buffer);
  StreamingStatus GetBufferBlocked();

  void Rescale(const std::vector<ObjectID> &id_vec,
               const std::vector<ObjectID> &target_id_vec,
               std::vector<StreamingQueueInitialParameter> &actor_handle_vec) override;

  void Rescale(const std::vector<ObjectID> &id_vec,
               std::vector<StreamingQueueInitialParameter> &actor_handle_vec);

  void OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) override;
  std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer) override;
  void SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) override;

  void BroadcastEndOfDataBarrier();

  const StreamingConfig &GetConfig() const;

  std::string GetProfilingInfo() const;

 protected:
  // Registered for runtime metric reporter.
  void ReportTimer();
  void ScaleUp(
      const std::vector<ray::ObjectID> &id_vec,
      std::vector<StreamingQueueInitialParameter> &scale_actor_handle_vec) override;
  void ScaleDown(const std::vector<ray::ObjectID> &id_vec) override;
  void RefreshChannelInfo(ProducerChannelInfo &channel_info);
  ObjectID GetLastWriteQueueId() { return last_write_q_id_; }
  double GetChannelBackPressureRatio(const ObjectID &channel_id);
  uint64_t GetLastGlobalBarrierId() { return last_global_barrier_id_; }

 private:
  void EmptyMessageTimer();
  void FlowControlThread();
  bool WriteAllToChannel(ProducerChannelInfo *channel_info);
  bool SendEmptyToChannel(ProducerChannelInfo *channel_info);
  bool Reload(ProducerChannelInfo *channel_info);

  bool IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info);

  StreamingStatus WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain);

  void WriterLoopForward();

  /*!
   * @brief push empty message when no valid message or bundle was produced each time
   * interval
   * @param q_id, queue id
   * @param meta_ptr, it's resend empty bundle if meta pointer is non-null.
   */
  StreamingStatus WriteEmptyMessage(
      ProducerChannelInfo &channel_info,
      const StreamingMessageBundleMetaPtr &meta_ptr = nullptr);

  /// Set WriterQueue's eviction_limit_ and queue_limit_.
  void ClearCheckpointId(ProducerChannelInfo &channel_info, uint64_t msg_id);

  /// Set WriterQueue's queue_limit_, data less than it need to be put into ElasticBuffer.
  void NotifyRemoveLimit(ProducerChannelInfo &channel_info, uint64_t msg_id);

  StreamingStatus WriteBundleToChannel(ProducerChannelInfo &channel_info);

  /// The queue (and elasticbuffer) full, delete all the data have been sent
  void NotifyConsumed(ProducerChannelInfo &channel_info, StreamingStatus status);

  /// A wrapper of ProduceItemToChannel, handle FullChannel and produce item again.
  StreamingStatus ProduceItem(ProducerChannelInfo &channel_info,
                              StreamingMessageBundlePtr bundle);
  bool CollectFromRingBuffer(ProducerChannelInfo &channel_info, uint64_t &buffer_remain,
                             StreamingMessageList *message_list_ptr = nullptr,
                             const StreamingMessageBundleMetaPtr &meta_ptr = nullptr);

  StreamingStatus WriteChannelProcess(ProducerChannelInfo &channel_info,
                                      bool *is_empty_message, uint64_t &buffer_remain);

  /*!
   * @brief send barrier to all channel
   * Note: there are user define data in barrier bundle
   * @param barrier_id
   * @param data
   * @param data_size
   */
  void BroadcastBarrier(uint64_t barrier_id, const uint8_t *data, uint32_t data_size);

  StreamingStatus InitChannel(const ObjectID &q_id,
                              const StreamingQueueInitialParameter &param,
                              uint64_t channel_message_id, uint64_t queue_size);

  StreamingStatus DestroyChannel(const std::vector<ObjectID> &id_vec);

  void OnStartLogging(const ObjectID &queue_id, uint64_t msg_id, uint64_t seq_id,
                      uint64_t barrier_id);
  int32_t GetCyclicChannelCount(const std::vector<ObjectID> &queue_ids);

  void ClearCyclicChannelCheckpoint(const ObjectID queue_id, uint64_t checkpoint_id);

  inline bool IsTransferRunning();

 private:
  struct StreamingRescaleRAII {
    DataWriter *p_;
    explicit StreamingRescaleRAII(DataWriter *p) : p_(p) {
      STREAMING_LOG(INFO) << "runner suspened";
      {
        AutoSpinLock lock(p_->transfer_changing_);
        p_->runtime_context_->transfer_state_.SetRescaling();
      }
      p_->runtime_context_->ShutdownTimer();
      if (p_->runtime_context_->config_.GetTransferEventDriven()) {
        p_->event_service_->Stop();
        if (p_->empty_message_thread_ && p_->empty_message_thread_->joinable()) {
          p_->empty_message_thread_->join();
        }
        if (p_->flow_control_thread_ && p_->flow_control_thread_->joinable()) {
          p_->flow_control_thread_->join();
        }
      } else {
        p_->loop_thread_->join();
      }
    }

    ~StreamingRescaleRAII() {
      STREAMING_LOG(INFO) << "runner restart";
      p_->runtime_context_->transfer_state_.SetRunning();
      if (p_->runtime_context_->config_.GetTransferEventDriven()) {
        p_->event_service_->Run();
        p_->empty_message_thread_ =
            std::make_shared<std::thread>(&DataWriter::EmptyMessageTimer, p_);
        p_->flow_control_thread_ =
            std::make_shared<std::thread>(&DataWriter::FlowControlThread, p_);
      } else {
        p_->loop_thread_ =
            std::make_shared<std::thread>(&DataWriter::WriterLoopForward, p_);
      }
      p_->runtime_context_->EnableTimer(std::bind(&DataWriter::ReportTimer, p_));
    }
  };

  std::shared_ptr<EventService> event_service_;

  std::shared_ptr<std::thread> loop_thread_;
  std::shared_ptr<std::thread> flow_control_thread_;
  std::shared_ptr<std::thread> empty_message_thread_;
  std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> channel_map_;

  // One channel have unique identity.
  std::vector<ObjectID> output_queue_ids_;

  StreamingBarrierHelper barrier_helper_;

  std::shared_ptr<StreamingFlowControl> flow_controller_;
  std::shared_ptr<ReliabilityHelper> reliability_helper_;

  // This property is for debug if transfering blocked.
  // The verbose log in timer will report and show which channel is active,
  // which helps us to find some useful channel informations.
  ObjectID last_write_q_id_;

  // Make thread-safe between loop thread and user thread.
  // High-level runtime send notification about clear checkpoint if global
  // checkpoint is finished and low-level will auto flush & evict item memory
  // when no more space is available.
  std::atomic_flag notify_flag_ = ATOMIC_FLAG_INIT;

  // For accessing transfer info thread-safe.
  std::atomic_flag transfer_changing_ = ATOMIC_FLAG_INIT;

  // In daemon loop thread, the first phase is reloading meta data from global
  // storage outside if it's exactly same reliability. But it should not be
  // reopen while rescaling.
  bool reload_once = false;

  static constexpr uint32_t kQueueItemMaxBlocks = 10;

  int64_t last_bundle_unit_;

  std::mutex logging_mode_mutex_;

  uint64_t last_global_barrier_id_;

  bool get_buffer_blocked_ = false;
  ObjectID last_get_buffer_blocked_q_id_;

 protected:
  std::shared_ptr<RuntimeContext> runtime_context_;
  std::unordered_map<ObjectID, ProducerChannelInfo> channel_info_map_;
  std::shared_ptr<Config> transfer_config_;
  std::shared_ptr<QueueMessageHandler> queue_handler_;
  std::shared_ptr<AsyncWAL> async_wal_;
  CountingSemaphore cyclic_barrier_semaphore_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_WRITER_H
