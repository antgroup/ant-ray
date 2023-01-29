#ifndef RAY_STREAMING_CHANNEL_H
#define RAY_STREAMING_CHANNEL_H
#include <list>

#include "buffer_pool/buffer_pool.h"
#include "common/status.h"
#include "message/message_bundle.h"
#include "queue_handler.h"
#include "ray/common/id.h"
#include "ring_buffer.h"
#include "streaming_config.h"
#include "util/config.h"

namespace ray {
namespace streaming {

enum class TransferCreationStatus : uint32_t {
  FreshStarted = 0,
  PullOk = 1,
  Timeout = 2,
  DataLost = 3,
  Failed = 4,  // for plasma queue only
  Invalid = 999,
};

static inline std::ostream &operator<<(std::ostream &os,
                                       const TransferCreationStatus &status) {
  os << static_cast<std::underlying_type<TransferCreationStatus>::type>(status);
  return os;
}

class TransferState {
 public:
  enum TransferStateEnum { Init = 0, Running = 1, Interrupted = 2, Rescaling = 3 };
  TransferState() : state_(Init) {}
  constexpr TransferState(const TransferState &stream_transfer_state)
      : state_(stream_transfer_state.state_) {}
  constexpr TransferState(const TransferStateEnum state) : state_(state) {}
  constexpr bool operator==(TransferState transfer_state) const {
    return state_ == transfer_state.state_;
  }
#define DEF_STATE_CHECK_AND_SET(S)                  \
  inline bool Is##S() const { return state_ == S; } \
  inline void Set##S() { state_ = S; }

  DEF_STATE_CHECK_AND_SET(Init)
  DEF_STATE_CHECK_AND_SET(Running)
  DEF_STATE_CHECK_AND_SET(Interrupted)
  DEF_STATE_CHECK_AND_SET(Rescaling)

  const TransferStateEnum GetTransferState() const { return state_; }

 private:
  TransferStateEnum state_;
};

static inline std::ostream &operator<<(std::ostream &os,
                                       const streaming::TransferState &s_transfer_state) {
  os << static_cast<int>(s_transfer_state.GetTransferState());
  return os;
}

struct StreamingMessageList {
  std::list<StreamingMessagePtr> message_list;
  uint64_t bundle_size = 0;
};

struct StreamingQueueInfo {
  // First message id in channel.
  uint64_t first_message_id = 0;
  // Last message id in channel.
  uint64_t last_message_id = 0;
  // Target message id for flow control with consuemd window size.
  uint64_t target_message_id = 0;
  // Message id in last upstream notification.
  uint64_t consumed_message_id = 0;
  // Unconsumed byte size.
  uint64_t unconsumed_bytes = 0;
  // Last bundle id consumed by downstream.
  uint64_t consumed_bundle_id = -1;
};

using StreamingRingBufferPtr = std::shared_ptr<StreamingRingBuffer<StreamingMessagePtr>>;
struct ProducerChannelInfo {
  ObjectID channel_id;
  StreamingRingBufferPtr writer_ring_buffer;
  // Lastest message id of last bundle.
  uint64_t current_message_id;
  // Lastest bundle id of last bundle.
  uint64_t current_bundle_id;
  /// Next bundle need to be written to channel
  StreamingMessageBundlePtr bundle;
  // Lastest message id in end of channel.
  uint64_t message_last_commit_id;
  uint64_t queue_size;
  QueueCreationType queue_creation_type;
  StreamingQueueInfo queue_info;
  // Last transporting timestamp.
  int64_t message_pass_by_ts;
  // Initial message id.
  uint64_t initial_message_id;

  // For Event-Driven
  uint64_t sent_empty_cnt = 0;
  uint64_t flow_control_cnt = 0;
  uint64_t user_event_cnt = 0;
  uint64_t queue_full_cnt = 0;
  uint64_t in_event_queue_cnt = 0;
  std::atomic<bool> in_event_queue;
  std::atomic<bool> flow_control;

  StreamingQueueInitialParameter parameter;

  WriterQueueProfilingInfo debug_infos;
};

struct ConsumerChannelInfo {
  ObjectID channel_id;
  uint64_t current_message_id;
  uint64_t barrier_id;
  uint64_t partial_barrier_id;
  uint64_t udc_msg_barrier_id;

  StreamingQueueInfo queue_info;

  uint64_t last_queue_item_delay;
  uint64_t last_queue_target_diff;
  uint64_t get_queue_item_times;
  uint64_t notify_cnt;
  uint64_t max_notified_msg_id;
  StreamingQueueInitialParameter parameter;
  uint64_t last_queue_item_read_latency;
  uint64_t last_queue_item_read_latency_large_than_10ms;
  uint64_t last_queue_item_read_latency_large_than_5ms;
  uint64_t last_bundle_merger_latency;
  uint64_t last_bundle_merger_latency_large_than_10ms;
  uint64_t last_bundle_merger_latency_large_than_5ms;
  uint64_t resend_notify_timer;

  ReaderQueueProfilingInfo debug_infos;
};

/// Two types of channel are presented:
///   * ProducerChannel is supporting all writing operations for upperlevel.
///   * ConsumerChannel is for all reader operations.
///  They share similar interfaces:
///    * ClearTransferCheckpoint(it's empty and unsupported now, we will add
///      implementation in next PR)
///    * NotifychannelConsumed (notify owner of channel which range data should
//       be release to avoid out of memory)
///  but some differences in read/write function.(named ProduceItemTochannel and
///  ConsumeItemFrom channel)
class ProducerChannel {
 public:
  explicit ProducerChannel(std::shared_ptr<Config> &transfer_config,
                           ProducerChannelInfo &p_channel_info);
  virtual ~ProducerChannel() = default;
  virtual StreamingStatus CreateTransferChannel() = 0;
  virtual StreamingStatus DestroyTransferChannel() = 0;
  virtual StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size,
                                               uint64_t start_id, uint64_t end_id) = 0;
  virtual StreamingStatus ProduceItemToChannel(StreamingMessageBundlePtr &bundle) = 0;
  virtual StreamingStatus NotifyChannelConsumed(uint64_t channel_offset) = 0;
  virtual StreamingStatus NotifyRemoveLimit(uint64_t channel_offset) = 0;
  virtual void RefreshChannelInfo() = 0;
  virtual void RefreshProfilingInfo() = 0;
  virtual uint64_t GetLastReceivedMsgId() = 0;
  virtual uint64_t GetLastBundleId() = 0;
  virtual void GetMetrics(std::unordered_map<std::string, double> &metrics) = 0;
  virtual std::shared_ptr<BufferPool> GetBufferPool() = 0;
  virtual bool IsInitialized() = 0;
  virtual void GetBundleLargerThan(uint64_t msg_id,
                                   std::vector<StreamingMessageBundlePtr> &vec) = 0;
  virtual int Evict(uint32_t data_size) = 0;
  virtual double GetUsageRate() = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
  ProducerChannelInfo &channel_info_;
};

class ConsumerChannel {
 public:
  explicit ConsumerChannel(std::shared_ptr<Config> &transfer_config,
                           ConsumerChannelInfo &c_channel_info);
  virtual ~ConsumerChannel() = default;
  virtual TransferCreationStatus CreateTransferChannel() = 0;
  virtual StreamingStatus DestroyTransferChannel() = 0;
  virtual StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual void RefreshChannelInfo() = 0;
  virtual void RefreshProfilingInfo() = 0;
  virtual StreamingStatus ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                                 uint32_t &data_size, uint32_t timeout,
                                                 uint64_t &item_received_ts) = 0;
  virtual StreamingStatus ConsumeItemFromChannel(
      std::shared_ptr<StreamingReaderBundle> &message, uint32_t timeout,
      uint64_t &item_received_ts) = 0;
  virtual StreamingStatus NotifyChannelConsumed(uint64_t offset_id) = 0;
  virtual void GetMetrics(std::unordered_map<std::string, double> &metrics) = 0;
  virtual void NotifyStartLogging(uint64_t barrier_id, uint64_t msg_id,
                                  uint64_t seq_id) = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
  ConsumerChannelInfo &channel_info_;
};

class StreamingQueueProducer : public ProducerChannel {
 public:
  explicit StreamingQueueProducer(
      std::shared_ptr<Config> &transfer_config, ProducerChannelInfo &p_channel_info,
      std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler);
  ~StreamingQueueProducer() override;
  /// Set upstreamhandler and create a new WriterQueue.
  StreamingStatus CreateTransferChannel() override;
  /// Delete the WriterQueue held by StreamingQueueProducer.
  StreamingStatus DestroyTransferChannel() override;
  /// Let the WriterQueue set evict_limit.
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_offset) override;
  /// Write a bundle data into the WriterQueue.
  StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size,
                                       uint64_t start_id, uint64_t end_id) override;
  StreamingStatus ProduceItemToChannel(StreamingMessageBundlePtr &bundle) override;
  /// If data can't be put into queue, set queue_limit or evict_limit to put more data
  /// into queue.
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;
  /// Set queue_limit, data's msg id is less than offset_id need to put into
  /// ElasticBuffer.
  StreamingStatus NotifyRemoveLimit(uint64_t offset_id) override;
  /// Update consumed bundle_id and msg_id.
  void RefreshChannelInfo() override;
  void RefreshProfilingInfo() override;
  uint64_t GetLastReceivedMsgId() override;
  uint64_t GetLastBundleId() override;
  void GetMetrics(std::unordered_map<std::string, double> &metrics) override;
  std::shared_ptr<BufferPool> GetBufferPool() override;
  bool IsInitialized() override;
  void GetBundleLargerThan(uint64_t msg_id,
                           std::vector<StreamingMessageBundlePtr> &vec) override;
  int Evict(uint32_t data_size) override;
  double GetUsageRate() override;

 private:
  StreamingStatus CreateQueue();
  StreamingStatus PushQueueItem(uint8_t *meta_data, uint32_t meta_data_size,
                                uint8_t *data, uint32_t data_size, uint64_t timestamp,
                                uint64_t msg_id_start, uint64_t msg_id_end);

 private:
  std::shared_ptr<WriterQueue> queue_;
  StreamingQueueInitialParameter init_param_;
  ObjectID queue_id_;
  std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler_;
};

class StreamingQueueConsumer : public ConsumerChannel {
 public:
  explicit StreamingQueueConsumer(
      std::shared_ptr<Config> &transfer_config, ConsumerChannelInfo &c_channel_info,
      std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler);
  ~StreamingQueueConsumer() override;
  TransferCreationStatus CreateTransferChannel() override;
  StreamingStatus DestroyTransferChannel() override;
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  void RefreshChannelInfo() override;
  void RefreshProfilingInfo() override;
  StreamingStatus ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                         uint32_t &data_size, uint32_t timeout,
                                         uint64_t &item_received_ts) override;
  StreamingStatus ConsumeItemFromChannel(std::shared_ptr<StreamingReaderBundle> &message,
                                         uint32_t timeout,
                                         uint64_t &item_received_ts) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;
  void GetMetrics(std::unordered_map<std::string, double> &metrics) override;
  virtual void NotifyStartLogging(uint64_t barrier_id, uint64_t msg_id,
                                  uint64_t seq_id) override;

 private:
  StreamingQueueStatus GetQueue(const ObjectID &queue_id, uint64_t checkpoint_id,
                                uint64_t start_msg_id,
                                StreamingQueueInitialParameter &init_param);

 private:
  std::shared_ptr<ReaderQueue> queue_;
  StreamingQueueInitialParameter init_param_;
  std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler_;
};

/// MockProducer and Mockconsumer are independent implementation of channels that
/// conduct a very simple memory channel for unit tests or intergation test.
class MockProducer : public ProducerChannel {
 public:
  explicit MockProducer(std::shared_ptr<Config> &transfer_config,
                        ProducerChannelInfo &p_channel_info)
      : ProducerChannel(transfer_config, p_channel_info), current_bundle_id_(0){};
  virtual ~MockProducer();
  StreamingStatus CreateTransferChannel() override;

  StreamingStatus DestroyTransferChannel() override;

  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }

  StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size,
                                       uint64_t start_id, uint64_t end_id) override;
  StreamingStatus ProduceItemToChannel(StreamingMessageBundlePtr &bundle) override;

  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override {
    return StreamingStatus::OK;
  }
  StreamingStatus NotifyRemoveLimit(uint64_t offset_id) override {
    return StreamingStatus::OK;
  }
  void RefreshChannelInfo() override;
  void RefreshProfilingInfo() override {}
  uint64_t GetLastReceivedMsgId() override { return 0; }
  uint64_t GetLastBundleId() override { return current_bundle_id_; }
  void GetMetrics(std::unordered_map<std::string, double> &metrics) override {}
  std::shared_ptr<BufferPool> GetBufferPool() override { return buffer_pool_; }
  bool IsInitialized() override { return true; }
  void GetBundleLargerThan(uint64_t msg_id,
                           std::vector<StreamingMessageBundlePtr> &vec) override {}
  int Evict(uint32_t data_size) override { return 0; }
  double GetUsageRate() override { return 0; }

 private:
  std::shared_ptr<BufferPool> buffer_pool_;
  uint64_t current_bundle_id_;
};

class MockConsumer : public ConsumerChannel {
 public:
  explicit MockConsumer(std::shared_ptr<Config> &transfer_config,
                        ConsumerChannelInfo &c_channel_info)
      : ConsumerChannel(transfer_config, c_channel_info){};
  TransferCreationStatus CreateTransferChannel() override {
    return TransferCreationStatus::PullOk;
  }
  StreamingStatus DestroyTransferChannel() override { return StreamingStatus::OK; }
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }
  StreamingStatus ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                         uint32_t &data_size, uint32_t timeout,
                                         uint64_t &item_received_ts) override;
  StreamingStatus ConsumeItemFromChannel(std::shared_ptr<StreamingReaderBundle> &message,
                                         uint32_t timeout,
                                         uint64_t &item_received_ts) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;
  void RefreshChannelInfo() override;
  void RefreshProfilingInfo() override {}
  void GetMetrics(std::unordered_map<std::string, double> &metrics) override {}
  void NotifyStartLogging(uint64_t barrier_id, uint64_t msg_id,
                          uint64_t seq_id) override {}
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_CHANNEL_H
