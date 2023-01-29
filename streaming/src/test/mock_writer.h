#pragma once

#include "data_writer.h"

namespace ray {
namespace streaming {

class MockWriter : public DataWriter {
 public:
  friend class MockWriterTest;
  MockWriter(std::shared_ptr<RuntimeContext> &runtime_context)
      : DataWriter(runtime_context) {}
  void Init(const std::vector<ObjectID> &input_channel_vec) {
    transfer_config_->Set(ConfigEnum::BUFFER_POOL_SIZE,
                          runtime_context_->config_.GetStreamingBufferPoolSize());
    transfer_config_->Set(
        ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE,
        runtime_context_->config_.GetStreamingBufferPoolMinBufferSize());
    output_queue_ids_ = input_channel_vec;
    for (size_t i = 0; i < input_channel_vec.size(); ++i) {
      const StreamingQueueInitialParameter param{RandomActorID(), nullptr, nullptr, false,
                                                 false};
      InitChannel(input_channel_vec[i], param, 0, 0xfff);
    }
    reliability_helper_ = ReliabilityHelperFactory::GenReliabilityHelper(
        runtime_context_->config_, barrier_helper_, this, nullptr);
    runtime_context_->transfer_state_.SetRunning();
  }

  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::vector<StreamingQueueInitialParameter> &init_params,
                       const std::vector<uint64_t> &channel_message_id_vec,
                       const std::vector<uint64_t> &queue_size_vec) {
    return DataWriter::Init(queue_id_vec, init_params, channel_message_id_vec,
                            queue_size_vec);
  }

  bool IsMessageAvailableInBuffer(const ObjectID &id) {
    return DataWriter::IsMessageAvailableInBuffer(channel_info_map_[id]);
  }

  std::unordered_map<ObjectID, ProducerChannelInfo> &GetChannelInfoMap() {
    return channel_info_map_;
  };

  bool CollectFromRingBuffer(const ObjectID &id, uint64_t &buffer_remain) {
    return DataWriter::CollectFromRingBuffer(channel_info_map_[id], buffer_remain);
  }

  StreamingStatus WriteBufferToChannel(const ObjectID &id, uint64_t &buffer_remain) {
    return DataWriter::WriteBufferToChannel(channel_info_map_[id], buffer_remain);
  }

  void BroadcastBarrier(uint64_t barrier_id) {
    static const uint8_t barrier_data[] = {1, 2, 3, 4};
    DataWriter::BroadcastBarrier(barrier_id, barrier_data, 4);
  }

  uint64_t WriteMessageToBufferRing(const ObjectID &channel_id, uint8_t *data,
                                    uint32_t data_size) {
    return DataWriter::WriteMessageToBufferRing(channel_id, data, data_size);
  }

  uint64_t WriteMessageToBufferRing(const ObjectID &q_id, MemoryBuffer buffer,
                                    StreamingMessageType message_type) {
    return DataWriter::WriteMessageToBufferRing(q_id, buffer, message_type);
  }

  std::shared_ptr<UpstreamQueueMessageHandler> GetUpStreamHandler() {
    return std::dynamic_pointer_cast<UpstreamQueueMessageHandler>(queue_handler_);
  }

  uint64_t GetSendEmptyCnt(const ObjectID &queue_id) {
    return channel_info_map_[queue_id].sent_empty_cnt;
  }

  void OnStartLogging(const ObjectID &queue_id, uint64_t msg_id, uint64_t seq_id,
                      uint64_t barrier_id) {
    DataWriter::OnStartLogging(queue_id, msg_id, seq_id, barrier_id);
  }

  std::shared_ptr<AsyncWAL> GetAsyncWAL() { return async_wal_; }

  int GetEventNum() { return event_service_->EventNums(); }

  StreamingBarrierHelper &GetBarrierHelper() { return barrier_helper_; }

  void StopEventService() { event_service_->Stop(); }
};

}  // namespace streaming
}  // namespace ray
