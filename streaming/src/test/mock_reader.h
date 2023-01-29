#pragma once
#include "data_reader.h"
namespace ray {
namespace streaming {

class MockReader : public DataReader {
 public:
  MockReader(std::shared_ptr<RuntimeContext> &runtime_context)
      : DataReader(runtime_context) {}
  inline std::unordered_map<ObjectID, std::shared_ptr<ConsumerChannel>> &GetChannelMap() {
    return channel_map_;
  }

  void Init(std::vector<ObjectID> input_ids) {
    for (auto &id : input_ids) {
      channel_info_map_[id].current_message_id = 0;
    }
    std::copy(input_ids.begin(), input_ids.end(), std::back_inserter(unready_queue_ids_));
  }

  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<StreamingQueueInitialParameter> &init_params) {
    DataReader::Init(input_ids, init_params, 1000);
  }

  void ResetLastMessageIds(const std::unordered_map<ObjectID, uint64_t> &last_msg_map) {
    for (auto item : last_msg_map) {
      last_message_id_[item.first] = item.second;
    }
  }

  void SplitBundle(std::shared_ptr<StreamingReaderBundle> &message,
                   uint64_t last_msg_id) override {}
  void InitChannel() {
    std::vector<TransferCreationStatus> creation_status;
    DataReader::InitChannel(creation_status);
  }

  void ResetUnreadyIds(const std::vector<ObjectID> &ids) { unready_queue_ids_ = ids; }

  BundleCheckStatus CheckBundle(const std::shared_ptr<StreamingReaderBundle> bundle) {
    return DataReader::CheckBundle(bundle);
  }

  bool IsValidBundle(std::shared_ptr<StreamingReaderBundle> bundle) {
    return DataReader::IsValidBundle(bundle);
  }
  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<StreamingQueueInitialParameter> &init_params,
            const std::vector<uint64_t> &streaming_msg_ids, int64_t timer_interval,
            std::vector<TransferCreationStatus> &creation_status) {
    DataReader::Init(input_ids, init_params, streaming_msg_ids, timer_interval,
                     creation_status);
  }

  int32_t GetTargetBarrierCount(const std::vector<ObjectID> &queue_ids) {
    return DataReader::GetTargetBarrierCount(queue_ids);
  }
};

}  // namespace streaming
}  // namespace ray