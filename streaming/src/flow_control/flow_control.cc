#include "flow_control.h"

namespace ray {
namespace streaming {

StreamingUnconsumedMessage::StreamingUnconsumedMessage(
    std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map,
    uint32_t message_step, uint32_t bundle_step)
    : channel_map_(channel_map),
      message_consumed_step_(message_step),
      bundle_consumed_step_(bundle_step){};

bool StreamingUnconsumedMessage::ShouldFlowControl(ProducerChannelInfo &channel_info) {
  auto &queue_info = channel_info.queue_info;
  bool cyclic = channel_info.parameter.cyclic;

  // Data messages should be under flow control if target message id is less then lastest
  // sent message id, and empty message can not be dumped without limitation. So we keep
  // available bucket size of unconsumed message in [0, message consumed step] and
  // unconsumed empty message size in [0, bundle consumed step].
  if ((!cyclic && queue_info.target_message_id <= channel_info.message_last_commit_id) ||
      channel_info.sent_empty_cnt > 0) {
    channel_map_[channel_info.channel_id]->RefreshChannelInfo();
    channel_info.queue_info.target_message_id =
        channel_info.queue_info.consumed_message_id + message_consumed_step_;
    bool message_in_buffer =
        channel_info.current_message_id > channel_info.message_last_commit_id;
    STREAMING_LOG(DEBUG) << "Flow control stop writing to downstream, current max id => "
                         << channel_info.current_message_id << ", last committed id => "
                         << channel_info.message_last_commit_id
                         << ", current bundle id => " << channel_info.current_bundle_id
                         << ", target message id => " << queue_info.target_message_id
                         << ", consumed message id=> " << queue_info.consumed_message_id
                         << ", consumed bundle id => " << queue_info.consumed_bundle_id
                         << ", q id => " << channel_info.channel_id
                         << ", bundle consumed step =>" << bundle_consumed_step_
                         << ", has message in buffer " << message_in_buffer;
    // Double check whether flow control is valid.
    // A channel will not produce new item if
    // 1. Some messages from upstream to downstream are unconsumed if target message id
    //    is less than last commit message id.
    // 2. All valid messages are consumed, but there are still a lot of empty messages in
    //    downstream waiting list.
    // Besides, we assume queue consumed bundle id will be -1 if no bundle have been
    // consumed by downstream or no notification message received from downstream.
    // In failover case, queue_info.consumed_bundle_id will be updated in old value that
    // may greater than channel_info.current_bundle_id.

    if ((!cyclic && queue_info.target_message_id < channel_info.message_last_commit_id) ||
        (!message_in_buffer &&
         channel_info.current_bundle_id > queue_info.consumed_bundle_id &&
         channel_info.current_bundle_id - queue_info.consumed_bundle_id >
             bundle_consumed_step_)) {
      return true;
    }
  }
  return false;
}

StreamingUnconsumedBytes::StreamingUnconsumedBytes(
    std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map)
    : channel_map_(channel_map){};

bool StreamingUnconsumedBytes::ShouldFlowControl(ProducerChannelInfo &channel_info) {
  uint64_t unconsumed_bytes_threhold = channel_info.queue_size * 0.3;
  channel_map_[channel_info.channel_id]->RefreshChannelInfo();
  return channel_info.queue_info.unconsumed_bytes >= unconsumed_bytes_threhold;
}
}  // namespace streaming

}  // namespace ray
