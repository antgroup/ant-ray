#ifndef RAY_STREAMING_FLOW_CONTROL_H
#define RAY_STREAMING_FLOW_CONTROL_H
#include "channel/channel.h"

namespace ray {
namespace streaming {
class ProducerTransfer;
// We devise a very simple flow control system in queue channel, and that
// include two methods :
// 1. unconsumed seq, upstream worker will detect consumer stat via api so it
// can keep fixed length messages in this process, which make a continuous
// datastream in channel or on the transporting way, then downstream can read
// them from channel immediately.
// 2. unconsumed bytes, more exact controlling in data bytes than unconsumed
// seq. In this way, upstream keep fix length data buffer.
//
//  To debug or compare with theses flow control methods, we also support
//  no-flow-control that will do nothing in transporting.
class StreamingFlowControl {
 public:
  virtual ~StreamingFlowControl() = default;
  virtual bool ShouldFlowControl(ProducerChannelInfo &channel_info) = 0;
};

class NoFlowControl : public StreamingFlowControl {
 public:
  bool ShouldFlowControl(ProducerChannelInfo &channel_info) override { return false; }
  ~NoFlowControl() = default;
};

class StreamingUnconsumedMessage : public StreamingFlowControl {
 public:
  StreamingUnconsumedMessage(
      std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map,
      uint32_t message_step, uint32_t bundle_step = 0xfff);
  ~StreamingUnconsumedMessage() = default;
  bool ShouldFlowControl(ProducerChannelInfo &channel_info) override;

 private:
  /// NOTE(wanxing.wwx) Reference to channel_map_ variable in DataWriter.
  /// Flow-control is check in FlowControlThread, so channel_map_ is accessed
  /// in multithread situation. Especially, when in rescaling, channel_map_ maybe
  /// changed. But for now, FlowControlThread is stopped before rescaling.
  std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map_;
  uint32_t message_consumed_step_;
  uint32_t bundle_consumed_step_;
};

class StreamingUnconsumedBytes : public StreamingFlowControl {
 public:
  explicit StreamingUnconsumedBytes(
      std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map);
  bool ShouldFlowControl(ProducerChannelInfo &channel_info) override;

 private:
  std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map_;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_FLOW_CONTROL_H
