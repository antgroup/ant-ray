#ifndef RAY_STREAMING_CHANNEL_FACTORY_H
#define RAY_STREAMING_CHANNEL_FACTORY_H

#include <memory>

#include "channel.h"
#include "conf/streaming_config.h"

namespace ray {
namespace streaming {
/// Factory help to create channels.
class ChannelFactory {
 public:
  static std::shared_ptr<ProducerChannel> CreateProducerChannel(
      TransferQueueType type, std::shared_ptr<Config> &transfer_config,
      ProducerChannelInfo &channel_info, std::shared_ptr<QueueMessageHandler> handler) {
    switch (type) {
    case TransferQueueType::STREAMING_QUEUE:
      return std::make_shared<StreamingQueueProducer>(
          transfer_config, channel_info,
          std::dynamic_pointer_cast<UpstreamQueueMessageHandler>(handler));
      break;
    case TransferQueueType::MOCK_QUEUE:
      return std::make_shared<MockProducer>(transfer_config, channel_info);
      break;
    default:
      STREAMING_CHECK(false) << "Unsupported queue type: " << (uint32_t)type;
      return nullptr;
      break;
    }
  }

  static std::shared_ptr<ConsumerChannel> CreateConsumerChannel(
      TransferQueueType type, std::shared_ptr<Config> &transfer_config,
      ConsumerChannelInfo &channel_info, std::shared_ptr<QueueMessageHandler> handler) {
    switch (type) {
    case TransferQueueType::STREAMING_QUEUE:
      return std::make_shared<StreamingQueueConsumer>(
          transfer_config, channel_info,
          std::dynamic_pointer_cast<DownstreamQueueMessageHandler>(handler));
      break;
    case TransferQueueType::MOCK_QUEUE:
      return std::make_shared<MockConsumer>(transfer_config, channel_info);
      break;
    default:
      STREAMING_CHECK(false) << "Unsupported queue type: " << (uint32_t)type;
      return nullptr;
      break;
    }
  }
};

}  // namespace streaming
}  // namespace ray

#endif
