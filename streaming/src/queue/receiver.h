#ifndef RAY_STREAMING_RECEIVER_H
#define RAY_STREAMING_RECEIVER_H

#include "transport.h"

namespace ray {
namespace streaming {

class QueueMessageHandler;
class DirectCallReceiver {
 public:
  virtual void OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffer) = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer) = 0;
  virtual void SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) = 0;
};

}  // namespace streaming
}  // namespace ray
#endif
