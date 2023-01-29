#pragma once
#include <csignal>

#include "data_reader.h"
#include "data_writer.h"
#include "elasticbuffer/elastic_buffer.h"
#include "gtest/gtest.h"
#include "message.h"
#include "message_bundle.h"
#include "persistence.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/core_worker.h"
#include "ring_buffer.h"
#include "streaming.h"
#include "test/test_utils.h"
#include "util/utility.h"

namespace ray {
namespace streaming {

const uint32_t MESSAGE_BOUND_SIZE = 10000;
const uint32_t DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE = 1000;
const uint32_t MESSAGE_BARRIER_INTERVAL = 1000;

class StreamingQueueTestSuite {
 public:
  StreamingQueueTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                          const std::vector<ObjectID> &queue_ids,
                          const std::vector<ObjectID> &rescale_queue_ids)
      : status_(false),
        worker_id_(worker_id),
        peer_actor_id_(peer_actor_id),
        queue_ids_(queue_ids),
        rescale_queue_ids_(rescale_queue_ids) {}

  virtual void ExecuteTest(std::string test_name) {
    auto it = test_func_map_.find(test_name);
    STREAMING_CHECK(it != test_func_map_.end());
    current_test_ = test_name;
    status_ = false;
    auto func = it->second;
    executor_thread_ = std::make_shared<std::thread>([func, this]() {
      this->SetUp();
      func();
      this->TearDown();
    });
    executor_thread_->detach();
  }

  virtual std::shared_ptr<LocalMemoryBuffer> CheckCurTestStatus() {
    TestCheckStatusRspMsg msg(current_test_, status_);
    return msg.ToBytes();
  }

  virtual bool TestDone() { return status_; }

  virtual ~StreamingQueueTestSuite() {}

  virtual void OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (receiver_) {
      receiver_->OnMessage(buffers);
    }
  }
  virtual std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::shared_ptr<LocalMemoryBuffer> rst = nullptr;
    if (receiver_ && (rst = receiver_->OnMessageSync(buffer)) != nullptr) {
      return rst;
    } else {
      uint8_t data[4];
      std::shared_ptr<LocalMemoryBuffer> result =
          std::make_shared<LocalMemoryBuffer>(data, 4, true);
      return result;
    }
  }

  void SetReceiver(std::shared_ptr<DirectCallReceiver> receiver) {
    std::unique_lock<std::mutex> lock(mutex_);
    receiver_ = receiver;
  }

  void SetPlasmaStoreSocket(const std::string &socket) { plasma_store_socket_ = socket; }

  virtual void SetUp() {}
  virtual void TearDown() {}

 protected:
  std::unordered_map<std::string, std::function<void()>> test_func_map_;
  std::string current_test_;
  bool status_;
  std::shared_ptr<std::thread> executor_thread_;
  WorkerID worker_id_;
  ActorID peer_actor_id_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
  std::shared_ptr<DirectCallReceiver> receiver_;
  std::mutex mutex_;
  std::string plasma_store_socket_;
  static const std::shared_ptr<RayFunction> ASYNC_CALL_FUNC;
  static const std::shared_ptr<RayFunction> SYNC_CALL_FUNC;
};

}  // namespace streaming
}  // namespace ray