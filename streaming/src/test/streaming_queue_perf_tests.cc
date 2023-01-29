#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <chrono>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "ray/util/util.h"
// #include "ray/common/status.h"

#include "queue.h"
#include "queue_client.h"
#include "queue_handler.h"
#include "transport.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingQueuePerfTest, EvictItemsTests) {
  ray::ObjectID queue_id = ray::ObjectID::FromRandom();
  JobID job_id = JobID::FromInt(0);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ray::ActorID actor_id = ray::ActorID::Of(job_id, task_id, 0);
  ray::ActorID peer_actor_id = ray::ActorID::Of(job_id, task_id, 0);

  std::shared_ptr<WriterQueue> queue =
      std::make_shared<WriterQueue>(queue_id, actor_id, peer_actor_id, 10000000, nullptr);

  uint8_t mock_data[32];
  for (int i = 0; i < 1000000; i++) {
    ray::Status status = queue->Push(mock_data, 32, 0, 2 * i, 2 * i + 1, false);
    if (StatusCode::OutOfMemory == status.code()) {
      STREAMING_LOG(INFO) << "OutOfMemory i: " << i;
      break;
    }
  }
  queue->MockSend();

  uint64_t target = 2 * 200000 + 1;
  queue->SetQueueEvictionLimit(target);
  std::shared_ptr<NotificationMessage> msg =
      std::make_shared<NotificationMessage>(actor_id, peer_actor_id, queue_id, target);
  queue->OnNotify(msg);
}

TEST(StreamingQueuePerfTest, EvictItemsOptimizedTests) {
  ray::ObjectID queue_id = ray::ObjectID::FromRandom();
  JobID job_id = JobID::FromInt(0);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ray::ActorID actor_id = ray::ActorID::Of(job_id, task_id, 0);
  ray::ActorID peer_actor_id = ray::ActorID::Of(job_id, task_id, 0);

  std::shared_ptr<WriterQueue> queue =
      std::make_shared<WriterQueue>(queue_id, actor_id, peer_actor_id, 10000000, nullptr);

  uint8_t mock_data[32];
  for (int i = 0; i < 1000000; i++) {
    ray::Status status = queue->Push(mock_data, 32, 0, 2 * i, 2 * i + 1, false);
    if (StatusCode::OutOfMemory == status.code()) {
      STREAMING_LOG(INFO) << "OutOfMemory i: " << i;
      break;
    }
  }
  queue->MockSend();

  uint64_t target = 2 * 200000 + 1;
  queue->SetQueueEvictionLimit(target);
  std::shared_ptr<NotificationMessage> msg =
      std::make_shared<NotificationMessage>(actor_id, peer_actor_id, queue_id, target);
  queue->OnNotify(msg);
  queue->Evict(32);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
