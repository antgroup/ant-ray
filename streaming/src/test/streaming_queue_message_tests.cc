#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <chrono>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "queue.h"
#include "queue_handler.h"
#include "ray/util/util.h"
#include "streaming_queue_generated.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingQueueMessageTest, TestDataMessageTimestamp) {
  ray::ObjectID queue_id = ray::ObjectID::FromRandom();

  JobID job_id = JobID::FromInt(0);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ray::ActorID actor_id = ray::ActorID::Of(job_id, task_id, 0);
  ray::ActorID peer_actor_id = ray::ActorID::Of(job_id, task_id, 1);
  STREAMING_LOG(WARNING) << "actor_id: " << actor_id;
  STREAMING_LOG(WARNING) << "peer_actor_id: " << peer_actor_id;

  uint8_t byte = 100;
  std::shared_ptr<ray::streaming::LocalMemoryBuffer> meta_buffer =
      std::unique_ptr<ray::streaming::LocalMemoryBuffer>(
          new ray::streaming::LocalMemoryBuffer(&byte, 1, true));
  std::shared_ptr<ray::streaming::LocalMemoryBuffer> buffer =
      std::unique_ptr<ray::streaming::LocalMemoryBuffer>(
          new ray::streaming::LocalMemoryBuffer(&byte, 1, true));

  uint64_t current = current_sys_time_ms();
  QueueItem item(1, meta_buffer, buffer, current, 2222, 0, 1, 1, false);
  DataMessage msg(actor_id, peer_actor_id, queue_id, item, false, 0,
                  queue::flatbuf::ResendReason::PULLED);

  std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers{
      msg.GetMetaBytes(), meta_buffer, buffer};
  std::shared_ptr<DataMessage> result_msg = DataMessage::FromBytes(buffers);

  QueueItem result_item(result_msg, 0);
  EXPECT_LE(result_item.TimestampMessage(), current_sys_time_ms());
  EXPECT_EQ(result_item.TimestampItem(), 2222);
}

TEST(StreamingQueueMessageTest, TestPullRequestMessage) {
  ActorID actor_id = RandomActorID();
  ActorID peer_actor_id = RandomActorID();
  ObjectID queue_id = ObjectID::FromRandom();
  uint64_t checkpoint_id = 100;
  uint64_t msg_id = 1000;
  PullRequestMessage message(actor_id, peer_actor_id, queue_id, checkpoint_id, msg_id);
  auto bytes = message.ToBytes();
  auto message_2 = PullRequestMessage::FromBytes(bytes->Data());
  EXPECT_EQ(message_2->CheckpointId(), checkpoint_id);
  EXPECT_EQ(message_2->MsgId(), msg_id);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
