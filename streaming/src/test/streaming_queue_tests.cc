#define BOOST_BIND_NO_PLACEHOLDERS
#include <unistd.h>

#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"
#include "message.h"
#include "message_bundle.h"
#include "ray/core_worker/core_worker.h"
#include "ring_buffer.h"
#include "streaming.h"
#include "streaming_queue_tests_base.h"
#include "test_utils.h"

using namespace std::placeholders;
namespace ray {
namespace streaming {

static int node_manager_port = 0;

class StreamingQueueTest : public StreamingQueueTestBase {
 public:
  StreamingQueueTest() : StreamingQueueTestBase(1, node_manager_port) {}
};

TEST_P(StreamingQueueTest, PullPeerAsyncTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.pull_peer_async_test";

  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "pull_peer_async_test", 60 * 1000);
}

TEST_P(StreamingQueueTest, GetQueueTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.get_queue_test";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "get_queue_test", 60 * 1000);
}

TEST_P(StreamingQueueTest, DeleteQueueTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.delete_queue_test";
  uint32_t queue_num = 2;
  SubmitTest(queue_num, "StreamingQueueTest", "delete_queue_test", 60 * 1000);
}

TEST_P(StreamingQueueTest, GetPeerLastMsgIdTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.get_peer_last_msg_id";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "get_peer_last_msg_id", 60 * 1000);
}

TEST_P(StreamingQueueTest, DirectCallPerfTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.direct_call_perf_test";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "direct_call_perf_test", 60 * 1000);
}

TEST_P(StreamingQueueTest, DirectCallConnectionBrokenTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.direct_call_connection_broken_test";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "direct_call_connection_broken_test",
             60 * 60 * 1000);
}

TEST_P(StreamingQueueTest, MultipleArgsTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.multiple_args";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "multiple_args", 60 * 1000);
}

TEST_P(StreamingQueueTest, GetPeerHostnameTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.get_peer_hostname";
  uint32_t queue_num = 256;
  SubmitTest(queue_num, "StreamingQueueTest", "get_peer_hostname", 120 * 1000);
}

TEST_P(StreamingQueueTest, CollocateSendTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.collocate_send_test";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "collocate_send_test", 60 * 1000);
}

TEST_P(StreamingQueueTest, CollocateFOTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.collocate_fo_test";
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "collocate_fo_test", 60 * 1000);
}

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingQueueTest, testing::Values(0));

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  // set_streaming_log_config("streaming_writer_test", StreamingLogLevel::INFO, 0);
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 6);
  ray::streaming::node_manager_port = ray::streaming::util::GenerateRandomPort();
  ray::TEST_RAYLET_EXEC_PATH = std::string(argv[1]);
  ray::TEST_MOCK_WORKER_EXEC_PATH = std::string(argv[2]);
  ray::TEST_GCS_SERVER_EXEC_PATH = std::string(argv[3]);
  ray::TEST_REDIS_CLIENT_EXEC_PATH = std::string(argv[4]);
  ray::TEST_REDIS_SERVER_EXEC_PATH = std::string(argv[5]);

  return RUN_ALL_TESTS();
}
