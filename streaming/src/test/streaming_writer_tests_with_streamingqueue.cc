#define BOOST_BIND_NO_PLACEHOLDERS
#include <unistd.h>

#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"
#include "message/message.h"
#include "message/message_bundle.h"
#include "ray/core_worker/core_worker.h"
#include "streaming.h"
#include "streaming_queue_tests_base.h"
#include "test_utils.h"
#include "util/ring_buffer.h"

using namespace std::placeholders;
namespace ray {
namespace streaming {

static int node_manager_port;

class StreamingWriterTest : public StreamingQueueTestBase {
 public:
  StreamingWriterTest() : StreamingQueueTestBase(1, node_manager_port) {}
};

class StreamingExactlySameTest : public StreamingQueueTestBase {
 public:
  StreamingExactlySameTest() : StreamingQueueTestBase(1, node_manager_port) {}
};

TEST_P(StreamingWriterTest, streaming_writer_exactly_once_test) {
  STREAMING_LOG(INFO) << "StreamingWriterTest.streaming_writer_exactly_once_test";

  uint32_t queue_num = 2;

  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY ONCE";
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_writer_exactly_once_test",
             60 * 1000);
}

TEST_P(StreamingWriterTest, streaming_writer_at_least_once_test) {
  uint32_t queue_num = 5;

  STREAMING_LOG(INFO) << "Streaming Strategy => AT_LEAST_ONCE";
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_writer_at_least_once_test",
             60 * 1000);
}

TEST_P(StreamingWriterTest, streaming_recreate_test) {
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_recreate_test", 60 * 1000);
}

// TEST_P(StreamingWriterTest, no_skip_source_barrier) {
//   uint32_t queue_num = 1;
//   SubmitTest(queue_num, "StreamingWriterTest", "no_skip_source_barrier", 60 * 1000);
// }

TEST_P(StreamingWriterTest, streaming_recreate_test_without_data) {
  uint32_t queue_num = 3;
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_recreate_test_without_data",
             60 * 1000);
}

// Test thrown unexpected empty message after FO.
TEST_P(StreamingWriterTest, streaming_recreate_test_thrown) {
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_recreate_test_thrown",
             60 * 1000);
}

TEST_P(StreamingWriterTest, streaming_with_elasticbuffer_test) {
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_with_elasticbuffer_test",
             60 * 1000);
}

TEST_P(StreamingWriterTest, streaming_empty_message_test) {
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_empty_message_test", 60 * 1000);
}

TEST_P(StreamingWriterTest, streaming_rescale_exactly_once_test) {
  uint32_t queue_num = 3;
  SubmitTest(queue_num, "StreamingRescaleTest", "streaming_rescale_exactly_once_test",
             120 * 1000);
}

TEST_P(StreamingExactlySameTest, streaming_exactly_same_source_test) {
  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingExactlySameTest", "streaming_exactly_same_source_test",
             60 * 1000);
}

TEST_P(StreamingExactlySameTest, streaming_exactly_same_operator_test) {
  uint32_t queue_num = 2;
  SubmitTest(queue_num, "StreamingExactlySameTest",
             "streaming_exactly_same_operator_test", 60 * 1000);
}

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingWriterTest, testing::Values(0));

// INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingExactlySameTest,
//                         testing::Values(0, 1, 5, 9));

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
