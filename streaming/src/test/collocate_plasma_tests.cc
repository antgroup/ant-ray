
#define BOOST_BIND_NO_PLACEHOLDERS
#include <unistd.h>

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
class CollocatePlasmaTest : public StreamingQueueTestBase {
 public:
  CollocatePlasmaTest() : StreamingQueueTestBase(1, node_manager_port) {
    plasma_store_socket_ = GetStoreSocketName();
  }

 protected:
  std::string plasma_store_socket_;
};

TEST_P(CollocatePlasmaTest, TestProcessCrash) {
  ObjectID objectId = ObjectID::FromRandom();
  std::shared_ptr<plasma::PlasmaClient> client1 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client1->Connect(plasma_store_socket_).ok());
  bool has_object;
  auto contain = client1->Contains(objectId, &has_object);
  ASSERT_TRUE(contain.ok());
  ASSERT_TRUE(!has_object);
  std::shared_ptr<ray::Buffer> buffer;
  uint64_t retry_with_request_id = 0;
  ASSERT_TRUE((client1->Create(objectId, ray::rpc::Address(), 10240, nullptr, 0,
                               &retry_with_request_id, &buffer))
                  .ok());
  ASSERT_TRUE((client1->Seal(objectId)).ok());
  // Use disconnect to mock process exit abnormally.
  RAY_IGNORE_EXPR(client1->Disconnect());
  client1.reset();

  // The plasma store server is initialized with 20000bytes memory, if another
  // 10240 bytes object is created, the first object should be evicted.
  std::shared_ptr<plasma::PlasmaClient> client2 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client2->Connect(plasma_store_socket_).ok());
  contain = client2->Contains(objectId, &has_object);
  ASSERT_TRUE(contain.ok());
  ASSERT_TRUE(has_object) << has_object;
  ObjectID objectId2 = ObjectID::FromRandom();
  ASSERT_TRUE((client2->Create(objectId2, ray::rpc::Address(), 10240, nullptr, 0,
                               &retry_with_request_id, &buffer))
                  .ok());
  ASSERT_TRUE((client2->Seal(objectId2)).ok());
}

TEST_P(CollocatePlasmaTest, TestTwoProcessCrash) {
  ObjectID objectId = ObjectID::FromRandom();
  std::shared_ptr<plasma::PlasmaClient> client1 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client1->Connect(plasma_store_socket_).ok());
  std::shared_ptr<plasma::PlasmaClient> client2 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client2->Connect(plasma_store_socket_).ok());
  std::shared_ptr<plasma::PlasmaClient> client3 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client3->Connect(plasma_store_socket_).ok());

  bool has_object;
  auto contain = client1->Contains(objectId, &has_object);
  ASSERT_TRUE(contain.ok());
  ASSERT_TRUE(!has_object);
  std::shared_ptr<ray::Buffer> buffer;
  uint64_t retry_with_request_id = 0;
  ASSERT_TRUE((client1->Create(objectId, ray::rpc::Address(), 10240, nullptr, 0,
                               &retry_with_request_id, &buffer))
                  .ok());
  ASSERT_TRUE((client1->Seal(objectId)).ok());

  std::vector<ray::ObjectID> object_ids{objectId};
  std::vector<plasma::ObjectBuffer> object_buffers;
  ASSERT_TRUE(client2->Get(object_ids, 1000, &object_buffers, true).ok());

  std::shared_ptr<ray::Buffer> buffer3;
  // ASSERT_TRUE(!(client3->Create(ObjectID::FromRandom(), ray::rpc::Address(), 10240,
  //                               nullptr, 0, &retry_with_request_id, &buffer3))
  //                  .ok());

  RAY_IGNORE_EXPR(client1->Disconnect());
  client1.reset();

  ASSERT_TRUE(client2->Get(object_ids, 1000, &object_buffers, true).ok());
  ASSERT_TRUE(client2->Release(objectId).ok());
  ASSERT_TRUE(client2->Delete(objectId).ok());

  ObjectID objectId3 = ObjectID::FromRandom();
  ASSERT_TRUE((client3->Create(objectId3, ray::rpc::Address(), 10240, nullptr, 0,
                               &retry_with_request_id, &buffer3))
                  .ok());
  ASSERT_TRUE((client3->Seal(objectId3)).ok());

  RAY_IGNORE_EXPR(client2->Disconnect());
  RAY_IGNORE_EXPR(client3->Disconnect());
}

TEST_P(CollocatePlasmaTest, TestGetTimeout) {
  ObjectID objectId = ObjectID::FromRandom();
  std::shared_ptr<plasma::PlasmaClient> client1 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client1->Connect(plasma_store_socket_).ok());
  std::shared_ptr<plasma::PlasmaClient> client2 =
      std::make_shared<plasma::PlasmaClient>();
  ASSERT_TRUE(client2->Connect(plasma_store_socket_).ok());

  std::shared_ptr<ray::Buffer> buffer;
  uint64_t retry_with_request_id = 0;
  ASSERT_TRUE((client1->Create(objectId, ray::rpc::Address(), 10240, nullptr, 0,
                               &retry_with_request_id, &buffer))
                  .ok());
  ASSERT_TRUE((client1->Seal(objectId)).ok());

  std::vector<ray::ObjectID> object_ids{objectId};
  std::vector<plasma::ObjectBuffer> object_buffers;
  plasma::ObjectBuffer object_buffer;
  ASSERT_TRUE(client2->Get(&objectId, 1, 10000, &object_buffer, true).ok());
  ASSERT_NE(object_buffer.data, nullptr);

  STREAMING_LOG(INFO) << "TestGetTimeout 1";
  ASSERT_TRUE(client1->Release(objectId).ok());
  STREAMING_LOG(INFO) << "TestGetTimeout 2";
  // ASSERT_TRUE(client1->Delete(objectId).ok());
  STREAMING_LOG(INFO) << "TestGetTimeout 3";
  ASSERT_TRUE(client2->Release(objectId).ok());
  STREAMING_LOG(INFO) << "TestGetTimeout 4";
  ASSERT_TRUE(client2->Delete(objectId).ok());
  STREAMING_LOG(INFO) << "TestGetTimeout 5";

  plasma::ObjectBuffer object_buffer2;
  ASSERT_TRUE(client2->Get(&objectId, 1, 10000, &object_buffer2, true).ok());
  ASSERT_EQ(object_buffer2.data, nullptr);
  STREAMING_LOG(INFO) << "TestGetTimeout 6";
  // client1->Disconnect();
  // client2->Disconnect();
}

INSTANTIATE_TEST_CASE_P(StreamingTest, CollocatePlasmaTest, testing::Values(0));

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::streaming::node_manager_port = ray::streaming::util::GenerateRandomPort();
  ray::TEST_RAYLET_EXEC_PATH = std::string(argv[1]);
  ray::TEST_GCS_SERVER_EXEC_PATH = std::string(argv[2]);
  ray::TEST_REDIS_CLIENT_EXEC_PATH = std::string(argv[3]);
  ray::TEST_REDIS_SERVER_EXEC_PATH = std::string(argv[4]);
  return RUN_ALL_TESTS();
}
