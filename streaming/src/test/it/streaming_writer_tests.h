#pragma once
#include "streaming_test_suite.h"

namespace ray {
namespace streaming {

class StreamingQueueWriterTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueWriterTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                const std::vector<ObjectID> &queue_ids,
                                const std::vector<ObjectID> &rescale_queue_ids)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_writer_exactly_once_test",
         std::bind(&StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest, this)},
        {"streaming_writer_at_least_once_test",
         std::bind(&StreamingQueueWriterTestSuite::StreamingWriterAtLeastOnceTest, this)},
        {"streaming_recreate_test",
         std::bind(&StreamingQueueWriterTestSuite::StreamingRecreateTest, this)},
        {"no_skip_source_barrier",
         std::bind(&StreamingQueueWriterTestSuite::NoSkipSourceBarrier, this)},
        {"streaming_recreate_test_without_data",
         std::bind(&StreamingQueueWriterTestSuite::StreamingRecreateWithoutDataTest,
                   this)},
        {"streaming_with_elasticbuffer_test",
         std::bind(&StreamingQueueWriterTestSuite::ElasticBufferTest, this)},
        {"streaming_empty_message_test",
         std::bind(&StreamingQueueWriterTestSuite::StreamingEmptyMessageTest, this)},
        {"streaming_recreate_test_thrown",
         std::bind(&StreamingQueueWriterTestSuite::StreamingRecreateThrownTest, this)}};
  }

 private:
  void TestWriteMessageToBufferRing(std::shared_ptr<DataWriter> writer_client,
                                    std::vector<ray::ObjectID> &q_list);

  void StreamingWriterStrategyTest(std::shared_ptr<RuntimeContext> &runtime_context);

  void StreamingWriterExactlyOnceTest();

  void StreamingWriterAtLeastOnceTest();

  void StreamingRecreateTest();

  void ElasticBufferTest();

  void NoSkipSourceBarrier();

  void StreamingRecreateWithoutDataTest();

  void StreamingEmptyMessageTest();

  void StreamingRecreateThrownTest();
};

class StreamingQueueReaderTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueReaderTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                const std::vector<ObjectID> &queue_ids,
                                const std::vector<ObjectID> &rescale_queue_ids)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_writer_exactly_once_test",
         std::bind(&StreamingQueueReaderTestSuite::StreamingWriterExactlyOnceTest, this)},
        {"streaming_writer_at_least_once_test",
         std::bind(&StreamingQueueReaderTestSuite::StreamingWriterAtLeastOnceTest, this)},
        {"streaming_recreate_test",
         std::bind(&StreamingQueueReaderTestSuite::StreamingRecreateTest, this)},
        {"no_skip_source_barrier",
         std::bind(&StreamingQueueReaderTestSuite::NoSkipSourceBarrier, this)},
        {"streaming_recreate_test_without_data",
         std::bind(&StreamingQueueReaderTestSuite::StreamingRecreateWithoutDataTest,
                   this)},
        {"streaming_with_elasticbuffer_test",
         std::bind(&StreamingQueueReaderTestSuite::ElasticBufferTest, this)},
        {"streaming_empty_message_test",
         std::bind(&StreamingQueueReaderTestSuite::StreamingEmptyMessageTest, this)},
        {"streaming_recreate_test_thrown",
         std::bind(&StreamingQueueReaderTestSuite::StreamingRecreateThrownTest, this)}};
  }

 private:
  void ReaderLoopForward(std::shared_ptr<DataReader> reader_client,
                         std::vector<ray::ObjectID> &queue_id_vec);
  void StreamingReaderStrategyTest(std::shared_ptr<RuntimeContext> &runtime_context);
  void StreamingWriterExactlyOnceTest();
  void StreamingWriterAtLeastOnceTest();
  void StreamingRecreateTest();
  void ElasticBufferTest();
  void NoSkipSourceBarrier();
  void StreamingRecreateWithoutDataTest();
  void StreamingEmptyMessageTest();

  void StreamingRecreateThrownTest();
};

}  // namespace streaming
}  // namespace ray