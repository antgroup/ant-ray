#pragma once
#include "test/it/streaming_test_suite.h"

namespace ray {
namespace streaming {

class StreamingExactlySameWriterTestSuite : public StreamingQueueTestSuite {
 private:
  uint64_t param_;

 public:
  StreamingExactlySameWriterTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                      const std::vector<ObjectID> &queue_ids,
                                      const std::vector<ObjectID> &rescale_queue_ids,
                                      uint64_t param)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids),
        param_(param) {
    test_func_map_ = {
        {"streaming_exactly_same_source_test",
         std::bind(&StreamingExactlySameWriterTestSuite::StreamingExactlySameSourceTest,
                   this)},
        {"streaming_exactly_same_operator_test",
         std::bind(&StreamingExactlySameWriterTestSuite::StreamingExactlySameOperatorTest,
                   this)}};
  }

  void RemoveAllMetaFile(const std::vector<ray::ObjectID> &q_list,
                         uint64_t max_checkpoint_id);
  void TestWriteMessageToBufferRing(DataWriter *writer_client,
                                    const std::vector<ray::ObjectID> &q_list);
  void streaming_strategy_test(
      std::shared_ptr<RuntimeContext> &runtime_context,
      const std::vector<ray::ObjectID> &queue_id_vec, DataWriter **writer_client_ptr,
      QueueCreationType queue_creation_type = QueueCreationType::RECREATE,
      StreamingRole replay_role = StreamingRole::TRANSFORM,
      bool remove_meta_file = false);
  void StreamingExactlySameSourceTest();
  void StreamingExactlySameOperatorTest();
};

class StreamingExactlySameReaderTestSuite : public StreamingQueueTestSuite {
 private:
  uint64_t param_;

 public:
  StreamingExactlySameReaderTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                      const std::vector<ObjectID> &queue_ids,
                                      const std::vector<ObjectID> &rescale_queue_ids,
                                      uint64_t param)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids),
        param_(param) {
    test_func_map_ = {
        {"streaming_exactly_same_source_test",
         std::bind(&StreamingExactlySameReaderTestSuite::StreamingExactlySameSourceTest,
                   this)},
        {"streaming_exactly_same_operator_test",
         std::bind(&StreamingExactlySameReaderTestSuite::StreamingExactlySameOperatorTest,
                   this)}};
  }

  void RemoveAllMetaFile(const std::vector<ray::ObjectID> &q_list,
                         uint64_t max_checkpoint_id);
  void ReaderLoopForward(DataReader *reader_client,
                         const std::vector<ray::ObjectID> &queue_id_vec,
                         std::vector<StreamingMessageBundlePtr> &bundle_vec);

  void streaming_strategy_test(
      std::shared_ptr<RuntimeContext> &runtime_context,
      const std::vector<ray::ObjectID> &queue_id_vec, DataReader **reader_client_ptr,
      std::vector<StreamingMessageBundlePtr> &bundle_vec,
      QueueCreationType queue_creation_type = QueueCreationType::RECREATE,
      StreamingRole replay_role = StreamingRole::TRANSFORM,
      bool remove_meta_file = false);

  void StreamingExactlySameSourceTest();

  void StreamingExactlySameOperatorTest();
};

}  // namespace streaming
}  // namespace ray