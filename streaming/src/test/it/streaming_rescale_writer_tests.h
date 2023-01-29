#pragma once
#include "streaming_test_suite.h"

namespace ray {
namespace streaming {

class StreamingRescaleWriterTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingRescaleWriterTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                  const std::vector<ObjectID> &queue_ids,
                                  const std::vector<ObjectID> &rescale_queue_ids)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_rescale_exactly_once_test",
         std::bind(&StreamingRescaleWriterTestSuite::StreamingRescaleExactlyOnceTest,
                   this)}};
  }

  void PingMessage(std::shared_ptr<DataWriter> writer,
                   std::vector<ray::ObjectID> queue_id_vec, uint8_t *data,
                   size_t data_size);
  void PingGlobalBarrier(std::shared_ptr<DataWriter> writer, uint8_t *data,
                         size_t data_size, uint64_t barrier_id);
  void PingPartialBarrier(std::shared_ptr<DataWriter> writer, uint8_t *data,
                          size_t data_size, uint64_t global_barrier_id,
                          uint64_t barrier_id);

  void StreamingRescaleExactlyOnceTest();
};

class StreamingRescaleReaderTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingRescaleReaderTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                  const std::vector<ObjectID> &queue_ids,
                                  const std::vector<ObjectID> &rescale_queue_ids)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"streaming_rescale_exactly_once_test",
         std::bind(&StreamingRescaleReaderTestSuite::StreamingRescaleExactlyOnceTest,
                   this)}};
  }

  void PingMessage(std::shared_ptr<DataReader> reader,
                   std::vector<ray::ObjectID> queue_id_vec, uint8_t *data,
                   size_t data_size);

  void PingGlobalBarrier(std::shared_ptr<DataReader> reader, uint8_t *data,
                         size_t data_size, uint64_t barrier_id);

  void PingPartialBarrier(std::shared_ptr<DataReader> reader, uint8_t *data,
                          size_t data_size, uint64_t global_barrier_id,
                          uint64_t barrier_id);

  void StreamingRescaleExactlyOnceTest();
};

}  // namespace streaming
}  // namespace ray