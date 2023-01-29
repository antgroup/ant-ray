#pragma once
#include "test/it/streaming_test_suite.h"

namespace ray {
namespace streaming {

class StreamingQueueUpStreamTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueUpStreamTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                  const std::vector<ObjectID> &queue_ids,
                                  const std::vector<ObjectID> &rescale_queue_ids,
                                  uint64_t param)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"pull_peer_async_test",
         std::bind(&StreamingQueueUpStreamTestSuite::PullPeerAsyncTest, this)},
        {"get_queue_test",
         std::bind(&StreamingQueueUpStreamTestSuite::GetQueueTest, this)},
        {"delete_queue_test",
         std::bind(&StreamingQueueUpStreamTestSuite::DeleteQueueTest, this)},
        {"get_peer_last_msg_id",
         std::bind(&StreamingQueueUpStreamTestSuite::GetPeerLastMsgIdTest, this)},
        {"direct_call_perf_test",
         std::bind(&StreamingQueueUpStreamTestSuite::DirectCallPerfTest, this)},
        {"direct_call_connection_broken_test",
         std::bind(&StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTest,
                   this)},
        {"direct_call_connection_broken_test_tcpkill",
         std::bind(
             &StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTestTcpKill,
             this)},
        {"multiple_args",
         std::bind(&StreamingQueueUpStreamTestSuite::MultipleArgs, this)},
        {"collocate_send_test",
         std::bind(&StreamingQueueUpStreamTestSuite::CollocateSendTest, this)},
        {"collocate_fo_test",
         std::bind(&StreamingQueueUpStreamTestSuite::CollocateFOTest, this)},
        {"get_peer_hostname",
         std::bind(&StreamingQueueUpStreamTestSuite::GetPeerHostnameTest, this)}};
  }

  void SetUp() override;
  void TearDown() override;

  class MockDataWriter : public DirectCallReceiver {
   public:
    explicit MockDataWriter(std::shared_ptr<UpstreamQueueMessageHandler> handler)
        : handler_(handler) {}
    virtual ~MockDataWriter() {}
    void OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffer) override {
      handler_->DispatchMessageAsync(buffer);
    }
    std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
        std::shared_ptr<LocalMemoryBuffer> buffer) override {
      return handler_->DispatchMessageSync(buffer);
    }
    void SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) override {}

   private:
    std::shared_ptr<UpstreamQueueMessageHandler> handler_;
  };

  void GetQueueTest();

  void PullPeerAsyncTest();

  void DeleteQueueTest();

  void GetPeerLastMsgIdTest();

  void DirectCallPerfTest();

  void DirectCallConnectionBrokenTest();

  void DirectCallConnectionBrokenTestTcpKill();

  void MultipleArgs();

  void CollocateSendTest();

  void CollocateFOTest();

  void GetPeerHostnameTest();

 private:
  std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler_;
};

class StreamingQueueDownStreamTestSuite : public StreamingQueueTestSuite {
 public:
  StreamingQueueDownStreamTestSuite(const WorkerID &worker_id, ActorID peer_actor_id,
                                    const std::vector<ObjectID> &queue_ids,
                                    const std::vector<ObjectID> &rescale_queue_ids,
                                    uint64_t param)
      : StreamingQueueTestSuite(worker_id, peer_actor_id, queue_ids, rescale_queue_ids) {
    test_func_map_ = {
        {"pull_peer_async_test",
         std::bind(&StreamingQueueDownStreamTestSuite::PullPeerAsyncTest, this)},
        {"get_queue_test",
         std::bind(&StreamingQueueDownStreamTestSuite::GetQueueTest, this)},
        {"delete_queue_test",
         std::bind(&StreamingQueueDownStreamTestSuite::DeleteQueueTest, this)},
        {"get_peer_last_msg_id",
         std::bind(&StreamingQueueDownStreamTestSuite::GetPeerLastMsgIdTest, this)},
        {"direct_call_perf_test",
         std::bind(&StreamingQueueDownStreamTestSuite::DirectCallPerfTest, this)},
        {"direct_call_connection_broken_test",
         std::bind(&StreamingQueueDownStreamTestSuite::DirectCallConnectionBrokenTest,
                   this)},
        {"direct_call_connection_broken_test_tcpkill",
         std::bind(
             &StreamingQueueDownStreamTestSuite::DirectCallConnectionBrokenTestTcpKill,
             this)},
        {"multiple_args",
         std::bind(&StreamingQueueDownStreamTestSuite::MultipleArgs, this)},
        {"collocate_send_test",
         std::bind(&StreamingQueueDownStreamTestSuite::CollocateSendTest, this)},
        {"collocate_fo_test",
         std::bind(&StreamingQueueDownStreamTestSuite::CollocateFOTest, this)},
        {"get_peer_hostname",
         std::bind(&StreamingQueueDownStreamTestSuite::GetPeerHostnameTest, this)}};
  };

  void SetUp() override;
  void TearDown() override;

  class MockDataReader : public DirectCallReceiver {
   public:
    MockDataReader(std::shared_ptr<DownstreamQueueMessageHandler> handler)
        : handler_(handler) {}
    virtual ~MockDataReader() {}
    void OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffer) override {
      handler_->DispatchMessageAsync(buffer);
    }
    std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
        std::shared_ptr<LocalMemoryBuffer> buffer) override {
      return handler_->DispatchMessageSync(buffer);
    }
    void SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) override {}

   private:
    std::shared_ptr<DownstreamQueueMessageHandler> handler_;
  };

  void GetQueueTest();

  void PullPeerAsyncTest();

  void DeleteQueueTest();

  void GetPeerLastMsgIdTest();
  void DirectCallPerfTest();

  void DirectCallConnectionBrokenTest();
  void DirectCallConnectionBrokenTestTcpKill();

  void CollocateSendTest();

  void MultipleArgs();

  void CollocateFOTest();

  void GetPeerHostnameTest();

 private:
  std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler_;
};  // namespace streaming
}  // namespace streaming
}  // namespace ray
