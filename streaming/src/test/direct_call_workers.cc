#define BOOST_BIND_NO_PLACEHOLDERS
#include "test/it/streaming_exactly_same_writer_tests.h"
#include "test/it/streaming_queue_tests.h"
#include "test/it/streaming_rescale_writer_tests.h"
#include "test/it/streaming_test_suite.h"
#include "test/it/streaming_writer_tests.h"
using namespace std::placeholders;

namespace ray {
namespace streaming {

class TestSuiteFactory {
 public:
  static std::shared_ptr<StreamingQueueTestSuite> CreateTestSuite(
      const WorkerID &worker_id, ActorID peer_actor_id,
      std::shared_ptr<TestInitMsg> message) {
    std::shared_ptr<StreamingQueueTestSuite> test_suite = nullptr;
    std::string suite_name = message->TestSuiteName();
    STREAMING_LOG(INFO) << "suite_name: " << suite_name;
    queue::flatbuf::StreamingQueueTestRole role = message->Role();
    const std::vector<ObjectID> &queue_ids = message->QueueIds();
    const std::vector<ObjectID> &rescale_queue_ids = message->RescaleQueueIds();

    if (role == queue::flatbuf::StreamingQueueTestRole::WRITER) {
      if (suite_name == "StreamingWriterTest") {
        test_suite = std::make_shared<StreamingQueueWriterTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids);
      } else if (suite_name == "StreamingRescaleTest") {
        test_suite = std::make_shared<StreamingRescaleWriterTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids);
      } else if (suite_name == "StreamingExactlySameTest") {
        test_suite = std::make_shared<StreamingExactlySameWriterTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids, message->Param());
      } else if (suite_name == "StreamingQueueTest") {
        test_suite = std::make_shared<StreamingQueueUpStreamTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids, message->Param());
      } else {
        STREAMING_CHECK(false) << "unsurported suite_name: " << suite_name;
      }
    } else {
      if (suite_name == "StreamingWriterTest") {
        test_suite = std::make_shared<StreamingQueueReaderTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids);
      } else if (suite_name == "StreamingRescaleTest") {
        test_suite = std::make_shared<StreamingRescaleReaderTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids);
      } else if (suite_name == "StreamingExactlySameTest") {
        test_suite = std::make_shared<StreamingExactlySameReaderTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids, message->Param());
      } else if (suite_name == "StreamingQueueTest") {
        test_suite = std::make_shared<StreamingQueueDownStreamTestSuite>(
            worker_id, peer_actor_id, queue_ids, rescale_queue_ids, message->Param());
      } else {
        STREAMING_CHECK(false) << "unsurported suite_name: " << suite_name;
      }
    }

    test_suite->SetPlasmaStoreSocket(message->PlasmaStoreSocket());
    return test_suite;
  }
};

class StreamingWorker {
 public:
  StreamingWorker(const std::string &store_socket, const std::string &raylet_socket,
                  int node_manager_port, const gcs::GcsClientOptions &gcs_options)
      : test_suite_(nullptr) {
    STREAMING_LOG(INFO) << "StreamingWorker constructor";

    CoreWorkerOptions options;
    options.worker_type = WorkerType::WORKER;
    options.language = Language::PYTHON;
    options.store_socket = store_socket;
    options.raylet_socket = raylet_socket;
    options.gcs_options = gcs_options;
    options.enable_logging = true;
    options.log_dir = "/tmp/mylog/";
    options.install_failure_signal_handler = true;
    options.node_ip_address = "127.0.0.1";
    options.node_manager_port = node_manager_port;
    options.raylet_ip_address = "127.0.0.1";
    options.task_execution_callback = std::bind(&StreamingWorker::ExecuteTask, this, _1,
                                                _2, _3, _4, _5, _6, _7, _8, _9);
    options.ref_counting_enabled = true;
    options.num_workers = 1;
    options.metrics_agent_port = -1;
    CoreWorkerProcess::Initialize(options);
  }

  void RunTaskExecutionLoop() { CoreWorkerProcess::RunTaskExecutionLoop(); }

 private:
  Status ExecuteTask(TaskType task_type, const std::string task_name,
                     const RayFunction &ray_function,
                     const std::unordered_map<std::string, double> &required_resources,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const std::vector<ObjectID> &arg_reference_ids,
                     const std::vector<ObjectID> &return_ids,
                     const std::string &debugger_breakpoint,
                     std::vector<std::shared_ptr<RayObject>> *results) {
    // Note that this doesn't include dummy object id.
    // STREAMING_CHECK(num_returns >= 0);
    // Only one arg param used in streaming.
    STREAMING_CHECK(args.size() >= 2 && args.size() <= 6) << args.size();

    std::shared_ptr<PythonFunctionDescriptor> function_descriptor =
        std::dynamic_pointer_cast<PythonFunctionDescriptor>(
            ray_function.GetFunctionDescriptor());
    auto data = args[1]->GetData();
    std::shared_ptr<LocalMemoryBuffer> local_buffer =
        std::make_shared<LocalMemoryBuffer>(data->Data(), data->Size(), true);
    std::string func_name = function_descriptor->FunctionName();
    STREAMING_LOG(DEBUG) << "StreamingWorker::ExecuteTask func_name "
                         << function_descriptor->FunctionName();
    if (func_name == "init") {
      HandleInitTask(local_buffer);
    } else if (func_name == "execute_test") {
      STREAMING_LOG(INFO) << "Test name: " << function_descriptor->ClassName();
      test_suite_->ExecuteTest(function_descriptor->ClassName());
    } else if (func_name == "check_current_test_status") {
      results->push_back(std::make_shared<RayObject>(test_suite_->CheckCurTestStatus(),
                                                     nullptr, std::vector<ObjectID>()));
    } else if (func_name == "shutdown") {
      STREAMING_LOG(INFO) << "Worker Shutdown";
      CoreWorkerProcess::GetCoreWorker().Shutdown();
    } else if (func_name == "sync_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(DEBUG) << "Test has done!!";
        uint8_t data[4];
        results->push_back(std::make_shared<RayObject>(
            std::make_shared<LocalMemoryBuffer>(data, 4, true), nullptr,
            std::vector<ObjectID>()));
        return Status::OK();
      }
      auto result_buffer = test_suite_->OnMessageSync(local_buffer);
      results->push_back(
          std::make_shared<RayObject>(result_buffer, nullptr, std::vector<ObjectID>()));
    } else if (func_name == "async_call_func") {
      if (test_suite_->TestDone()) {
        STREAMING_LOG(DEBUG) << "Test has done!!";
        return Status::OK();
      }
      STREAMING_LOG(DEBUG) << "args.size() = " << args.size();
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers;
      for (size_t index = 1; index < args.size(); index += 2) {
        /// +2 to skip __RAY_DUMMY__ in python arguments.
        auto data = args[index]->GetData();
        buffers.push_back(
            std::make_shared<LocalMemoryBuffer>(data->Data(), data->Size(), true));
      }
      test_suite_->OnMessage(buffers);
    } else if (func_name == "mock_actor_creation_task") {
    } else if (func_name == "direct_call_perf_test_func") {
      DirectCallPerfTest(local_buffer);
    } else {
      STREAMING_CHECK(false) << "Invalid function name " << func_name;
    }

    return Status::OK();
  }

 private:
  void DirectCallPerfTest(std::shared_ptr<LocalMemoryBuffer> buffer) {
    static uint64_t total_count = 0;
    static uint64_t large_count = 0;
    static uint64_t qps_timestamp = current_sys_time_ms();
    total_count++;
    uint64_t current = current_sys_time_ms();
    uint8_t *data = buffer->Data();
    uint64_t *timestamp = (uint64_t *)data;
    if (current - *timestamp > 10) {
      large_count++;
    }
    if (total_count % 10000 == 0) {
      STREAMING_LOG(INFO) << "10000 tasks, QPS: "
                          << 10000 / ((float)(current - qps_timestamp) / 1000) << "/s"
                          << " larger than 10ms latency count: " << large_count;
      large_count = 0;
      qps_timestamp = current;
    }
    // STREAMING_LOG(INFO) << "latency: " << current - *timestamp;
  }

  void HandleInitTask(std::shared_ptr<LocalMemoryBuffer> buffer) {
    uint8_t *bytes = buffer->Data();
    uint8_t *p_cur = bytes;
    uint32_t *magic_num = (uint32_t *)p_cur;
    STREAMING_CHECK(*magic_num == Message::MagicNum);

    auto &worker = CoreWorkerProcess::GetCoreWorker();
    p_cur += sizeof(Message::MagicNum);
    queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;
    STREAMING_CHECK(*type == queue::flatbuf::MessageType::StreamingQueueTestInitMsg);
    std::shared_ptr<TestInitMsg> message = TestInitMsg::FromBytes(bytes);

    STREAMING_LOG(INFO) << "Init message";
    std::string actor_handle_serialized = message->ActorHandleSerialized();

    STREAMING_LOG(DEBUG) << "actor_handle_serialized: " << actor_handle_serialized;
    ray::ObjectID outer;
    peer_actor_id_ =
        worker.DeserializeAndRegisterActorHandle(actor_handle_serialized, outer);

    STREAMING_LOG(INFO) << "HandleInitTask queues:";
    for (auto qid : message->QueueIds()) {
      STREAMING_LOG(INFO) << "queue: " << qid;
    }
    for (auto qid : message->RescaleQueueIds()) {
      STREAMING_LOG(INFO) << "rescale queue: " << qid;
    }

    test_suite_ =
        TestSuiteFactory::CreateTestSuite(worker.GetWorkerID(), peer_actor_id_, message);
    STREAMING_CHECK(test_suite_ != nullptr);
  }

 private:
  std::shared_ptr<std::thread> test_thread_;
  std::shared_ptr<StreamingQueueTestSuite> test_suite_;
  ActorID peer_actor_id_;
};

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  STREAMING_LOG(INFO) << "direct_call_worker main, argc: " << argc;
  RAY_CHECK(argc == 4);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);
  auto node_manager_port = std::stoi(std::string(argv[3]));

  ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, "");
  ray::streaming::StreamingWorker worker(store_socket, raylet_socket, node_manager_port,
                                         gcs_options);
  auto term = [](int num) {
    STREAMING_LOG(WARNING) << "Caught SIGTERM";
    _exit(0);
  };
  std::signal(SIGTERM, term);
  worker.RunTaskExecutionLoop();
  return 0;
}
