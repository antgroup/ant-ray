#include "hiredis/hiredis.h"
#include "ray/common/test_util.h"
#include "ray/util/filesystem.h"
namespace ray {
namespace streaming {

ray::ObjectID RandomObjectID() { return ObjectID::FromRandom(); }

static void flushall_redis(void) {
  redisContext *context = ::redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);

  context = redisConnect("127.0.0.1", 6380);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}
/// Base class for real-world tests with streaming queue
class StreamingQueueTestBase : public ::testing::TestWithParam<uint64_t> {
 public:
  StreamingQueueTestBase(int num_nodes, int node_manager_port)
      : gcs_options_("127.0.0.1", 6379, ""), node_manager_port_(node_manager_port) {
    RAY_LOG(INFO) << "StreamingQueueTestBase construct";
    TestSetupUtil::StartUpRedisServers(std::vector<int>{6379, 6380});
    // flush redis first.
    flushall_redis();

    RAY_CHECK(num_nodes >= 0);
    if (num_nodes > 0) {
      raylet_socket_names_.resize(num_nodes);
      store_socket_names_.resize(num_nodes);
    }

    // start gcs server
    gcs_server_pid_ = TestSetupUtil::StartGcsServer("127.0.0.1");

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node. Plasma store will start by raylet.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] = TestSetupUtil::StartRaylet(
          "127.0.0.1", node_manager_port_ + i, "127.0.0.1",
          "\"CPU,4.0,resource" + std::to_string(i) + ",10,memory,10\"",
          &store_socket_names_[i]);
    }
    RAY_LOG(INFO) << "StreamingQueueTestBase construct done";
  }

  ~StreamingQueueTestBase() {
    STREAMING_LOG(INFO) << "Stop raylet store and actors";
    for (const auto &raylet_socket : raylet_socket_names_) {
      TestSetupUtil::StopRaylet(raylet_socket);
    }

    if (!gcs_server_pid_.empty()) {
      TestSetupUtil::StopGcsServer(gcs_server_pid_);
    }

    TestSetupUtil::ShutDownRedisServers();
  }

  std::string GetStoreSocketName() { return store_socket_names_[0]; }

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  void SubmitActorTask(const ActorID &self_actor_id,
                       std::shared_ptr<LocalMemoryBuffer> buffer, RayFunction &func,
                       std::vector<ObjectID> &return_ids, int num_return) {
    std::unordered_map<std::string, double> resources;
    TaskOptions options{"", num_return, resources};
    std::vector<std::unique_ptr<TaskArg>> args;
    char meta_data[3] = {'R', 'A', 'W'};
    std::shared_ptr<LocalMemoryBuffer> meta =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

    auto dummy = "__RAY_DUMMY__";
    std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy, 13, true);
    args.emplace_back(
        std::unique_ptr<TaskArg>(new TaskArgByValue(std::make_shared<RayObject>(
            std::move(dummyBuffer), meta, std::vector<ObjectID>()))));

    args.emplace_back(std::unique_ptr<TaskArg>(new TaskArgByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>()))));
    CoreWorkerProcess::GetCoreWorker().SubmitActorTask(self_actor_id, func, args, options,
                                                       &return_ids);
  }

  void InitWorker(const ActorID &self_actor_id, const ActorID &peer_actor_id,
                  const queue::flatbuf::StreamingQueueTestRole role,
                  const std::vector<ObjectID> &queue_ids,
                  const std::vector<ObjectID> &rescale_queue_ids, std::string suite_name,
                  std::string test_name, uint64_t param) {
    std::string serialized_str;
    ray::ObjectID actor_handle_id;
    RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SerializeActorHandle(
        peer_actor_id, &serialized_str, &actor_handle_id));
    STREAMING_LOG(DEBUG) << "serialized_str: " << serialized_str;
    TestInitMsg msg(role, self_actor_id, peer_actor_id, serialized_str, queue_ids,
                    rescale_queue_ids, suite_name, test_name, param,
                    store_socket_names_[0]);

    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON,
                     ray::FunctionDescriptorBuilder::FromVector(ray::Language::PYTHON,
                                                                {"", "", "init", ""}));
    SubmitActorTask(self_actor_id, msg.ToBytes(), func, return_ids, 0);
  }

  void SubmitTestToActor(const ActorID &actor_id, const std::string test) {
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);

    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON,
                     ray::FunctionDescriptorBuilder::FromVector(
                         ray::Language::PYTHON, {"", test, "execute_test", ""}));
    SubmitActorTask(actor_id, buffer, func, return_ids, 0);
  }

  void ShutdownWorker(const ActorID &actor_id) {
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);

    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON,
                     ray::FunctionDescriptorBuilder::FromVector(
                         ray::Language::PYTHON, {"", "", "shutdown", ""}));
    SubmitActorTask(actor_id, buffer, func, return_ids, 0);
  }

  bool CheckCurTest(const ActorID &actor_id, const std::string test_name) {
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);

    std::vector<ObjectID> return_ids;
    RayFunction func(
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "check_current_test_status", ""}));
    SubmitActorTask(actor_id, buffer, func, return_ids, 1);

    std::vector<bool> wait_results;
    std::vector<std::shared_ptr<RayObject>> results;
    Status wait_st = CoreWorkerProcess::GetCoreWorker().Wait(return_ids, 1, 5 * 1000,
                                                             &wait_results, true);
    if (!wait_st.ok()) {
      STREAMING_LOG(ERROR) << "Wait fail.";
      return false;
    }
    STREAMING_CHECK(wait_results.size() >= 1);
    if (!wait_results[0]) {
      STREAMING_LOG(WARNING) << "Wait direct call fail.";
      return false;
    }

    Status get_st = CoreWorkerProcess::GetCoreWorker().Get(return_ids, -1, &results);
    if (!get_st.ok()) {
      STREAMING_LOG(ERROR) << "Get fail.";
      return false;
    }
    STREAMING_CHECK(results.size() >= 1);
    if (results[0]->IsException()) {
      STREAMING_LOG(INFO) << "peer actor may has exceptions.";
      return false;
    }
    STREAMING_CHECK(results[0]->HasData());
    STREAMING_LOG(DEBUG) << "SendForResult result[0] DataSize: " << results[0]->GetSize();

    const std::shared_ptr<ray::Buffer> result_buffer = results[0]->GetData();
    std::shared_ptr<LocalMemoryBuffer> return_buffer =
        std::make_shared<LocalMemoryBuffer>(result_buffer->Data(), result_buffer->Size(),
                                            true);

    uint8_t *bytes = result_buffer->Data();
    uint8_t *p_cur = bytes;
    uint32_t *magic_num = (uint32_t *)p_cur;
    STREAMING_CHECK(*magic_num == Message::MagicNum);

    p_cur += sizeof(Message::MagicNum);
    queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;
    STREAMING_CHECK(*type ==
                    queue::flatbuf::MessageType::StreamingQueueTestCheckStatusRspMsg);
    std::shared_ptr<TestCheckStatusRspMsg> message =
        TestCheckStatusRspMsg::FromBytes(bytes);
    STREAMING_CHECK(message->TestName() == test_name);
    return message->Status();
  }

  ActorID CreateActorHelper(const std::unordered_map<std::string, double> &resources,
                            int64_t max_reconstructions) {
    ActorID actor_id;

    // Test creating actor.
    uint8_t array[] = {1, 2, 3};
    auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

    RayFunction func(
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(
            ray::Language::PYTHON, {"", "", "mock_actor_creation_task", ""}));
    std::vector<std::unique_ptr<TaskArg>> args;
    char meta_data[3] = {'R', 'A', 'W'};
    std::shared_ptr<LocalMemoryBuffer> meta =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

    auto dummy = "__RAY_DUMMY__";
    std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy, 13, true);
    args.emplace_back(
        std::unique_ptr<TaskArg>(new TaskArgByValue(std::make_shared<RayObject>(
            std::move(dummyBuffer), meta, std::vector<ObjectID>()))));
    args.emplace_back(std::unique_ptr<TaskArg>(new TaskArgByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>()))));

    std::string name = "";
    std::string ray_namespace = "";
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    ActorCreationOptions actor_options{max_reconstructions,
                                       /*max_task_retries=*/0,
                                       /*max_concurrency*/ 1,
                                       resources,
                                       resources,
                                       {},
                                       /*is_detached*/ std::make_shared<bool>(false),
                                       name,
                                       /*ray_namespace*/ ray_namespace,
                                       /*is_asyncio*/ false,
                                       scheduling_strategy};
    // Create an actor.
    RAY_UNUSED(CoreWorkerProcess::GetCoreWorker().CreateActor(func, args, actor_options,
                                                              /*extension_data*/ "",
                                                              &actor_id));
    return actor_id;
  }

  void SubmitTest(uint32_t queue_num, std::string suite_name, std::string test_name,
                  uint64_t timeout_ms) {
    std::vector<ray::ObjectID> queue_id_vec;
    std::vector<ray::ObjectID> rescale_queue_id_vec;
    for (uint32_t i = 0; i < queue_num; ++i) {
      ObjectID queue_id = ray::ObjectID::FromRandom();
      ConvertToValidQueueId(queue_id);
      queue_id_vec.emplace_back(queue_id);
    }

    // One scale id
    ObjectID rescale_queue_id = ray::ObjectID::FromRandom();
    ConvertToValidQueueId(rescale_queue_id);
    rescale_queue_id_vec.emplace_back(rescale_queue_id);

    std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);

    for (size_t i = 0; i < queue_id_vec.size(); ++i) {
      STREAMING_LOG(INFO) << " qid hex => " << queue_id_vec[i].Hex();
    }
    for (auto &qid : rescale_queue_id_vec) {
      STREAMING_LOG(INFO) << " rescale qid hex => " << qid.Hex();
    }
    STREAMING_LOG(INFO) << "Sub process: writer.";

    // Create writer and reader actors
    std::unordered_map<std::string, double> resources{{"CPU", 1.0}};
    auto actor_id_writer = CreateActorHelper(resources, 0);
    auto actor_id_reader = CreateActorHelper(resources, 0);

    InitWorker(actor_id_writer, actor_id_reader,
               queue::flatbuf::StreamingQueueTestRole::WRITER, queue_id_vec,
               rescale_queue_id_vec, suite_name, test_name, GetParam());
    InitWorker(actor_id_reader, actor_id_writer,
               queue::flatbuf::StreamingQueueTestRole::READER, queue_id_vec,
               rescale_queue_id_vec, suite_name, test_name, GetParam());

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    SubmitTestToActor(actor_id_writer, test_name);
    SubmitTestToActor(actor_id_reader, test_name);

    uint64_t slept_time_ms = 0;
    bool writer_has_done = false;
    bool reader_has_done = false;
    while (slept_time_ms < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5 * 1000));
      STREAMING_LOG(INFO) << "writer_has_done " << writer_has_done << " reader_has_done "
                          << reader_has_done;
      if (!writer_has_done && CheckCurTest(actor_id_writer, test_name))
        writer_has_done = true;
      if (!reader_has_done && CheckCurTest(actor_id_reader, test_name))
        reader_has_done = true;
      if (writer_has_done && reader_has_done) {
        ShutdownWorker(actor_id_writer);
        ShutdownWorker(actor_id_reader);
        STREAMING_LOG(INFO) << "Test Success, Exit.";
        return;
      }
      slept_time_ms += 5 * 1000;
    }

    EXPECT_TRUE(false);
    STREAMING_LOG(INFO) << "Test Timeout, Exit.";
  }

  void SetUp() {
    RAY_LOG(INFO) << "StreamingQueueTestBase SetUp";
    CoreWorkerOptions options;
    options.worker_type = WorkerType::DRIVER;
    options.language = Language::PYTHON;
    options.store_socket = store_socket_names_[0];
    options.raylet_socket = raylet_socket_names_[0];
    options.job_id = NextJobId();
    options.gcs_options = gcs_options_;
    options.enable_logging = true;
    options.log_dir = "/tmp/mylog/";
    options.install_failure_signal_handler = true;
    options.node_ip_address = "127.0.0.1";
    options.node_manager_port = node_manager_port_;
    options.raylet_ip_address = "127.0.0.1";
    options.driver_name = "streaming_queue_test";
    options.ref_counting_enabled = true;
    options.num_workers = 1;
    options.metrics_agent_port = -1;
    CoreWorkerProcess::Initialize(options);
    RAY_LOG(INFO) << "StreamingQueueTestBase SetUp done";
  }

  void TearDown() { CoreWorkerProcess::Shutdown(/*shutdown_with_error=*/false); }

 protected:
  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> store_socket_names_;
  gcs::GcsClientOptions gcs_options_;
  std::string raylet_executable_;
  std::string store_executable_;
  std::string actor_executable_;
  int node_manager_port_;
  std::string gcs_server_pid_;
  std::string gcs_server_executable_;
};

}  // namespace streaming
}  // namespace ray
