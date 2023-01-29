#include <cstdlib>

#include "channel/channel.h"
#include "gtest/gtest.h"
#include "http_profiler.h"
#include "ray/common/id.h"
#include "ray/util/http_client.h"

using namespace ray::streaming;
using namespace ray;

class HttpProfilerTests : public ::testing::Test {
 protected:
  void RegisterProducerProfiler(
      const std::unordered_map<ObjectID, ProducerChannelInfo> &producer_map) {
    HttpRouter::Register("/streaming/profiler/test", "Profiler Test",
                         [&producer_map](HttpParams &&params, std::string &&data,
                                         std::shared_ptr<HttpReply> r) {
                           rapidjson::Document ret(rapidjson::kObjectType);
                           for (auto &item : producer_map) {
                             rapidjson::Value key;
                             key.Set(item.first.Hex(), ret.GetAllocator());

                             rapidjson::Value val;
                             val.Set(item.second.current_bundle_id, ret.GetAllocator());
                             ret.AddMember(key, val, ret.GetAllocator());
                           }
                           r->SetJsonContent(rapidjson::to_string(ret));
                         });
  }

  virtual void SetUp() override {
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    http_profiler = HttpProfiler::GetHttpProfilerInstance();
  }
  virtual void TearDown() override { StreamingLog::ShutDownStreamingLog(); }

 protected:
  std::shared_ptr<HttpProfiler> http_profiler;
};

TEST_F(HttpProfilerTests, writer_profiler) {
  std::unordered_map<ObjectID, ProducerChannelInfo> producer_info_map;
  std::vector<ObjectID> id_vec;
  std::unordered_map<std::string, uint64_t> expected;
  for (uint64_t i = 0; i < 3; ++i) {
    ObjectID queue_id = ray::ObjectID::FromRandom();
    id_vec.push_back(queue_id);
    producer_info_map[queue_id].current_bundle_id = i;
    expected.emplace(queue_id.Hex(), i);
  }
  RegisterProducerProfiler(producer_info_map);
  HttpSyncClient client;
  int port = http_profiler->GetPort();
  client.Connect("0.0.0.0", port);
  rapidjson::Document doc;
  std::unordered_map<std::string, std::string> dummpy_params;
  auto ret_body = client.Get("/streaming/profiler/test", dummpy_params);
  std::string ret = ret_body.second;
  STREAMING_LOG(INFO) << ret;
  STREAMING_LOG(DEBUG) << "No such debug log";
  doc.Parse(ret);
  auto obj = doc.GetObject();
  ASSERT_EQ(expected.size(), obj.MemberCount());
  for (auto &item : obj) {
    ASSERT_TRUE(expected.count(item.name.GetString()));
    ASSERT_EQ(expected[item.name.GetString()], item.value.GetUint64());
  }

  auto update_body = client.Get("/update", {{"log_level", "debug"}});
  ret = update_body.second;
  STREAMING_LOG(INFO) << ret;
  STREAMING_LOG(DEBUG) << "Debug";
  client.Get("/update", {{"log_level", "info"}});
  STREAMING_LOG(DEBUG) << "Debug 2";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
