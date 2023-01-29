#include "ray/util/opentsdb_client.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

static std::string CERESDB_URL = "ceresdb-daily.alibaba.net";
static int CERESDB_PORT = 5000;
static std::string REPORT_USER = "raychild";
static std::string REPORT_TOKEN = "randoa";
namespace ray {
class CeresdbTest : public ::testing::Test {
 public:
  void SetUp() {
    db_client = std::make_shared<OpentsdbClient>(CERESDB_URL, CERESDB_PORT, REPORT_USER,
                                                 REPORT_TOKEN);
  }
  void TearDown() { db_client.reset(); }

 protected:
  std::shared_ptr<OpentsdbClient> db_client;
};

TEST_F(CeresdbTest, PointExchange) {
  OpentsdbPoint point{.metric_name = "a1",
                      .timestamp = 10,
                      .value = 1.0,
                      .tags = std::unordered_map<std::string, std::string>{}};
  ASSERT_EQ(point.ToJsonString(),
            R"({"metric":"a1","timestamp":10,"value":1.000000,"tags":{}})");
  OpentsdbPoint point2{.metric_name = "a2",
                       .timestamp = 20,
                       .value = 1.1,
                       .tags = {{"k1", "v1"}, {"k2", "v2"}}};
  ASSERT_EQ(
      point2.ToJsonString(),
      R"({"metric":"a2","timestamp":20,"value":1.100000,"tags":{"k2":"v2","k1":"v1"}})");
}

TEST_F(CeresdbTest, Queries) {
  OpentsdbQuery query{.aggregator = "sum",
                      .metric_name = "sys.cpu.0",
                      .tags = {{"host", "*"}, {"dc", "lga"}}};
  std::vector<OpentsdbQuery> queries;
  queries.push_back(query);
  OpentsdbRequest request{.start_timestamp = 1356998400000,
                          .end_timestamp = 1356998460000,
                          .queries = queries};
  ASSERT_EQ(
      request.ToJsonString(),
      R"({"start":1356998400000,"end":1356998460000,"queries":[{"aggregator":"sum","metric":"sys.cpu.0","tags":{"dc":"lga","host":"*"}}]})");
}

TEST_F(CeresdbTest, DISABLED_single_point_report) {
  int64_t ts = current_sys_time_ms();
  for (int i = 0; i < 50; ++i) {
    OpentsdbPoint point{.metric_name = "ray.ceresdb.a1",
                        .timestamp = current_sys_time_ms(),
                        .value = (double)(current_sys_time_ms() - ts),
                        .tags = {{"k1", "v1"}, {"k2", "v2"}}};
    db_client->Post(point);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

TEST_F(CeresdbTest, DISABLED_batch_report) {
  int64_t ts = current_sys_time_ms();
  for (int i = 0; i < 50; ++i) {
    std::vector<OpentsdbPoint> points;
    for (int j = 0; j < 10; ++j) {
      OpentsdbPoint point{.metric_name = "ray.ceresdb.a2",
                          .timestamp = current_sys_time_ms(),
                          .value = (double)i,
                          .tags = {{"k1", "v1"}, {"k2", "v2"}}};
      points.push_back(point);
    }
    db_client->Post(points);
    OpentsdbQuery query{
        .aggregator = "sum", .metric_name = "ray.ceresdb.a2", .tags = {{"k1", "*"}}};
    std::vector<OpentsdbQuery> queries;
    queries.push_back(query);
    OpentsdbRequest request{.start_timestamp = ts,
                            .end_timestamp = current_sys_time_ms(),
                            .queries = queries};
    RAY_LOG(DEBUG) << "Request json str : " << request.ToJsonString();
    auto res = db_client->Query(request);
    RAY_LOG(DEBUG) << "Response json str : " << res.second;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  if (argc >= 5) {
    CERESDB_URL = argv[1];
    CERESDB_PORT = std::atoi(argv[2]);
    REPORT_USER = argv[3];
    REPORT_TOKEN = argv[4];
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
