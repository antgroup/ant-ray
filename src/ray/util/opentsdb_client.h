#ifndef RAY_UTIL_CERESDB_CLIENT_H
#define RAY_UTIL_CERESDB_CLIENT_H
#include <string>
#include <unordered_map>
#include <vector>

#include "http_client.h"
#include "util.h"

#define METRIC_AROUND(M) std::string("\"") + M + std::string("\"")
namespace ray {
struct OpentsdbPoint {
  std::string metric_name;
  int64_t timestamp;
  double value;
  std::unordered_map<std::string, std::string> tags;

  std::string ToJsonString() const {
    static std::string metric_key = METRIC_AROUND("metric");
    static std::string timestamp_key = METRIC_AROUND("timestamp");
    static std::string value_key = METRIC_AROUND("value");
    static std::string tags_key = METRIC_AROUND("tags");

    std::string flatten_str = "{" + metric_key + ":" + METRIC_AROUND(metric_name) + "," +
                              timestamp_key + ":" + std::to_string(timestamp) + "," +
                              value_key + ":" + std::to_string(value) + "," + tags_key +
                              ":{";

    flatten_str +=
        StrJoin(tags.begin(), tags.end(),
                [](decltype(tags.begin()) it) -> std::string {
                  return METRIC_AROUND(it->first) + ":" + METRIC_AROUND(it->second);
                },
                ",") +
        "}}";
    return flatten_str;
  }
};

struct OpentsdbQuery {
  // Aggregator options :
  // ["avg","sum","count","max","min","none","p999","p99","p95",
  //  "p90","p75","p50","mimmax","mimmin","zimsum"].
  std::string aggregator;
  // Query metric name.
  std::string metric_name;
  // Query tags "tags" : { "host" : "*", "dc" : "lgs" }.
  std::unordered_map<std::string, std::string> tags;
  std::string ToJsonString() const {
    std::string josn_str =
        "{" + METRIC_AROUND("aggregator") + ":" + METRIC_AROUND(aggregator) + "," +
        METRIC_AROUND("metric") + ":" + METRIC_AROUND(metric_name) + "," +
        METRIC_AROUND("tags") + ":{" +
        StrJoin(tags.begin(), tags.end(),
                [](decltype(tags.begin()) it) -> std::string {
                  return METRIC_AROUND(it->first) + ":" + METRIC_AROUND(it->second);
                },
                ",") +
        +"}}";
    return josn_str;
  }
};

struct OpentsdbRequest {
  int64_t start_timestamp;
  int64_t end_timestamp;
  std::vector<OpentsdbQuery> queries;
  std::string ToJsonString() const {
    std::string flatten_str = "{" + METRIC_AROUND("start") + ":" +
                              std::to_string(start_timestamp) + "," +
                              METRIC_AROUND("end") + ":" + std::to_string(end_timestamp) +
                              "," + METRIC_AROUND("queries") + ":[" +
                              StrJoin(queries.begin(), queries.end(),
                                      [](decltype(queries.begin()) it) -> std::string {
                                        return it->ToJsonString();
                                      },
                                      ",") +
                              "]}";
    return flatten_str;
  }
};

using OpentsdbResponse = std::pair<boost::system::error_code, std::string>;

class OpentsdbClient {
 public:
  OpentsdbClient(const std::string &ceresdb_ip, const int ceresdb_port,
                 const std::string &access_user, const std::string &access_key);
  virtual ~OpentsdbClient();
  OpentsdbResponse Post(const std::vector<OpentsdbPoint> &points);
  OpentsdbResponse Post(const OpentsdbPoint &point);
  OpentsdbResponse Query(const OpentsdbRequest &request);

 private:
  inline std::unordered_map<std::string, std::string> CeresdbAuthHeader() {
    std::string current_ts_str = std::to_string(current_sys_time_ms());
    return {{"X-CeresDB-AccessToken", MD5Digest(access_key_ + current_ts_str)},
            {"X-CeresDB-AccessUser", access_user_},
            {"X-CeresDB-Timestamp", current_ts_str}};
  }

  OpentsdbResponse SendMsg(std::string path, std::string &&post_info);

 private:
  std::string ceresdb_ip_;
  std::string url_;
  std::string access_user_;
  std::string access_key_;

  static constexpr int kMaxRetryTimes = 3;
  std::vector<std::string> http_headers_;
};
}  // namespace ray
#endif
