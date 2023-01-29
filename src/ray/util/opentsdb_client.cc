#include "ray/util/opentsdb_client.h"

#include "curl/curl.h"
#include "ray/util/http_router.h"

namespace ray {
OpentsdbClient::OpentsdbClient(const std::string &ceresdb_ip, const int ceresdb_port,
                               const std::string &access_user,
                               const std::string &access_key)
    : ceresdb_ip_(ceresdb_ip), access_user_(access_user), access_key_(access_key) {
  url_ = "http://" + ceresdb_ip + ":" + std::to_string(ceresdb_port);

  const auto &map = CeresdbAuthHeader();
  for (auto &pair : map) {
    std::string header_str;
    header_str.append(pair.first).append(":").append(pair.second);
    http_headers_.push_back(std::move(header_str));
  }
}

OpentsdbClient::~OpentsdbClient() { RAY_LOG(INFO) << "Ceresdb client stopped."; }

OpentsdbResponse OpentsdbClient::Post(const OpentsdbPoint &point) {
  std::vector<OpentsdbPoint> points;
  points.push_back(point);
  return Post(points);
}

inline size_t WriteToString(void *ptr, size_t size, size_t count, void *stream) {
  ((std::string *)stream)->append((char *)ptr, 0, size * count);
  return size * count;
}

OpentsdbResponse OpentsdbClient::SendMsg(std::string path, std::string &&post_info) {
  std::string response;
  std::string url = url_ + path;
  bool success = false;
  int retry_times = 3;
  CURLcode ret = CURLcode::CURL_LAST;
  do {
    CURL *curl_handle;
    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    for (auto &header : http_headers_) {
      headers = curl_slist_append(headers, header.c_str());
    }
    curl_handle = curl_easy_init();
    if (curl_handle) {
      curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);
      curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10);
      curl_easy_setopt(curl_handle, CURLOPT_POST, 1L);
      curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, post_info.c_str());
      curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, post_info.size());

      curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteToString);
      curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &response);
      ret = curl_easy_perform(curl_handle);
      curl_easy_cleanup(curl_handle);
      success = (ret == CURLcode::CURLE_OK);
    }
    curl_slist_free_all(headers);
  } while (!success && --retry_times > 0);
  if (!success) {
    RAY_LOG(WARNING) << "Ceresdb client post failed, curl code: " << ret
                     << ", reason: " << response;
  }
  boost::system::error_code ec =
      success ? boost::system::error_code{} : boost::asio::error::basic_errors::fault;
  return std::make_pair(ec, std::move(response));
}

OpentsdbResponse OpentsdbClient::Post(const std::vector<OpentsdbPoint> &points) {
  auto json_data =
      std::string("[") +
      StrJoin(points.begin(), points.end(),
              [](decltype(points.begin()) it) { return it->ToJsonString(); }, ",") +
      "]";

  return SendMsg("/api/put", std::move(json_data));
}

OpentsdbResponse OpentsdbClient::Query(const OpentsdbRequest &request) {
  return SendMsg("/api/query", request.ToJsonString());
}

}  // namespace ray
