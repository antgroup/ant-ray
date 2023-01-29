#ifndef RAY_STREAMING_HTTP_PROFILER_H
#define RAY_STREAMING_HTTP_PROFILER_H
#include <thread>

#include "ray/common/id.h"
#include "ray/util/http_server.h"
namespace ray {
namespace streaming {
class DataWriter;
class DataReader;

class HttpProfiler {
 public:
  static std::shared_ptr<HttpProfiler> &GetHttpProfilerInstance();
  virtual ~HttpProfiler();
  inline int GetPort() { return port_; };
  void RegisterDataWriter(DataWriter *writer);
  void UnregisterDataWriter();
  void RegisterDataReader(DataReader *reader);
  void UnregisterDataReader();
  void DumpProducerInfo(rapidjson::Document &ret);
  void DumpConsumerInfo(rapidjson::Document &ret);

 private:
  explicit HttpProfiler();
  void StartServer();
  void RegisterWriterRoute();
  void RegisterReaderRoute();
  void DumpProducerChannelInfo(const ObjectID &channel_id, rapidjson::Value &obj,
                               rapidjson::Document &ret);
  void DumpConsumerChannelInfo(const ObjectID &channel_id, rapidjson::Value &obj,
                               rapidjson::Document &ret);
  void RegisterLogLevelRoute();

 private:
  std::shared_ptr<HttpServer> http_server_;
  boost::asio::io_context ioc_;
  std::unique_ptr<std::thread> thread_;
  int port_;

  DataWriter *writer_;
  DataReader *reader_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_HTTP_PROFILER_H
