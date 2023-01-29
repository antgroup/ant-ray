#pragma once
#ifndef RAY_STREAMING_RUNTIME_CONTEXT_H
#define RAY_STREAMING_RUNTIME_CONTEXT_H
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>

#include "channel.h"
#include "common/status.h"
#include "conf/streaming_config.h"
#include "logging.h"
#include "metrics/streaming_perf_metric.h"
#include "ray/common/id.h"
#include "streaming_asio.h"
#include "util/utility.h"
namespace ray {

namespace streaming {

class RuntimeContext {
 public:
  RuntimeContext();
  virtual ~RuntimeContext();
  inline const StreamingConfig &GetConfig() const { return config_; }
  void SetConfig(const StreamingConfig &config);
  void SetConfig(const std::string &config_proto_string);
  void SetConfig(const uint8_t *data, uint32_t size);
  TransferState &GetTransferState();
  void SetTransferState(TransferState transfer_state);

  friend std::ostream &operator<<(std::ostream &os, const RuntimeContext &common);

  void InitMetricsReporter();
  void ReportMetrics(const std::string &metric_name, double value,
                     const std::map<std::string, std::string> &tags = {});
  void EnableTimer(std::function<void()> report_timer_handler);
  void ShutdownTimer();

 private:
  void RunTimer();

 public:
  StreamingConfig config_;
  TransferState transfer_state_;
  StreamingMetricsConfig metrics_config_;

 protected:
  std::unique_ptr<StreamingPerf> perf_metrics_reporter_;
  std::function<void()> report_timer_handler_;

  boost::asio::io_service async_io_;

 private:
  bool is_streaming_log_init_;
  bool enable_timer_service_;

  std::unique_ptr<boost::asio::deadline_timer> metrics_timer_;
  std::shared_ptr<std::thread> timer_thread_;
  std::atomic_flag report_flag_ = ATOMIC_FLAG_INIT;
};

}  // namespace streaming
}  // namespace ray
#endif