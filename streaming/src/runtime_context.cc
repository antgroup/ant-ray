
#include "runtime_context.h"

#include <atomic>
#include <cstdlib>
#include <mutex>

#include "protobuf/streaming.pb.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "streaming.h"
namespace ray {
namespace streaming {

void RuntimeContext::SetConfig(const StreamingConfig &streaming_config) {
  STREAMING_CHECK(transfer_state_.IsInit()) << "Set config must be at beginning";
  config_ = streaming_config;
}

void RuntimeContext::SetConfig(const uint8_t *data, uint32_t size) {
  std::string proto_string(reinterpret_cast<const char *>(data), size);
  SetConfig(proto_string);
}

void RuntimeContext::SetConfig(const std::string &conf_proto_string) {
  STREAMING_CHECK(transfer_state_.IsInit()) << "Set config must be at beginning";
  config_.FromProto(conf_proto_string);

  is_streaming_log_init_ = true;
  std::string log_path = config_.GetStreamingLogPath();
  // Env parameter first for streaming lob directory.
  auto log_path_ch = std::getenv("STREAMING_LOG_DIR");
  if (log_path_ch) {
    log_path = std::string(log_path_ch);
  }
  set_streaming_log_config(
      config_.GetStreamingJobName() + "_" + config_.GetStreamingOpName() + "_" +
          config_.GetStreamingWorkerName(),
      static_cast<StreamingLogLevel>(config_.GetStreamingLogLevel()), 0, log_path);
  if (!config_.GetStreamingTaskJobId().empty() &&
      config_.GetStreamingTaskJobId().size() == 2 * JobID::Size()) {
    ray::JobID task_job_id = ray::JobID::FromBinary(
        StreamingUtility::Hexqid2str(config_.GetStreamingTaskJobId()));
    STREAMING_LOG(INFO) << "str = > " << task_job_id << ", hex " << task_job_id.Hex();
  }

  auto disable_event_driven_ch = std::getenv("DISABLE_EVENT_DRIVER");
  if (disable_event_driven_ch) {
    config_.SetTransferEventDriven(false);
  }
  auto network_buffer_timeout_ms = std::getenv("NETWORK_BUFFER_TIMEOUT_MS");
  if (network_buffer_timeout_ms) {
    unsigned long ul = std::stoul(std::string(network_buffer_timeout_ms), nullptr, 10);
    config_.SetNetworkBufferTimeoutMs(ul);
  }
  auto network_buffer_sleep_ms = std::getenv("NETWORK_BUFFER_SLEEP_MS");
  if (network_buffer_sleep_ms) {
    unsigned long ul = std::stoul(std::string(network_buffer_sleep_ms), nullptr, 10);
    config_.SetNetworkBufferSleepMs(ul);
  }
  auto buffer_threshold = std::getenv("STREAMING_BUFFER_THREHOLD");
  if (buffer_threshold) {
    unsigned long ul = std::stoul(std::string(buffer_threshold), nullptr, 10);
    config_.SetStreamingRingBufferThreshold(ul);
  }
  auto bundle_consumed_step = std::getenv("BUNDLE_CONSUMED_STEP");
  if (bundle_consumed_step) {
    unsigned long ul = std::stoul(std::string(bundle_consumed_step), nullptr, 10);
    config_.SetStreamingBundleConsumedStep(ul);
  }

  // NOTE(lingxuan.zlx): disable collocate by default and this function can be opened
  // when export STREAMING_QUEUE_DISABLE_COLLOCATE=false
  config_.SetCollocateEnable(false);
  auto disable_collocate = std::getenv("STREAMING_QUEUE_DISABLE_COLLOCATE");
  if (disable_collocate && std::string(disable_collocate) == std::string("false")) {
    config_.SetCollocateEnable(true);
  }
}

RuntimeContext::~RuntimeContext() {
  STREAMING_LOG(INFO) << "Stop timer async io service.";
  if (!async_io_.stopped()) {
    async_io_.stop();
  }
  // To make sure all daemon threads can exit.
  if (!transfer_state_.IsInterrupted()) {
    transfer_state_.SetInterrupted();
  }
  if (is_streaming_log_init_) {
    streaming_log_shutdown();
  }
}

void RuntimeContext::InitMetricsReporter() {
  STREAMING_LOG(INFO) << "init metrics";
  if (!config_.IsStreamingMetricsEnable()) {
    STREAMING_LOG(WARNING) << "metrics is disable";
    return;
  }
  perf_metrics_reporter_.reset(new StreamingPerf());

  std::unordered_map<std::string, std::string> default_tag_map = {
      {"jobname", config_.GetStreamingJobName()},
      {"role", NodeType_Name(config_.GetStreamingRole())},
      {"hostname", StreamingUtility::GetHostname()},
      {"op_name", config_.GetStreamingOpName()},
      // To avoid opentsdb query error if there are too many tags in metric.
      /*{"pid", std::to_string(getpid())},*/
      {"worker_name", config_.GetStreamingWorkerName()}};
  metrics_config_.SetMetricsGlobalTags(default_tag_map);

  perf_metrics_reporter_->Start(metrics_config_);
}

void RuntimeContext::ReportMetrics(const std::string &metric_name, double value,
                                   const std::map<std::string, std::string> &tags) {
  if (config_.IsStreamingMetricsEnable()) {
    perf_metrics_reporter_->UpdateGauge(config_.GetStreamingJobName() + "." + metric_name,
                                        tags, value);
  }
}

void RuntimeContext::RunTimer() {
  AutoSpinLock lock(report_flag_);
  if (!transfer_state_.IsRunning()) {
    STREAMING_LOG(WARNING) << "runtimer failed in state " << transfer_state_;
    return;
  }
  STREAMING_LOG(INFO) << "streaming metric timer called, interval="
                      << metrics_config_.GetMetricsReportInterval();
  if (async_io_.stopped()) {
    STREAMING_LOG(INFO) << "Async io stopped, return from timer reporting.";
    return;
  }
  this->report_timer_handler_();
  boost::posix_time::seconds interval(metrics_config_.GetMetricsReportInterval());
  metrics_timer_->expires_from_now(interval);
  metrics_timer_->async_wait([this](const boost::system::error_code &e) {
    if (boost::asio::error::operation_aborted == e) {
      return;
    }
    this->RunTimer();
  });
}

void RuntimeContext::EnableTimer(std::function<void()> report_timer_handler) {
  if (!config_.IsStreamingMetricsEnable()) {
    STREAMING_LOG(WARNING) << "streaming metrics disabled";
    return;
  }
  if (enable_timer_service_) {
    STREAMING_LOG(INFO) << "timer service already enabled";
    return;
  }
  this->report_timer_handler_ = report_timer_handler;
  STREAMING_LOG(INFO) << "streaming metric timer enabled";
  // We new a thread for timer if timer is not alive currently.
  if (!timer_thread_) {
    async_io_.reset();
    boost::posix_time::seconds interval(metrics_config_.GetMetricsReportInterval());
    metrics_timer_.reset(new boost::asio::deadline_timer(async_io_, interval));
    metrics_timer_->async_wait(
        [this](const boost::system::error_code & /*e*/) { this->RunTimer(); });
    timer_thread_ = std::make_shared<std::thread>([this]() {
      STREAMING_LOG(INFO) << "Async io running.";
      async_io_.run();
    });
    STREAMING_LOG(INFO) << "new thread " << timer_thread_->get_id();
  }
  enable_timer_service_ = true;
}

void RuntimeContext::ShutdownTimer() {
  {
    AutoSpinLock lock(report_flag_);
    if (!config_.IsStreamingMetricsEnable()) {
      STREAMING_LOG(WARNING) << "streaming metrics disabled";
      return;
    }
    if (!enable_timer_service_) {
      STREAMING_LOG(INFO) << "timer service already disabled";
      return;
    }
    STREAMING_LOG(INFO) << "timer server shutdown";
    enable_timer_service_ = false;
    STREAMING_LOG(INFO) << "Cancel metrics timer.";
    metrics_timer_->cancel();
  }
  STREAMING_LOG(INFO) << "Wake up all reporting conditions.";
  if (timer_thread_) {
    STREAMING_LOG(INFO) << "Join and reset timer thread.";
    timer_thread_->join();
    timer_thread_.reset();
    metrics_timer_.reset();
  }
}

TransferState &RuntimeContext::GetTransferState() { return transfer_state_; }

RuntimeContext::RuntimeContext()
    : transfer_state_(TransferState::Init),
      is_streaming_log_init_(false),
      enable_timer_service_(false) {}
}  // namespace streaming
}  // namespace ray