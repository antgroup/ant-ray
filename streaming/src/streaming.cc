#include "streaming.h"

#include <atomic>
#include <cstdlib>
#include <mutex>

#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace streaming {

static std::atomic<int32_t> streaming_log_ref_count(0);
static std::mutex streaming_log_mutex;

void set_streaming_log_config(const std::string &app_name,
                              const StreamingLogLevel &log_level,
                              const int &log_buffer_flush_in_secs,
                              const std::string &log_dir) {
  std::unique_lock<std::mutex> lock(streaming_log_mutex);
  if (!streaming_log_ref_count) {
    ray::streaming::StreamingLog::StartStreamingLog(app_name, log_level,
                                                    log_buffer_flush_in_secs, log_dir);
  }
  streaming_log_ref_count++;
  STREAMING_LOG(WARNING) << "streaming log ref => " << streaming_log_ref_count;
}

void streaming_log_shutdown() {
  std::unique_lock<std::mutex> lock(streaming_log_mutex);
  streaming_log_ref_count--;
  STREAMING_LOG(INFO) << "streaming log ref => " << streaming_log_ref_count;
  if (streaming_log_ref_count == 0) {
    STREAMING_LOG(WARNING) << "streaming log shutdown";
    ray::streaming::StreamingLog::ShutDownStreamingLog();
  }
}

}  // namespace streaming
}  // namespace ray
