#ifndef RAY_STREAMING_LOGGING_H
#define RAY_STREAMING_LOGGING_H
#include "ray/util/logging.h"

#include "spdlog/spdlog.h"

namespace ray {
namespace streaming {

class StreamingLog;

enum class StreamingLogLevel {
  TRACE = -2,
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

int GetMappedSeverity(StreamingLogLevel severity);

/// NOTE(lingxuan.zlx): we reuse glog const_basename function from its utils.
inline const char *ConstBasename(const char *filepath) {
  const char *base = strrchr(filepath, '/');
#ifdef OS_WINDOWS  // Look for either path separator in Windows
  if (!base) base = strrchr(filepath, '\\');
#endif
  return base ? (base + 1) : filepath;
}

#define STREAMING_LOG_INTERNAL(level) \
  ::ray::streaming::StreamingLog(__FILE__, __LINE__, __FUNCTION__, level)

#define STREAMING_LOG(level)                           \
  if (::ray::streaming::StreamingLog::IsLevelEnabled(  \
          ::ray::streaming::StreamingLogLevel::level)) \
  STREAMING_LOG_INTERNAL(::ray::streaming::StreamingLogLevel::level)

/// Format stream log in efficient style.
#define STREAM_LOG(LEVEL, FMT, ...)                                       \
  spdlog::default_logger()->log(                                          \
      static_cast<spdlog::level::level_enum>(                             \
          GetMappedSeverity(::ray::streaming::StreamingLogLevel::LEVEL)), \
      "{}:{} " FMT, ConstBasename(__FILE__), __LINE__, __VA_ARGS__);

#define STREAMING_IGNORE_EXPR(expr) ((void)(expr))

#define STREAMING_CHECK(condition)                                                 \
  (condition) ? STREAMING_IGNORE_EXPR(0)                                           \
              : ::ray::Voidify() & ::ray::streaming::StreamingLog(                 \
                                       __FILE__, __LINE__, __FUNCTION__,           \
                                       ::ray::streaming::StreamingLogLevel::FATAL) \
                                       << " Check failed: " #condition " "
// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define STREAMING_CHECK_OK_PREPEND(to_call, msg)                                         \
  do {                                                                                   \
    ::ray::streaming::StreamingStatus _s = (to_call);                                    \
    STREAMING_CHECK(_s == ::ray::streaming::StreamingStatus::OK) << (msg) << ": " << _s; \
  } while (0)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define STREAMING_CHECK_OK(s) STREAMING_CHECK_OK_PREPEND(s, "Bad status")
#ifdef NDEBUG

#define STREAMING_DCHECK(condition) \
  STREAMING_IGNORE_EXPR(condition); \
  while (false) ::ray::RayLogBase()

#else
#define RAY_DCHECK(condition) RAY_CHECK(condition)
#endif

class StreamingLog : public RayLogBase {
 public:
  StreamingLog(const char *file_name, int line_number, const char *func_name,
               StreamingLogLevel severity);

  ~StreamingLog();

  /// Start a default logger in stdout.
  static void StartStreamingLog(StreamingLogLevel severity_threshold);

  // Start a default logger in destination directory files.
  static void StartStreamingLog(const std::string &app_name,
                                StreamingLogLevel severity_threshold,
                                int log_buffer_flush_in_secs, const std::string &log_dir);

  static bool IsLevelEnabled(StreamingLogLevel level);

  /// Reset log level inflight or triggering manually.
  static void ResetLogLevel(StreamingLogLevel level);

  static void ShutDownStreamingLog();

#ifndef USE_SPD_LOG
  static void FlushStreamingLog(int severity);

  static void InstallFailureSignalHandler();
#endif

  bool IsEnabled() const;

 private:
  void *logging_provider_;
  bool is_enabled_;
  static StreamingLogLevel severity_threshold_;

 protected:
  virtual std::ostream &Stream();
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_LOGGING_H
