#include "glog/logging.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "glog/log_severity.h"
#include "logging.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace ray {
namespace streaming {

/// A logger that prints logs to stdout.
/// This is the default logger if logging is not initialized.
class DefaultStdOutLogger final {
 public:
  std::shared_ptr<spdlog::logger> GetConsoleLogger() { return default_stdout_logger_; }
  static DefaultStdOutLogger &Instance() {
    static DefaultStdOutLogger instance;
    return instance;
  }

 private:
  DefaultStdOutLogger() {
    default_stdout_logger_ = spdlog::stderr_color_mt("console");
    default_stdout_logger_->set_pattern("[%Y-%m-%d %H:%M:%S,%e %^%L%$ %P %t] %v");
  }
  ~DefaultStdOutLogger() = default;
  DefaultStdOutLogger(DefaultStdOutLogger const &) = delete;
  DefaultStdOutLogger(DefaultStdOutLogger &&) = delete;
  std::shared_ptr<spdlog::logger> default_stdout_logger_;
};

class SpdLogMessage final {
 public:
  explicit SpdLogMessage(const char *file, int line, int loglevel) : loglevel_(loglevel) {
    stream() << ConstBasename(file) << ":" << line << ": ";
  }

  void Flush() {
    auto logger = spdlog::get("S");
    // If no default logger we just emit all log informations to console.
    if (!logger) {
      logger = DefaultStdOutLogger::Instance().GetConsoleLogger();
    }
    if (loglevel_ == static_cast<int>(spdlog::level::critical)) {
      stream() << "\n" << ray::GetCallTrace();
    }
    logger->log(static_cast<spdlog::level::level_enum>(loglevel_), "{}", str_.str());
    logger->flush();

    if (loglevel_ == static_cast<int>(spdlog::level::critical)) {
      // Abort after fatal log.
      std::abort();
    }
  }

  ~SpdLogMessage() { Flush(); }
  inline std::ostream &stream() { return str_; }

 private:
  std::ostringstream str_;
  int loglevel_;

  SpdLogMessage(const SpdLogMessage &) = delete;
  SpdLogMessage &operator=(const SpdLogMessage &) = delete;
};

int GetMappedSeverity(StreamingLogLevel severity) {
  switch (severity) {
#ifndef USE_SPD_LOG
  case StreamingLogLevel::TRACE:
  case StreamingLogLevel::DEBUG:
  case StreamingLogLevel::INFO:
    return google::GLOG_INFO;
  case StreamingLogLevel::WARNING:
    return google::GLOG_WARNING;
  case StreamingLogLevel::ERROR:
    return google::GLOG_ERROR;
  case StreamingLogLevel::FATAL:
    return google::GLOG_FATAL;
  default:
    STREAMING_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
    // This return won't be hit but compiler needs it.
    return google::GLOG_FATAL;
#else
  case StreamingLogLevel::TRACE:
    return spdlog::level::trace;
  case StreamingLogLevel::DEBUG:
    return spdlog::level::debug;
  case StreamingLogLevel::INFO:
    return spdlog::level::info;
  case StreamingLogLevel::WARNING:
    return spdlog::level::warn;
  case StreamingLogLevel::ERROR:
    return spdlog::level::err;
  case StreamingLogLevel::FATAL:
    return spdlog::level::critical;
  default:
    STREAMING_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
    // This return won't be hit but compiler needs it.
    return spdlog::level::off;
#endif
  }
}

StreamingLogLevel StreamingLog::severity_threshold_ = StreamingLogLevel::INFO;
StreamingLog::StreamingLog(const char *file_name, int line_number, const char *func_name,
                           StreamingLogLevel severity)
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
  if (is_enabled_) {
    logging_provider_ =
#ifndef USE_SPD_LOG
        new google::LogMessage(file_name, line_number, GetMappedSeverity(severity));
#else
        new SpdLogMessage(file_name, line_number, GetMappedSeverity(severity));

#endif
  }
}

/// Set default logger in console stdout.
void StreamingLog::StartStreamingLog(StreamingLogLevel severity_threshold) {
  ResetLogLevel(severity_threshold);
}

void StreamingLog::ResetLogLevel(StreamingLogLevel level) {
  severity_threshold_ = level;
  // Reset log level inflight
#ifdef USE_SPD_LOG
  spdlog::set_level(static_cast<spdlog::level::level_enum>(GetMappedSeverity(level)));
#endif
}

void StreamingLog::StartStreamingLog(const std::string &app_name,
                                     StreamingLogLevel severity_threshold,
                                     int log_buffer_flush_in_secs,
                                     const std::string &log_dir) {
  severity_threshold_ = severity_threshold;
  if (!log_dir.empty()) {
    auto dir_ends_with_slash = log_dir;
    if (log_dir[log_dir.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
    auto dir_path_cstr = dir_ends_with_slash.c_str();
    if (access(dir_path_cstr, 0) == -1) {
      mkdir(dir_path_cstr, S_IRWXU | S_IRGRP | S_IROTH);
    }
    auto app_name_without_path = app_name;
    if (app_name.empty()) {
      app_name_without_path = "DefaultApp";
    } else {
      // Find the app name without the path.
      size_t pos = app_name.rfind('/');
      if (pos != app_name.npos && pos + 1 < app_name.length()) {
        app_name_without_path = app_name.substr(pos + 1);
      }
    }
#ifndef USE_SPD_LOG
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    int level = GetMappedSeverity(static_cast<StreamingLogLevel>(severity_threshold_));
    google::SetLogDestination(level, dir_ends_with_slash.c_str());
    for (int i = static_cast<int>(StreamingLogLevel::INFO);
         i <= static_cast<int>(StreamingLogLevel::FATAL); ++i) {
      if (i != level) {
        google::SetLogDestination(i, "");
      }
    }
    FLAGS_logbufsecs = log_buffer_flush_in_secs;
    FLAGS_max_log_size = 1000;
    FLAGS_log_rotate_max_size = 10;
    FLAGS_stop_logging_if_full_disk = true;
#else
    // Reset log pattern and level and we assume a log file can be rotated with
    // 10 files in max size 512M.
    // Format pattern is 2020-08-21 17:00:00 I 100 1001 msg.
    // %L is loglevel, %P is process id, %t for thread id.
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S,%e %L %P %t] %v");
    spdlog::set_level(static_cast<spdlog::level::level_enum>(severity_threshold_));
    std::string realtime_logger_name = "S";
    if (spdlog::get(realtime_logger_name)) {
      spdlog::drop(realtime_logger_name);
    }
    auto file_logger =
        spdlog::rotating_logger_mt(realtime_logger_name,
                                   dir_ends_with_slash + app_name_without_path + "_" +
                                       std::to_string(getpid()) + ".log",
                                   1 << 29, 10);
    // Sink all log stuff to default file logger we defined here. We may need
    // multiple sinks for different files or loglevel.
    spdlog::set_default_logger(file_logger);
#endif
  }
}

bool StreamingLog::IsLevelEnabled(StreamingLogLevel level) {
  return level >= severity_threshold_;
}

void StreamingLog::ShutDownStreamingLog() {
#ifndef USE_SPD_LOG
  // google::ShutdownGoogleLogging();
#else
  spdlog::default_logger()->flush();
  // FIXME(lingxuan.zlx): Console logger has been initialized in log message, so
  // it could be double free if we shutdown all loggers here when process exit.
  // spdlog::shutdown();
#endif
}

#ifndef USE_SPD_LOG
void StreamingLog::FlushStreamingLog(int severity) { google::FlushLogFiles(severity); }

void StreamingLog::InstallFailureSignalHandler() {
  google::InstallFailureSignalHandler();
}
#endif

std::ostream &StreamingLog::Stream() {
  auto logging_provider =
#ifndef USE_SPD_LOG
      reinterpret_cast<google::LogMessage *>(logging_provider_);
#else
      reinterpret_cast<SpdLogMessage *>(logging_provider_);
#endif
  return logging_provider->stream();
}

bool StreamingLog::IsEnabled() const { return is_enabled_; }

StreamingLog::~StreamingLog() {
  if (logging_provider_ != nullptr) {
    auto logging_provider =
#ifndef USE_SPD_LOG
        reinterpret_cast<google::LogMessage *>(logging_provider_);
#else
        reinterpret_cast<SpdLogMessage *>(logging_provider_);
#endif
    delete logging_provider;
    logging_provider_ = nullptr;
  }
}
}  // namespace streaming
}  // namespace ray
