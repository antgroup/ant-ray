#include "gtest/gtest.h"
#include "logging.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingLogTest, log_test) {
  StreamingLog::StartStreamingLog("streaming-test", StreamingLogLevel::TRACE, 0,
                                  "/tmp/streaminglogs/");
  STREAMING_LOG(TRACE) << "TRACE";
  STREAMING_LOG(DEBUG) << "DEBUG";
  STREAMING_LOG(INFO) << "INFO";
  STREAMING_LOG(WARNING) << "WARNING";
  STREAMING_LOG(ERROR) << "ERROR";
  STREAM_LOG(INFO, "{}, {}", "Format log", 2);
  StreamingLog::ShutDownStreamingLog();

  StreamingLog::StartStreamingLog("streaming-test", StreamingLogLevel::DEBUG, 0,
                                  "/tmp/streaminglogs/");
  STREAMING_LOG(DEBUG) << "DEBUG2";
  STREAMING_LOG(DEBUG) << "DEBUG3";
  STREAMING_LOG(INFO) << "INFO2";
  STREAMING_LOG(INFO) << "INFO3";
  STREAMING_LOG(WARNING) << "WARNING2";
  STREAMING_LOG(WARNING) << "WARNING3";
  STREAMING_LOG(ERROR) << "ERROR2";

  STREAMING_LOG(DEBUG) << "DEBUG2";
  STREAMING_LOG(DEBUG) << "DEBUG3";
  STREAMING_LOG(INFO) << "INFO2";
  STREAMING_LOG(INFO) << "INFO3";
  STREAMING_LOG(WARNING) << "WARNING2";
  STREAMING_LOG(WARNING) << "WARNING3";
  STREAMING_LOG(ERROR) << "ERROR2";
  StreamingLog::ShutDownStreamingLog();

  StreamingLog::StartStreamingLog("streaming-test", StreamingLogLevel::INFO, 0,
                                  "/tmp/streaminglogs/");
  STREAMING_LOG(DEBUG) << "DEBUG4";
  STREAMING_LOG(DEBUG) << "DEBUG5";
  STREAMING_LOG(INFO) << "INFO4";
  STREAMING_LOG(INFO) << "INFO5";
  STREAMING_LOG(WARNING) << "WARNING4";
  STREAMING_LOG(WARNING) << "WARNING5";
  STREAMING_LOG(ERROR) << "ERROR4";
  StreamingLog::ShutDownStreamingLog();
}

TEST(StreamingLogTest, spd_log) {
  spdlog::set_level(spdlog::level::trace);
  auto file_logger = spdlog::basic_logger_mt("basic_logger", "/tmp/logs/basic.txt");
  spdlog::set_default_logger(file_logger);
  spdlog::info("Welcome to spdlog!");
  spdlog::error("Some error message with arg: {}", 1);

  spdlog::warn("Easy padding in numbers like {:08d}", 12);
  spdlog::critical("Support for int: {0:d};  hex: {0:x};  oct: {0:o}; bin: {0:b}", 42);
  spdlog::info("Support for floats {:03.2f}", 1.23456);
  spdlog::info("Positional args are {1} {0}..", "too", "supported");
  spdlog::info("{:<30}", "left aligned");

  spdlog::set_level(spdlog::level::debug);  // Set global log level to debug
  spdlog::debug("This message should be displayed..");

  // change log pattern
  spdlog::set_pattern("[%H:%M:%S %z] [%n] [%^---%L---%$] [thread %t] %v");

  // Compile time log levels
  // define SPDLOG_ACTIVE_LEVEL to desired level
  SPDLOG_TRACE("Some trace message with param {}", 42);
  SPDLOG_DEBUG("Some debug message");
  auto console = spdlog::stdout_color_mt("console");
  auto err_logger = spdlog::stderr_color_mt("s_stderr");
  spdlog::get("console")->warn("warn log.");
  spdlog::get("console")->info(
      "loggers can be retrieved from a global registry using the "
      "spdlog::get(logger_name)");
  spdlog::get("s_stderr")->info("std err info.");
}

TEST(StreamingLogTest, fmt_macro) {
  StreamingLog::StartStreamingLog(StreamingLogLevel::TRACE);
  STREAM_LOG(INFO, "{} {}", "This is info", 2);
  STREAM_LOG(TRACE, "{} {} {}", "trace", 3, "I'm level of ");
  StreamingLog::ShutDownStreamingLog();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
