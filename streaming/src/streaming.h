#pragma once
#ifndef RAY_STREAMING_H
#define RAY_STREAMING_H
#include "logging.h"
#include "util/utility.h"

namespace ray {
namespace streaming {

void set_streaming_log_config(
    const std::string &app_name = "streaming",
    const StreamingLogLevel &log_level = StreamingLogLevel::INFO,
    const int &log_buffer_flush_in_secs = 0,
    const std::string &log_dir = "/tmp/streaminglogs/");

void streaming_log_shutdown();

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_H
