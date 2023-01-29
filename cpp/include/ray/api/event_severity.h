// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <ray/api/ray_exception.h>
#include <string>

namespace ray {

enum class EventSeverity { INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

namespace internal {

inline std::string GetEventSeverityString(EventSeverity severity) {
  switch (severity) {
  case EventSeverity::INFO:
    return "INFO";
  case EventSeverity::WARNING:
    return "WARNING";
  case EventSeverity::ERROR:
    return "ERROR";
  case EventSeverity::FATAL:
    return "FATAL";
  default:
    throw RayException("Invalid event severity");
  }
}

}  // namespace internal

}  // namespace ray