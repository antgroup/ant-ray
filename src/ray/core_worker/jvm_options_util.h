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

////////////////////////////////ANT INTERNAL

#pragma once

#include <iostream>
#include <sstream>
#include <string>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/resource_util.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/gcs.pb.h"

#define JLONG_FORMAT_SPECIFIER "%l64d"

#define JLI_StrLen(p1) strlen((p1))
#define JLI_StrCmp(p1, p2) strcmp((p1), (p2))

#define KB (1024ULL)
#define MB (1024ULL * KB)
#define GB (1024ULL * MB)

namespace ray {

/// The following code is copied from OpenJDK/JDK.
inline static bool ParseMemorySize(const std::string &option, int64_t *result) {
  const char *s = option.c_str();
  int64_t n = -1;
  std::stringstream ss(option);
  ss >> n;
  if (n < 0) {
    return false;
  }

  while (*s != '\0' && *s >= '0' && *s <= '9') {
    s++;
  }
  // 4705540: illegal if more characters are found after the first non-digit
  if (JLI_StrLen(s) > 1) {
    return false;
  }
  switch (*s) {
  case 'T':
  case 't':
    *result = n * GB * KB;
    return true;
  case 'G':
  case 'g':
    *result = n * GB;
    return true;
  case 'M':
  case 'm':
    *result = n * MB;
    return true;
  case 'K':
  case 'k':
    *result = n * KB;
    return true;
  case 'B':
  case 'b':
    *result = n;
    return true;
  case '\0':
    *result = n;
    return true;
  default:
    /* Create JVM with default stack and let VM handle malformed -Xss string*/
    return false;
  }
}

inline static std::tuple<bool, std::string> ValidateJvmOptions(
    int64_t maximum_memory_bytes, const std::vector<std::string> &jvm_options) {
  int64_t xmx_bytes = -1;
  int64_t xms_bytes = -1;
  int64_t xmn_bytes = -1;
  int64_t direct_memory_bytes = -1;
  int64_t xss_bytes = -1;

  for (auto &option : jvm_options) {
    if (StartsWithIgnoreCase(option, "-Xmx")) {
      bool ok = ParseMemorySize(option.substr(4), &xmx_bytes);
      if (!ok) {
        return {false, "Failed to parse option " + option};
      }
      continue;
    } else if (StartsWithIgnoreCase(option, "-Xms")) {
      bool ok = ParseMemorySize(option.substr(4), &xms_bytes);
      if (!ok) {
        return {false, "Failed to parse option " + option};
      }
      continue;
    } else if (StartsWithIgnoreCase(option, "-Xmn")) {
      bool ok = ParseMemorySize(option.substr(4), &xmn_bytes);
      if (!ok) {
        return {false, "Failed to parse option " + option};
      }
      continue;
    } else if (StartsWithIgnoreCase(option, "-Xss")) {
      bool ok = ParseMemorySize(option.substr(4), &xss_bytes);
      if (!ok) {
        return {false, "Failed to parse option " + option};
      }
      continue;
    } else if (StartsWithIgnoreCase(option, "-XX:MaxDirectMemorySize=")) {
      bool ok =
          ParseMemorySize(option.substr(std::string("-XX:MaxDirectMemorySize=").size()),
                          &direct_memory_bytes);
      if (!ok) {
        return {false, "Failed to parse option " + option};
      }
      continue;
    }
  }

  /// Validation Rules:
  /// 1. xmx < maximum_memory_mb
  /// 2. xms < maximum_memory_mb, xms < xmx
  /// 3. xmn < maximum_memory_mb, xmn < xmx
  /// 4. xss < maximum_memory_mb
  /// 5. direct_memory < maximum_memory_mb
  if (xmx_bytes != -1) {
    if (xmx_bytes > maximum_memory_bytes) {
      std::stringstream ss;
      ss << "Xmx must be less than maximum_memory. -Xmx = " << xmx_bytes
         << "B, maximum_memory = " << maximum_memory_bytes << "B.";
      return {false, ss.str()};
    }
  }

  if (xms_bytes != -1) {
    if (xms_bytes > maximum_memory_bytes) {
      return {false, "Xms must be less than maximum_memory."};
    }
    if (xmx_bytes == -1) {
      return {false, "Xms was set but xmx was not set."};
    }
    if (xms_bytes > xmx_bytes) {
      return {false, "Xms must be less than Xmx."};
    }
  }

  if (xmn_bytes != -1) {
    if (xmn_bytes > maximum_memory_bytes) {
      return {false, "Xmn must be less than maximum_memory."};
    }
    if (xmn_bytes == -1) {
      return {false, "Xmn was set but xmx was not set."};
    }
    if (xmn_bytes > xmx_bytes) {
      return {false, "Xmn must be less than Xmx."};
    }
  }

  if (xss_bytes != -1) {
    if (xss_bytes > maximum_memory_bytes) {
      return {false, "Xss must be less than maximum_memory."};
    }
  }

  if (direct_memory_bytes != -1) {
    if (direct_memory_bytes > maximum_memory_bytes) {
      return {false, "direct_memory must be less than maximum_memory."};
    }
  }

  return {true, "ok"};
}

Status ValidateJvmOptions(std::unique_ptr<rpc::JobConfig> &job_config,
                          const ActorID &actor_id, double actor_memory_bytes,
                          const std::vector<std::string> &dynamic_options) {
  RAY_CHECK(job_config != nullptr);
  const int64_t maximum_memory_bytes =
      actor_memory_bytes > 0.0000001
          ? static_cast<int64_t>(actor_memory_bytes)
          : FromMemoryUnitsToBytes(static_cast<double>(
                job_config->java_worker_process_default_memory_units()));

  const auto jvm_options_size = static_cast<size_t>(job_config->jvm_options_size());
  /// TODO: Cache this for core worker to avoid unnecessary copies.
  std::vector<std::string> per_jov_jvm_options{jvm_options_size};
  for (size_t i = 0; i < jvm_options_size; ++i) {
    per_jov_jvm_options.push_back(job_config->jvm_options(i));
  }

  {
    /// Validate jvm options from dynamic options.
    const auto result = ValidateJvmOptions(maximum_memory_bytes, dynamic_options);
    const bool valid = std::get<0>(result);
    if (!valid) {
      RAY_LOG(ERROR)
          << "Failed to validate Jvm options from dynamic options when creating actor "
          << actor_id << " due to : " << std::get<1>(result);
      return Status::Invalid(std::get<1>(result));
    }
  }

  if (dynamic_options.empty()) {
    /// Validate jvm options from ray job config.
    const auto result = ValidateJvmOptions(maximum_memory_bytes, per_jov_jvm_options);
    const bool valid = std::get<0>(result);
    if (!valid) {
      RAY_LOG(ERROR)
          << "Failed to validate Jvm options in job config when creating actor "
          << actor_id << " due to : " << std::get<1>(result);
      return Status::Invalid(std::get<1>(result));
    }
  }

  return Status::OK();
}

}  // namespace ray
