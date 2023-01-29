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

#include "ray/common/ray_config.h"

#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <typeinfo>

#include "nlohmann/json.hpp"
#include "ray/util/logging.h"
#include "ray/util/macros.h"

using json = nlohmann::json;

RayConfig &RayConfig::instance() {
  static RayConfig config;
  return config;
}

/// -----------Include ray_config_def.h to set config items.-------------------
/// A helper macro that helps to set a value to a config item.
#define RAY_CONFIG(type, name, default_value) \
  if (pair.key() == #name) {                  \
    std::unique_lock lock(shared_mutex_);     \
    name##_ = pair.value().get<type>();       \
    continue;                                 \
  }

void RayConfig::initialize(const std::string &config_list) {
  if (config_list.empty()) {
    return;
  }
  try {
    // Parse the configuration list.
    json config_map = json::parse(config_list);
    for (const auto &pair : config_map.items()) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
#include "ray/common/ray_config_def.h"
      RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.key();
    }
    if (RAY_LOG_ENABLED(DEBUG)) {
      std::ostringstream oss;
      oss << "RayConfig is initialized with: ";
      for (auto const &pair : config_map.items()) {
        oss << pair.key() << "=" << pair.value() << ",";
      }
      RAY_LOG(DEBUG) << oss.str();
    }
  } catch (json::exception &ex) {
    RAY_LOG(FATAL) << "Failed to initialize RayConfig: " << ex.what()
                   << " The config string is: " << config_list;
  }
}
/// ---------------------------------------------------------------------
#undef RAY_CONFIG

// ANT-INTERNAL
#define RAY_CONFIG(type, name, default_value) \
  if (pair.key() == #name) {                  \
    std::shared_lock lock(shared_mutex_);     \
    RAY_UNUSED(pair.value().get<type>());     \
    continue;                                 \
  }

bool RayConfig::validate(const std::string &config_json_str, std::string *error_message) {
  try {
    // Parse the configuration list.
    json config_map = json::parse(config_json_str);
    for (const auto &pair : config_map.items()) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
#include "ray/common/ray_config_def.h"
      // "ray/common/ray_internal_flag_def.h" is intentionally not included,
      // because it contains Ray internal settings.
      std::stringstream ss;
      ss << "Invalid key: " << pair.key();
      if (error_message != nullptr) {
        *error_message = ss.str();
      }
      RAY_LOG(ERROR) << ss.str();
      return false;
    }
  } catch (json::exception &ex) {
    std::stringstream ss;
    ss << "Failed to validate RayConfig: " << ex.what()
       << " The config string is: " << config_json_str;
    if (error_message != nullptr) {
      *error_message = ss.str();
    }
    RAY_LOG(ERROR) << ss.str();
    return false;
  }
  return true;
}
#undef RAY_CONFIG

bool RayConfig::update(const std::string &config_json_str, std::string *error_message) {
  if (!validate(config_json_str, error_message)) {
    RAY_LOG(ERROR) << "Validate failed, won't update config, config_json_str="
                   << config_json_str;
    return false;
  }
  RAY_LOG(INFO) << "Start to update config, config_json_str=" << config_json_str;
  initialize(config_json_str);
  return true;
}

#define RAY_CONFIG(type, name, default_value) \
  {                                           \
    std::shared_lock lock(shared_mutex_);     \
    json[#name] = name##_;                    \
  }

std::string RayConfig::dump_to_json_str() const {
  json json;
#include "ray/common/ray_config_def.h"
  return json.dump();
}

#undef RAY_CONFIG