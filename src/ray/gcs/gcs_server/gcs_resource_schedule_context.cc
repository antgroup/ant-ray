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

#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"

#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"

namespace ray {
namespace gcs {
std::shared_ptr<ScheduleOptions> ScheduleOptions::FromProto(
    const rpc::ScheduleOptions &proto_options) {
  auto schedule_options = std::make_shared<ScheduleOptions>();
  schedule_options->pack_step_ = proto_options.pack_step();
  schedule_options->rare_resources_schedule_pack_step_ =
      proto_options.rare_resources_schedule_pack_step();
  schedule_options->num_candidate_nodes_for_scheduling_ =
      proto_options.num_candidate_nodes_for_scheduling();
  schedule_options->resource_weights_.insert(proto_options.resource_weights().begin(),
                                             proto_options.resource_weights().end());
  schedule_options->scorer_weights_.insert(proto_options.scorer_weights().begin(),
                                           proto_options.scorer_weights().end());
  schedule_options->runtime_resource_scheduling_enabled_ =
      proto_options.runtime_resource_scheduling_enabled();
  schedule_options->runtime_resources_calculation_interval_s_ =
      proto_options.runtime_resources_calculation_interval_s();
  schedule_options->runtime_memory_tail_percentile_ =
      proto_options.runtime_memory_tail_percentile();
  schedule_options->runtime_cpu_tail_percentile_ =
      proto_options.runtime_cpu_tail_percentile();
  schedule_options->runtime_resources_history_window_len_s_ =
      proto_options.runtime_resources_history_window_len_s();
  schedule_options->runtime_memory_history_window_tail_ =
      proto_options.runtime_memory_history_window_tail();
  schedule_options->runtime_cpu_history_window_tail_ =
      proto_options.runtime_cpu_history_window_tail();
  schedule_options->overcommit_ratio_ = proto_options.overcommit_ratio();
  schedule_options->node_overcommit_ratio_ = proto_options.node_overcommit_ratio();
  schedule_options->job_resource_requirements_max_min_ratio_limit_ =
      proto_options.job_resource_requirements_max_min_ratio_limit();
  return schedule_options;
}

std::shared_ptr<ScheduleOptions> ScheduleOptions::FromMapProto(
    const ::google::protobuf::Map<std::string, std::string> &schedule_option_proto) {
  auto schedule_options = std::make_shared<ScheduleOptions>();
  auto iter = schedule_option_proto.find("pack_step");
  if (iter != schedule_option_proto.end()) {
    schedule_options->pack_step_ = std::stod(iter->second);
  }
  iter = schedule_option_proto.find("rare_resources_schedule_pack_step");
  if (iter != schedule_option_proto.end()) {
    schedule_options->rare_resources_schedule_pack_step_ = std::stod(iter->second);
  }
  iter = schedule_option_proto.find("num_candidate_nodes_for_scheduling");
  if (iter != schedule_option_proto.end()) {
    schedule_options->num_candidate_nodes_for_scheduling_ = std::stoi(iter->second);
  }
  iter = schedule_option_proto.find("resource_weights");
  if (iter != schedule_option_proto.end()) {
    for (const auto &sp : absl::StrSplit(iter->second, '_')) {
      if (!sp.empty()) {
        std::pair<std::string, std::string> kv = absl::StrSplit(sp, ':');
        schedule_options->resource_weights_.emplace(kv.first, atoi(kv.second.c_str()));
      }
    }
  }
  iter = schedule_option_proto.find("scorer_weights");
  if (iter != schedule_option_proto.end()) {
    for (const auto &sp : absl::StrSplit(iter->second, '_')) {
      if (!sp.empty()) {
        std::pair<std::string, std::string> kv = absl::StrSplit(sp, ':');
        schedule_options->scorer_weights_.emplace(kv.first, atoi(kv.second.c_str()));
      }
    }
  }
  iter = schedule_option_proto.find("runtime_resource_scheduling_enabled");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_resource_scheduling_enabled_ = (iter->second == "true");
  }
  iter = schedule_option_proto.find("runtime_resources_calculation_interval_s");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_resources_calculation_interval_s_ =
        std::stoul(iter->second);
  }
  iter = schedule_option_proto.find("runtime_memory_tail_percentile");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_memory_tail_percentile_ = std::stof(iter->second);
  }
  iter = schedule_option_proto.find("runtime_cpu_tail_percentile");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_cpu_tail_percentile_ = std::stof(iter->second);
  }
  iter = schedule_option_proto.find("runtime_resources_history_window_len_s");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_resources_history_window_len_s_ = std::stoul(iter->second);
  }
  iter = schedule_option_proto.find("runtime_memory_history_window_tail");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_memory_history_window_tail_ = std::stof(iter->second);
  }
  iter = schedule_option_proto.find("runtime_cpu_history_window_tail");
  if (iter != schedule_option_proto.end()) {
    schedule_options->runtime_cpu_history_window_tail_ = std::stof(iter->second);
  }
  iter = schedule_option_proto.find("overcommit_ratio");
  if (iter != schedule_option_proto.end()) {
    schedule_options->overcommit_ratio_ = std::stof(iter->second);
  }
  iter = schedule_option_proto.find("node_overcommit_ratio");
  if (iter != schedule_option_proto.end()) {
    schedule_options->node_overcommit_ratio_ = std::stof(iter->second);
  }
  iter = schedule_option_proto.find("job_resource_requirements_max_min_ratio_limit");
  if (iter != schedule_option_proto.end()) {
    schedule_options->job_resource_requirements_max_min_ratio_limit_ =
        std::stof(iter->second);
  }
  return schedule_options;
}

std::unordered_map<std::string, std::string> ScheduleOptions::ToMap() const {
  std::unordered_map<std::string, std::string> schedule_options;
  schedule_options.emplace("pack_step", std::to_string(pack_step_));
  schedule_options.emplace("rare_resources_schedule_pack_step",
                           std::to_string(rare_resources_schedule_pack_step_));
  schedule_options.emplace("num_candidate_nodes_for_scheduling",
                           std::to_string(num_candidate_nodes_for_scheduling_));
  schedule_options.emplace("resource_weights", absl::StrJoin(resource_weights_, "_",
                                                             absl::PairFormatter(":")));
  schedule_options.emplace("scorer_weights",
                           absl::StrJoin(scorer_weights_, "_", absl::PairFormatter(":")));
  schedule_options.emplace("runtime_resource_scheduling_enabled",
                           runtime_resource_scheduling_enabled_ ? "true" : "false");
  schedule_options.emplace("runtime_resources_calculation_interval_s",
                           std::to_string(runtime_resources_calculation_interval_s_));
  schedule_options.emplace("runtime_memory_tail_percentile",
                           std::to_string(runtime_memory_tail_percentile_));
  schedule_options.emplace("runtime_cpu_tail_percentile",
                           std::to_string(runtime_cpu_tail_percentile_));
  schedule_options.emplace("runtime_resources_history_window_len_s",
                           std::to_string(runtime_resources_history_window_len_s_));
  schedule_options.emplace("runtime_memory_history_window_tail",
                           std::to_string(runtime_memory_history_window_tail_));
  schedule_options.emplace("runtime_cpu_history_window_tail",
                           std::to_string(runtime_cpu_history_window_tail_));
  schedule_options.emplace("overcommit_ratio", std::to_string(overcommit_ratio_));
  schedule_options.emplace("node_overcommit_ratio",
                           std::to_string(node_overcommit_ratio_));
  schedule_options.emplace(
      "job_resource_requirements_max_min_ratio_limit",
      std::to_string(job_resource_requirements_max_min_ratio_limit_));

  return schedule_options;
}

std::string ScheduleOptions::ToString() const { return ""; }

bool PGScheduleContext::IsNodeSelected(const NodeID &node) const {
  if (ResourceScheduleContext::IsNodeSelected(node)) {
    return true;
  }

  if (bundle_locations_.has_value()) {
    const auto &bundle_locations = bundle_locations_.value();
    if (bundle_locations) {
      auto iter =
          std::find_if(bundle_locations->begin(), bundle_locations->end(),
                       [&node](const auto &item) { return item.second.first == node; });
      return iter != bundle_locations->end();
    }
  }

  return false;
}

bool PGScheduleContext::HasAnySelectedNode() const {
  if (ResourceScheduleContext::HasAnySelectedNode()) {
    return true;
  }

  if (bundle_locations_.has_value()) {
    const auto &bundle_locations = bundle_locations_.value();
    return bundle_locations && !bundle_locations->empty();
  }

  return false;
}

}  // namespace gcs
}  // namespace ray
