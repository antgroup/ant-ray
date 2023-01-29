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

#include "ray/event/event.h"

#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "ray/stats/stats.h"
#include "ray/util/logging.h"

namespace ray {

thread_local std::unique_ptr<RayEventContext> RayEventContext::instance_ = nullptr;

std::unique_ptr<RayEventContext> RayEventContext::first_instance_ = nullptr;

std::atomic<int> RayEventContext::first_instance_started_setting_(0);

std::atomic<bool> RayEventContext::first_instance_finished_setting_(false);

// LogEventReporter::LogEventReporter(rpc::Event_SourceType source_type, std::string
// log_dir)
///
/// LogEventReporter
///
LogEventReporter::LogEventReporter(rpc::Event_SourceType source_type,
                                   const std::string &log_dir, bool force_flush,
                                   int rotate_max_file_size, int rotate_max_file_num)
    : log_dir_(log_dir),
      force_flush_(force_flush),
      rotate_max_file_size_(rotate_max_file_size),
      rotate_max_file_num_(rotate_max_file_num) {
  RAY_CHECK(log_dir_ != "");
  if (log_dir_.back() != '/') {
    log_dir_ += '/';
  }

  // generate file name, if the soucrce type is RAYLET or GCS, the file name would like
  // event_GCS.log, event_RAYLET.log other condition would like
  // event_CORE_WOREKER_{pid}.log
  file_name_ = "event_" + Event_SourceType_Name(source_type);
  if (source_type == rpc::Event_SourceType::Event_SourceType_CORE_WORKER ||
      source_type == rpc::Event_SourceType::Event_SourceType_COMMON) {
    file_name_ += "_" + std::to_string(getpid());
  }
  file_name_ += ".log";

  std::string log_sink_key = GetReporterKey() + log_dir_ + file_name_;
  log_sink_ = spdlog::get(log_sink_key);
  // If the file size is over {rotate_max_file_size_} MB, this file would be renamed
  // for example event_GCS.0.log, event_GCS.1.log, event_GCS.2.log ...
  // We alow to rotate for {rotate_max_file_num_} times.
  if (log_sink_ == nullptr) {
    log_sink_ =
        spdlog::rotating_logger_mt(log_sink_key, log_dir_ + file_name_,
                                   1048576 * rotate_max_file_size_, rotate_max_file_num_);
  }
  log_sink_->set_pattern("%v");
}

std::string LogEventReporter::replaceLineFeed(std::string message) {
  std::stringstream ss;
  // If the message has \n or \r, we will replace with \\n
  for (int i = 0, len = message.size(); i < len; ++i) {
    if (message[i] == '\n' || message[i] == '\r') {
      ss << "\\n";
    } else {
      ss << message[i];
    }
  }
  return ss.str();
}

void rewriteJson(boost::property_tree::ptree &pt) {
  auto child = pt.get_child("custom_fields");
  for (auto kv : child) {
    if (kv.first == "job_id") {
      pt.put("job_id", kv.second.data());
    } else if (kv.first == "job_name") {
      pt.put("job_name", kv.second.data());
    } else if (kv.first == "node_id") {
      pt.put("node_id", kv.second.data());
    } else if (kv.first == "task_id") {
      pt.put("task_id", kv.second.data());
    }
  }
}

LogEventReporter::~LogEventReporter() { Flush(); }

void LogEventReporter::Flush() { log_sink_->flush(); }

std::string LogEventReporter::EventToString(const rpc::Event &event) {
  boost::property_tree::ptree pt;

  auto time_stamp = event.timestamp();
  time_t epoch_time_as_time_t = time_stamp / 1000000;

  absl::Time absl_time = absl::FromTimeT(epoch_time_as_time_t);
  std::stringstream time_stamp_buffer;
  time_stamp_buffer << absl::FormatTime("%Y-%m-%d %H:%M:%S.", absl_time,
                                        absl::LocalTimeZone())
                    << std::setw(6) << std::setfill('0') << time_stamp % 1000000;
  pt.put("time_stamp", time_stamp_buffer.str());
  pt.put("severity", Event_Severity_Name(event.severity()));
  pt.put("label", event.label());
  pt.put("event_id", event.event_id());
  pt.put("source_type", Event_SourceType_Name(event.source_type()));
  pt.put("host_name", event.source_hostname());
  pt.put("pid", std::to_string(event.source_pid()));
  pt.put("message", replaceLineFeed(event.message()));

  boost::property_tree::ptree pt_child;
  for (auto &ele : event.custom_fields()) {
    pt_child.put(ele.first, ele.second);
  }

  pt.add_child("custom_fields", pt_child);

  rewriteJson(pt);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt, false);
  return ss.str();
}

void LogEventReporter::Report(const rpc::Event &event) {
  RAY_CHECK(Event_SourceType_IsValid(event.source_type()));
  RAY_CHECK(Event_Severity_IsValid(event.severity()));
  std::string result = EventToString(event);
  // Pop the last character from the result string because it is breakline '\n'.
  result.pop_back();

  log_sink_->info(result);
  if (force_flush_) {
    Flush();
  }
}

///
/// EventManager
///
EventManager::EventManager() { RayLog::SetEventCallback(RayEvent::ReportEvent); }

EventManager &EventManager::Instance() {
  static EventManager instance_;
  return instance_;
}

bool EventManager::IsEmpty() { return reporter_map_.empty(); }

void EventManager::Publish(const rpc::Event &event) {
  const ray::stats::TagsType event_tag = {
      {ray::stats::EventLabelKey, event.label()},
      {ray::stats::EventSourceTypeKey, Event_SourceType_Name(event.source_type())},
      {ray::stats::EventSeverityTypeKey, Event_Severity_Name(event.severity())}};
  ray::stats::EventCount().Record(1, event_tag);
  for (const auto &element : reporter_map_) {
    (element.second)->Report(event);
  }
}

void EventManager::AddReporter(std::shared_ptr<BaseEventReporter> reporter) {
  reporter_map_.emplace(reporter->GetReporterKey(), reporter);
}

void EventManager::ClearReporters() {
  RAY_LOG(INFO) << "Prepare to clear reporters, reporter size: " << reporter_map_.size();
  reporter_map_.clear();
}

RayEventContext &RayEventContext::Instance() {
  if (instance_ == nullptr) {
    instance_ = std::unique_ptr<RayEventContext>(new RayEventContext());
  }
  return *instance_;
}

RayEventContext &RayEventContext::OriginContext() {
  if (first_instance_finished_setting_ == false) {
    static RayEventContext tmp_instance_;
    return tmp_instance_;
  }
  return *first_instance_;
}

void RayEventContext::SetEventContext(
    rpc::Event_SourceType source_type,
    const std::unordered_map<std::string, std::string> &custom_fields) {
  source_type_ = source_type;
  custom_fields_ = custom_fields;

  initialized_ = true;
  if (!first_instance_started_setting_.fetch_or(1)) {
    first_instance_ = std::unique_ptr<RayEventContext>(new RayEventContext());
    first_instance_->SetSourceType(source_type);
    first_instance_->SetCustomFields(custom_fields);
    for (auto callback : event_post_processors_) {
      first_instance_->SetEventPostProcessor(callback);
    }
    first_instance_finished_setting_ = true;
  }
}

void RayEventContext::ResetEventContext() {
  source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  custom_fields_.clear();
  initialized_ = false;
  first_instance_started_setting_ = 0;
  first_instance_finished_setting_ = false;
}

void RayEventContext::SetCustomField(const std::string &key, const std::string &value) {
  custom_fields_[key] = value;
}

void RayEventContext::SetCustomFields(
    const std::unordered_map<std::string, std::string> &custom_fields) {
  custom_fields_ = custom_fields;
}

void RayEventContext::SetEventPostProcessor(const EventPostProcessor callback) {
  event_post_processors_.emplace_back(callback);
}

// ANT-INTERNAL
void RayEventContext::SetLabelBlacklist(std::string blacklist) {
  // Blacklist like "PIPELINE,PROCESS_EXIT".
  std::vector<std::string> split_blacklist =
      absl::StrSplit(blacklist, ',', absl::SkipEmpty());
  label_blacklist_.insert(split_blacklist.begin(), split_blacklist.end());
}

///
/// RayEvent
///
void RayEvent::ReportEvent(const std::string &severity, const std::string &label,
                           const std::string &message) {
  rpc::Event_Severity severity_ele =
      rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_Severity_Parse(severity, &severity_ele));
  RayEvent(severity_ele, label) << message;
}

RayEvent::~RayEvent() { SendMessage(osstream_.str()); }

void RayEvent::SendMessage(const std::string &message) {
  RAY_CHECK(rpc::Event_SourceType_IsValid(RayEventContext::Instance().GetSourceType()));
  RAY_CHECK(rpc::Event_Severity_IsValid(severity_));

  if (EventManager::Instance().IsEmpty()) {
    return;
  }

  const RayEventContext &context = RayEventContext::Instance().GetInitialzed()
                                       ? RayEventContext::Instance()
                                       : RayEventContext::OriginContext();

  // ANT-INTERNAL
  auto label_blacklist = context.GetLabelBlacklist();
  if (label_blacklist.count(label_) > 0) {
    RAY_LOG(DEBUG) << "Skip event in blacklist, the event label is " << label_;
    return;
  }

  rpc::Event event;

  std::string event_id_buffer = std::string(18, ' ');
  FillRandom(&event_id_buffer);
  event.set_event_id(StringToHex(event_id_buffer));

  event.set_source_type(context.GetSourceType());
  event.set_source_hostname(context.GetSourceHostname());
  event.set_source_pid(context.GetSourcePid());

  event.set_severity(severity_);
  event.set_label(label_);
  event.set_timestamp(current_sys_time_us());
  auto mp = context.GetCustomFields();
  event.mutable_custom_fields()->insert(mp.begin(), mp.end());
  event.mutable_custom_fields()->insert(custom_fields_.begin(), custom_fields_.end());
  for (auto callback : context.GetEventPostProcessors()) {
    callback(event);
  }

  if (event.severity() == rpc::Event_Severity::Event_Severity_FATAL) {
    std::stringstream stack_message;
    stack_message << message;
    std::string stacktrace = google::GetStackTraceToString();
    if (stacktrace != "") {
      stack_message << "\n******Stack Information******\n";
      stack_message << stacktrace;
    }
    event.set_message(stack_message.str());
  } else {
    event.set_message(message);
  }
  EventManager::Instance().Publish(event);
}

}  // namespace ray
