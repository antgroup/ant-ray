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

#include <boost/filesystem.hpp>
#include <csignal>
#include <fstream>
#include <set>
#include <thread>

#include "gtest/gtest.h"
#include "ray/event/event_test_util.h"

namespace ray {

class TestEventReporter : public BaseEventReporter {
 public:
  static std::vector<rpc::Event> event_list;
  virtual void Init() override {}
  virtual void Report(const rpc::Event &event) override { event_list.push_back(event); }
  virtual void Close() override {}
  virtual ~TestEventReporter() {}

  virtual std::string GetReporterKey() override { return "test.event.reporter"; }
};

std::vector<rpc::Event> TestEventReporter::event_list = std::vector<rpc::Event>();

rpc::Event GetEventFromString(std::string seq) {
  std::stringstream ss;
  ss << seq << '\n';
  rpc::Event event;
  boost::property_tree::ptree pt;
  boost::property_tree::read_json(ss, pt);

  std::vector<std::string> splitArray;
  for (auto it = pt.begin(); it != pt.end(); ++it) {
    splitArray.push_back(it->first);
    splitArray.push_back(it->second.get_value<std::string>());
  }

  auto pt_custom_fields = pt.get_child_optional("custom_fields");
  for (auto it = pt_custom_fields->begin(); it != pt_custom_fields->end(); ++it) {
    splitArray.push_back(it->first);
    splitArray.push_back(it->second.get_value<std::string>());
  }
  EXPECT_EQ(splitArray[2], "severity");
  rpc::Event_Severity severity_ele =
      rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_Severity_Parse(splitArray[3], &severity_ele));
  event.set_severity(severity_ele);
  EXPECT_EQ(splitArray[4], "label");
  event.set_label(splitArray[5]);
  EXPECT_EQ(splitArray[6], "event_id");
  event.set_event_id(splitArray[7]);
  EXPECT_EQ(splitArray[8], "source_type");
  rpc::Event_SourceType source_type_ele = rpc::Event_SourceType::
      Event_SourceType_Event_SourceType_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_SourceType_Parse(splitArray[9], &source_type_ele));
  event.set_source_type(source_type_ele);
  EXPECT_EQ(splitArray[10], "host_name");
  event.set_source_hostname(splitArray[11]);
  EXPECT_EQ(splitArray[12], "pid");
  event.set_source_pid(std::stoi(splitArray[13].c_str()));
  EXPECT_EQ(splitArray[14], "message");
  event.set_message(splitArray[15]);
  EXPECT_EQ(splitArray[16], "custom_fields");
  std::unordered_map<std::string, std::string> custom_fields;
  for (int i = 18, len = splitArray.size(); i < len; i += 2) {
    custom_fields[splitArray[i]] = splitArray[i + 1];
  }
  event.mutable_custom_fields()->insert(custom_fields.begin(), custom_fields.end());
  return event;
}

std::vector<std::string> ReadEventFromFile(std::string log_file) {
  std::string line;
  std::ifstream read_file;
  read_file.open(log_file, std::ios::binary);
  std::vector<std::string> vc;
  while (std::getline(read_file, line)) {
    vc.push_back(line);
  }
  read_file.close();
  return vc;
}

std::string GenerateLogDir() {
  std::string log_dir_generate = std::string(5, ' ');
  FillRandom(&log_dir_generate);
  std::string log_dir = "event" + StringToHex(log_dir_generate);
  return log_dir;
}

TEST(EVENT_TEST, TEST_BASIC) {
  ray::RayEventContext::Instance().ResetEventContext();
  TestEventReporter::event_list.clear();
  ray::EventManager::Instance().ClearReporters();

  RAY_EVENT(WARNING, "label") << "test for empty reporters";

  // If there are no reporters, it would not Publish event
  EXPECT_EQ(TestEventReporter::event_list.size(), 0);

  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  RAY_EVENT(WARNING, "label 0") << "send message 0";

  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER, custom_fields);

  RAY_EVENT(INFO, "label 1") << "send message 1";

  custom_fields.clear();
  custom_fields.emplace("node_id", "node 2");
  custom_fields.emplace("job_id", "job 2");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields);
  RAY_EVENT(ERROR, "label 2") << "send message 2 "
                              << "send message again";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_GCS);
  RAY_EVENT(WARNING, "") << "";

  custom_fields.clear();
  custom_fields.emplace("node_id", "node 4");
  custom_fields.emplace("job_id", "job 4");
  custom_fields.emplace("task_id", "task 4");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_COMMON, custom_fields);
  RAY_EVENT(WARNING, "label 4") << "send message 4\n"
                                << "send message 5\n";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 5);

  CheckEventDetail(result[0], "", "", "", "COMMON", "WARNING", "label 0",
                   "send message 0");

  CheckEventDetail(result[1], "job 1", "node 1", "task 1", "CORE_WORKER", "INFO",
                   "label 1", "send message 1");
  CheckEventDetail(result[2], "job 2", "node 2", "", "RAYLET", "ERROR", "label 2",
                   "send message 2 send message again");
  CheckEventDetail(result[3], "", "", "", "GCS", "WARNING", "", "");
  CheckEventDetail(result[4], "job 4", "node 4", "task 4", "COMMON", "WARNING", "label 4",
                   "send message 4\nsend message 5\n");
}

TEST(EVENT_TEST, TEST_FATAL_PRINT_STACK) {
  ray::EventManager::Instance().ClearReporters();
  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  TestEventReporter::event_list.clear();
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER, custom_fields);
  RAY_EVENT(FATAL, "label 0") << "send message 0";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 1);

  CheckEventDetail(result[0], "job 1", "node 1", "task 1", "CORE_WORKER", "FATAL",
                   "label 0", "NULL");
  EXPECT_TRUE(StringContains(result[0].message(), "send message 0"));
  EXPECT_TRUE(StringContains(result[0].message(), "******Stack Information******"));
  EXPECT_TRUE(StringContains(result[0].message(), "ray::RayEvent::SendMessage"));
  EXPECT_TRUE(StringContains(result[0].message(), "ray::RayEvent::~RayEvent()"));
}

TEST(EVENT_TEST, TEST_RAY_CHECK_ABORT) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields);
  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));

  RAY_CHECK(1 > 0) << "correct test case";

  ASSERT_DEATH({ RAY_CHECK(1 < 0) << "incorrect test case"; }, "");

  std::vector<std::string> vc = ReadEventFromFile(log_dir + "/event_RAYLET.log");

  rpc::Event ele_1 = GetEventFromString(vc.back());

  CheckEventDetail(ele_1, "job 1", "node 1", "task 1", "RAYLET", "FATAL",
                   EVENT_LABEL_RAY_CHECK_FAILED, "NULL");
  EXPECT_TRUE(StringContains(ele_1.message(), "Check failed: 1 < 0 incorrect test case"));
  EXPECT_TRUE(StringContains(ele_1.message(), "******Stack Information******"));
  EXPECT_TRUE(StringContains(ele_1.message(), "ray::RayEvent::SendMessage"));
  EXPECT_TRUE(StringContains(ele_1.message(), "ray::RayEvent::~RayEvent()"));

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, TEST_SIGNAL_HANDLER) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields);
  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));

  RayLog::InstallFailureSignalHandler();

  ASSERT_DEATH({ std::raise(SIGTERM); }, "");

  std::vector<std::string> vc = ReadEventFromFile(log_dir + "/event_RAYLET.log");

  // Ensure all the lines of stack merged into one event.
  EXPECT_EQ(vc.size(), 1);

  rpc::Event ele_1 = GetEventFromString(vc.back());
  CheckEventDetail(ele_1, "job 1", "node 1", "task 1", "RAYLET", "FATAL",
                   EVENT_LABEL_SIGNAL_ACTION_FAILED, "NULL");
  EXPECT_TRUE(StringContains(ele_1.message(), "******Stack Information******"));
  EXPECT_TRUE(StringContains(ele_1.message(), "ray::RayEvent::SendMessage"));
  EXPECT_TRUE(StringContains(ele_1.message(), "ray::RayEvent::~RayEvent()"));

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, LOG_ONE_THREAD) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields);
  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));
  int print_times = 1000;
  for (int i = 1; i <= print_times; ++i) {
    RAY_EVENT(INFO, "label " + std::to_string(i)) << "send message " + std::to_string(i);
  }

  std::vector<std::string> vc = ReadEventFromFile(log_dir + "/event_RAYLET.log");

  for (int i = 0, len = vc.size(); i < print_times; ++i) {
    rpc::Event ele = GetEventFromString(vc[len - print_times + i]);
    CheckEventDetail(ele, "job 1", "node 1", "task 1", "RAYLET", "INFO",
                     "label " + std::to_string(i + 1),
                     "send message " + std::to_string(i + 1));
  }

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, MULTI_THREAD_CONTEXT_COPY) {
  ray::RayEventContext::Instance().ResetEventContext();
  TestEventReporter::event_list.clear();
  ray::EventManager::Instance().ClearReporters();
  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  RAY_EVENT(INFO, "label 0") << "send message 0";

  std::thread private_thread = std::thread(std::bind([&]() {
    auto custom_fields = std::unordered_map<std::string, std::string>();
    custom_fields.emplace("node_id", "node 1");
    custom_fields.emplace("job_id", "job 1");
    custom_fields.emplace("task_id", "task 1");
    ray::RayEventContext::Instance().SetEventContext(
        rpc::Event_SourceType::Event_SourceType_GCS, custom_fields);
    RAY_EVENT(INFO, "label 2") << "send message 2";
  }));

  private_thread.join();

  RAY_EVENT(INFO, "label 1") << "send message 1";
  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 3);
  CheckEventDetail(result[0], "", "", "", "COMMON", "INFO", "label 0", "send message 0");
  CheckEventDetail(result[1], "job 1", "node 1", "task 1", "GCS", "INFO", "label 2",
                   "send message 2");
  CheckEventDetail(result[2], "job 1", "node 1", "task 1", "GCS", "INFO", "label 1",
                   "send message 1");

  ray::RayEventContext::Instance().ResetEventContext();
  TestEventReporter::event_list.clear();

  std::thread private_thread_2 = std::thread(std::bind([&]() {
    ray::RayEventContext::Instance().SetCustomField("job_id", "job 1");
    ray::RayEventContext::Instance().SetSourceType(
        rpc::Event_SourceType::Event_SourceType_RAYLET);
    RAY_EVENT(INFO, "label 2") << "send message 2";
  }));

  private_thread_2.join();

  RAY_EVENT(INFO, "label 3") << "send message 3";

  EXPECT_EQ(result.size(), 2);
  CheckEventDetail(result[0], "job 1", "", "", "RAYLET", "INFO", "label 2",
                   "send message 2");
  CheckEventDetail(result[1], "", "", "", "COMMON", "INFO", "label 3", "send message 3");
}

TEST(EVENT_TEST, LOG_MULTI_THREAD) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 2");
  custom_fields.emplace("job_id", "job 2");
  custom_fields.emplace("task_id", "task 2");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_GCS, custom_fields);
  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_GCS, log_dir));
  int nthreads = 80;
  int print_times = 1000;

  PARALLEL_FOR(nthreads, print_times, ({
                 auto custom_fields = std::unordered_map<std::string, std::string>();
                 custom_fields.emplace("node_id", "node 2");
                 custom_fields.emplace("job_id", "job 2");
                 custom_fields.emplace("task_id", "task 2");
                 ray::RayEventContext::Instance().SetEventContext(
                     rpc::Event_SourceType::Event_SourceType_GCS, custom_fields);
               }),
               {
                 RAY_EVENT(WARNING, "label " + std::to_string(loop_i))
                     << "send message " + std::to_string(loop_i);
               });

  std::vector<std::string> vc = ReadEventFromFile(log_dir + "/event_GCS.log");

  std::set<std::string> label_set;
  std::set<std::string> message_set;

  for (int i = 0, len = vc.size(); i < print_times; ++i) {
    rpc::Event ele = GetEventFromString(vc[len - print_times + i]);
    CheckEventDetail(ele, "job 2", "node 2", "task 2", "GCS", "WARNING", "NULL", "NULL");
    message_set.insert(ele.message());
    label_set.insert(ele.label());
  }

  EXPECT_EQ(message_set.size(), print_times);
  EXPECT_EQ(*(message_set.begin()), "send message 0");
  EXPECT_EQ(*(--message_set.end()), "send message " + std::to_string(print_times - 1));
  EXPECT_EQ(label_set.size(), print_times);
  EXPECT_EQ(*(label_set.begin()), "label 0");
  EXPECT_EQ(*(--label_set.end()), "label " + std::to_string(print_times - 1));

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, LOG_BREAKLINE) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 3");
  custom_fields.emplace("job_id", "job 3");
  custom_fields.emplace("task_id", "task 3");
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields);
  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));
  RAY_EVENT(INFO, "TEST BREAKLINE") << "message1\n"
                                    << "message2\n";
  RAY_EVENT(INFO, "TEST BREAKLINE") << "message1\n\nmessage2\n\n\nmessage3";

  std::vector<std::string> vc = ReadEventFromFile(log_dir + "/event_RAYLET.log");

  rpc::Event ele_1 = GetEventFromString(vc[(int)vc.size() - 2]);
  CheckEventDetail(ele_1, "job 3", "node 3", "task 3", "RAYLET", "INFO", "TEST BREAKLINE",
                   "message1\\nmessage2\\n");

  rpc::Event ele_2 = GetEventFromString(vc[(int)vc.size() - 1]);
  CheckEventDetail(ele_2, "job 3", "node 3", "task 3", "RAYLET", "INFO", "TEST BREAKLINE",
                   "message1\\n\\nmessage2\\n\\n\\nmessage3");

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, LOG_ROTATE) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      std::unordered_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir, true, 1, 20));

  int print_times = 100000;
  for (int i = 1; i <= print_times; ++i) {
    RAY_EVENT(INFO, "label " + std::to_string(i)) << "send message " + std::to_string(i);
  }

  int cnt = 0;
  for (auto &entry :
       boost::make_iterator_range(boost::filesystem::directory_iterator(log_dir), {})) {
    if (entry.path().string().find("event_RAYLET") != std::string::npos) {
      cnt++;
    }
  }

  EXPECT_EQ(cnt, 21);
}

// ANT-INTERNAL
TEST(EVENT_TEST, TEST_LABEL_BLACKLIST) {
  std::string log_dir = GenerateLogDir();
  ray::EventManager::Instance().ClearReporters();
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      std::unordered_map<std::string, std::string>());
  ray::RayEventContext::Instance().SetLabelBlacklist("PIPELINE,PROCESS_EXIT");
  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));
  RAY_EVENT(INFO, "PIPELINE") << "message1";
  RAY_EVENT(INFO, "PIPELINE_1") << "message2";
  RAY_EVENT(INFO, "PROCESS_EXIT") << "message3";
  RAY_EVENT(INFO, "PROCESS_EXIT_1") << "message4";

  std::vector<std::string> vc = ReadEventFromFile(log_dir + "/event_RAYLET.log");
  EXPECT_EQ(vc.size(), 2);

  rpc::Event ele_1 = GetEventFromString(vc[0]);
  CheckEventDetail(ele_1, "", "", "", "RAYLET", "INFO", "PIPELINE_1", "message2");

  rpc::Event ele_2 = GetEventFromString(vc[1]);
  CheckEventDetail(ele_2, "", "", "", "RAYLET", "INFO", "PROCESS_EXIT_1", "message4");

  boost::filesystem::remove_all(log_dir.c_str());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
