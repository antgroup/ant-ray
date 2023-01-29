#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/test_util.h"
#include "ray/common/watchdog.h"

namespace ray {

class WatchDogTest : public ::testing::Test {
  using TerminatedCallbackArgs =
      std::tuple<pid_t, boost::optional<int>, boost::optional<std::string>,
                 boost::optional<int>, std::string>;

 public:
  WatchDogTest()
      : watchdog_([this](pid_t pid, boost::optional<int> signal_no,
                         boost::optional<std::string> signal_name,
                         boost::optional<int> exit_code,
                         const std::string &process_data) {
          RAY_CHECK((signal_no && !exit_code) || (!signal_no && exit_code));
          terminated_processes_.push_back(
              std::make_tuple(pid, signal_no, signal_name, exit_code, process_data));
        }){};

  ChildProcessWatchdog<std::string> watchdog_;
  std::vector<TerminatedCallbackArgs> terminated_processes_;

  void TestWatchDogTerminated(boost::optional<int> expected_signal_no,
                              boost::optional<std::string> expected_signal_name,
                              boost::optional<int> expected_exit_code) {
    std::string expected_process_data("test_process_data");
    ASSERT_TRUE((expected_signal_no && !expected_exit_code) ||
                (!expected_signal_no && expected_exit_code));
    pid_t pid = fork();
    if (pid) {
      watchdog_.WatchChildProcess(pid, expected_process_data);
      if (expected_signal_no) kill(pid, *expected_signal_no);
    } else {
      if (expected_signal_no) {
        std::this_thread::sleep_for(std::chrono::milliseconds(3600 * 1000));
      } else {
        exit(*expected_exit_code);
      }
      return;
    }
    EXPECT_TRUE(WaitForCondition([this]() { return terminated_processes_.size() == 1; },
                                 10 * 1000));
    pid_t arg_pid;
    boost::optional<int> signal_no, exit_code;
    boost::optional<std::string> signal_name;
    std::string process_data;
    std::tie(arg_pid, signal_no, signal_name, exit_code, process_data) =
        terminated_processes_[0];
    ASSERT_EQ(arg_pid, pid);
    ASSERT_EQ(process_data, expected_process_data);
    if (signal_no) {
      ASSERT_EQ(*signal_no, *expected_signal_no);
      ASSERT_TRUE(boost::algorithm::contains(*signal_name, *expected_signal_name));
      ASSERT_TRUE(!exit_code);
    } else {
      ASSERT_TRUE(!signal_no);
      ASSERT_TRUE(!signal_name);
      ASSERT_EQ(*exit_code, *expected_exit_code);
    }
  }
};

TEST_F(WatchDogTest, TestWatchDogWhenChildProcessSignaledByNoSIGKILL) {
  std::string expected_signal_name("Killed");
  TestWatchDogTerminated(SIGKILL, expected_signal_name, boost::none);
}

TEST_F(WatchDogTest, TestWatchDogWhenChildProcessSignaledByNoSIGSEGV) {
  std::string expected_signal_name("Segmentation fault");
  TestWatchDogTerminated(SIGSEGV, expected_signal_name, boost::none);
}

TEST_F(WatchDogTest, TestWatchDogWhenChildProcessSignaledByNoSIGABRT) {
  std::string expected_signal_name("Abort");
  TestWatchDogTerminated(SIGABRT, expected_signal_name, boost::none);
}

TEST_F(WatchDogTest, TestWatchDogWhenChildProcessSignaledByNoSIGTERM) {
  std::string expected_signal_name("Terminated");
  TestWatchDogTerminated(SIGTERM, expected_signal_name, boost::none);
}

TEST_F(WatchDogTest, TestWatchDogWhenChildProcessExitedByCode12) {
  TestWatchDogTerminated(boost::none, boost::none, 12);
}

TEST_F(WatchDogTest, TestWatchDogWhenChildProcessExitedByBode0) {
  TestWatchDogTerminated(boost::none, boost::none, 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace ray