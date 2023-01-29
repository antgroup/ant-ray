#include <cstdlib>

#include "gtest/gtest.h"
#include "runtime_context.h"

using namespace ray::streaming;
using namespace ray;

class CommonMock {
 public:
  CommonMock() : runtime_context_(new RuntimeContext()), report_count_(0) {
    STREAMING_LOG(INFO) << "Common Mock constructing";
  }
  virtual ~CommonMock() {
    STREAMING_LOG(INFO) << "Common Mock deconstructing";
    runtime_context_->ShutdownTimer();
  }

  void InitCommonMock() {
    report_count_ = 0;
    runtime_context_->transfer_state_.SetInit();
    setenv("STREAMING_ENABLE_METRICS", "true", 1);
    runtime_context_->metrics_config_.SetMetricsReportInterval(1);
  }

  void SetTransferStateRunning() { runtime_context_->transfer_state_.SetRunning(); }

  void SetTransferStateInterrupted() {
    runtime_context_->transfer_state_.SetInterrupted();
  }

  void DoEnableTimer() {
    runtime_context_->EnableTimer(std::bind(&CommonMock::ReportTimer, this));
  }

  void DoShutdownTimer() { runtime_context_->ShutdownTimer(); }

  void SetConfig(StreamingConfig &config) { runtime_context_->SetConfig(config); }

 protected:
  void ReportTimer() {
    ++report_count_;
    STREAMING_LOG(INFO) << "Reporter count " << report_count_;
  }

 public:
  std::shared_ptr<RuntimeContext> runtime_context_;
  int report_count_;
};

class StreamingCommonTests : public ::testing::Test {
 protected:
  virtual void SetUp() override {
    common_mock_.InitCommonMock();
    StreamingConfig config;
    config.SetStreamingMetricsEnable(true);
    common_mock_.SetConfig(config);
    common_mock_.SetTransferStateRunning();
  }

  virtual void TearDown() override {
    common_mock_.SetTransferStateInterrupted();
    common_mock_.DoShutdownTimer();
  }

 protected:
  CommonMock common_mock_;
};

TEST_F(StreamingCommonTests, common_reporter_timer) {
  common_mock_.DoEnableTimer();
  std::this_thread::sleep_for(std::chrono::seconds(3));
  STREAMING_LOG(INFO) << "Shutdown Timer";
  common_mock_.SetTransferStateInterrupted();
  common_mock_.DoShutdownTimer();
  STREAMING_LOG(INFO) << "Enable Timer";
  common_mock_.SetTransferStateRunning();
  common_mock_.DoEnableTimer();
  std::this_thread::sleep_for(std::chrono::seconds(4));
  STREAMING_LOG(INFO) << "Report Count => " << common_mock_.report_count_;
  ASSERT_TRUE(common_mock_.report_count_ > 2 && common_mock_.report_count_ <= 7);
}

extern "C" void __cxa_pure_virtual() {
  STREAMING_LOG(INFO) << "Ignore pure virtual method called";
  exit(0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
