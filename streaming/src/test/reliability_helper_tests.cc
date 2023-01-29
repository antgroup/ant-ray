#include "gtest/gtest.h"
#include "reliability/barrier_helper.h"
#include "reliability_helper.h"

using namespace ray::streaming;
using namespace ray;

class ReliabilityHelperTest : public ::testing::Test {
 public:
  void SetUp() {
    re_helper_.reset(new ReliabilityHelper(config_, barrier_helper_, nullptr, nullptr));
  }

 protected:
  StreamingConfig config_;
  StreamingBarrierHelper barrier_helper_;
  std::unique_ptr<ReliabilityHelper> re_helper_;
  ProducerChannelInfo channel_info_;
};

TEST_F(ReliabilityHelperTest, filter_barrier_message) {
  uint64_t write_message_id = -1;
  channel_info_.current_message_id = 0;
  channel_info_.message_last_commit_id = 0;
  bool filterd = re_helper_->FilterMessage(
      channel_info_, nullptr, StreamingMessageType::Barrier, &write_message_id);
  ASSERT_TRUE(!filterd);
  ASSERT_EQ(write_message_id, 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
