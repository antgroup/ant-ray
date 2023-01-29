#include <memory>

#include "gtest/gtest.h"
#include "protobuf/streaming.pb.h"
#include "runtime_context.h"

using namespace ray;
using namespace ray::streaming;

class ConfigParserTest : public ::testing::Test {
  virtual void SetUp() { runtime_context = std::make_shared<RuntimeContext>(); }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
};

TEST_F(ConfigParserTest, common_test) {
  proto::StreamingConfig config;
  config.set_checkpoint_id(10);
  config.set_job_name("test");
  auto flow_control_config = config.flow_control_config();
  flow_control_config.set_flow_control_type(proto::FlowControlType::UNCONSUMED_MESSAGE);
  runtime_context->SetConfig(config.SerializeAsString());
  EXPECT_EQ(runtime_context->config_.GetStreamingRollbackCheckpointId(),
            config.checkpoint_id());
  EXPECT_EQ(runtime_context->config_.GetStreamingJobName(), config.job_name());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
