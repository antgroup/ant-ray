#include "ray/common/ray_config.h"

#include "gtest/gtest.h"

TEST(RayConfigTest, TestValidation) {
  // Key error
  ASSERT_FALSE(RayConfig::instance().validate("{\"lulala\": 123}"));
  ASSERT_FALSE(RayConfig::instance().validate("{\"evenStats\": true}"));
  // Value type error
  ASSERT_FALSE(RayConfig::instance().validate("{\"event_stats\": 1}"));
  ASSERT_FALSE(RayConfig::instance().validate("{\"event_stats\": 111}"));
  ASSERT_FALSE(RayConfig::instance().validate("{\"event_stats\": True}"));
  ASSERT_FALSE(RayConfig::instance().validate("{\"event_stats\": \"true\"}"));
  ASSERT_FALSE(RayConfig::instance().validate("{\"num_heartbeats_warning\": \"100\"}"));

  // OK
  ASSERT_TRUE(RayConfig::instance().validate("{\"event_stats\": true}"));
  ASSERT_TRUE(RayConfig::instance().validate("{\"num_heartbeats_warning\": 100}"));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
