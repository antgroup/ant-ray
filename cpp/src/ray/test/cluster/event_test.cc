
#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/event_severity.h>
#include <ray/api/ray_config.h>

#include <string>
#include "cpp/src/ray/config_internal.h"
#include "eventer.h"

TEST(EventTest, TestEventFile) {
  ray::ReportEvent(ray::EventSeverity::INFO, "DRIVER", "test event");
  std::string event_dir = ray::internal::ConfigInternal::Instance().logs_dir + "/events";
  EXPECT_EQ(1, Search(event_dir, "DRIVER", "test event"));
  ray::ActorHandleCpp<Eventer> actor =
      ray::Actor(RAY_FUNC(Eventer::FactoryCreate)).Remote();
  auto ret = actor.Task(&Eventer::WriteEvent).Remote();
  EXPECT_TRUE(*ret.Get());
  ret = actor.Task(&Eventer::WriteEvent).Remote();
  EXPECT_TRUE(*ret.Get());
  auto count = actor.Task(&Eventer::SearchEvent)
                   .Remote(event_dir, "PIPELINE_2",
                           "running increment function in the Eventer Actor");
  EXPECT_EQ(2, *count.Get());
  ret = actor.Task(&Eventer::WriteEventInPrivateThread).Remote();
  EXPECT_TRUE(*ret.Get());
  count =
      actor.Task(&Eventer::SearchEvent)
          .Remote(event_dir, "PIPELINE_2",
                  "running increment function in the Eventer Actor in private thread");
  EXPECT_EQ(1, *count.Get());
}

int main(int argc, char **argv) {
  ray::RayConfigCpp config;
  ray::Init(config, argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  ray::Shutdown();
  return ret;
}