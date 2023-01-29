
#include <gtest/gtest.h>
#include <ray/api.h>

#include <future>
#include <thread>

#include "boost/filesystem.hpp"
#include "ray/common/constants.h"
#include "ray/util/logging.h"

class SimpleActor {
 public:
  static SimpleActor *FactoryCreate() {
    SimpleActor *actor = new SimpleActor();
    return actor;
  }
  void TestJobDataDir() {
    auto job_data_dir = ray::GetJobDataDir();
    std::ostringstream ss;
    ss << getpid();
    std::string except_job_data_dir =
        std::string(kDefaultJobDataDirBase) + "/" + ss.str();
    EXPECT_EQ(job_data_dir, except_job_data_dir);
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(job_data_dir)));
  }
};

RAY_REMOTE(SimpleActor::FactoryCreate, &SimpleActor::TestJobDataDir);

TEST(RayApiTest, GetJobDataDir) {
  ray::RayConfigCpp config;
  ray::Init(config);

  auto actor = ray::Actor(SimpleActor::FactoryCreate).Remote();
  actor.Task(&SimpleActor::TestJobDataDir).Remote().Get();

  std::string except_job_data_dir_base =
      boost::filesystem::current_path().string() + "/tmp";
  setenv(kEnvVarKeyJobDataDirBase, except_job_data_dir_base.c_str(), true);
  auto job_data_dir = ray::GetJobDataDir();
  std::ostringstream ss;
  ss << getpid();
  std::string except_job_data_dir = except_job_data_dir_base + "/" + ss.str();
  EXPECT_EQ(job_data_dir, except_job_data_dir);
  EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(job_data_dir)));
  ray::Shutdown();
}
