#include <stdlib.h>

#include <thread>

#include "gtest/gtest.h"
#include "utility.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingUtilityTest, test_Byte2hex) {
  const uint8_t data[2] = {0x11, 0x07};
  EXPECT_TRUE(StreamingUtility::Byte2hex(data, 2) == "1107");
  EXPECT_TRUE(StreamingUtility::Byte2hex(data, 2) != "1108");
}

TEST(StreamingUtilityTest, test_Hex2str) {
  const uint8_t data[2] = {0x11, 0x07};
  EXPECT_TRUE(std::memcmp(StreamingUtility::Hexqid2str("1107").c_str(), data, 2) == 0);
  const uint8_t data2[2] = {0x10, 0x0f};
  EXPECT_TRUE(std::memcmp(StreamingUtility::Hexqid2str("100f").c_str(), data2, 2) == 0);
}

TEST(StreamingUtilityTest, testsplit) {
  std::string qid_hex = "000000000000000000000000000000009ae6745c0000000000010002";
  ray::ObjectID q_id = ray::ObjectID::FromBinary(StreamingUtility::Hexqid2str(qid_hex));
  std::vector<std::string> splited_vec;
  StreamingUtility::Split(q_id, splited_vec);
  EXPECT_TRUE(splited_vec[0] == "1" && splited_vec[1] == "2");
}

TEST(StreamingUtilityTest, test_edge_split) {
  std::string qid_hex = "000000000000000000000000000000009ae6745c0000000000010002";
  ray::ObjectID q_id = ray::ObjectID::FromBinary(StreamingUtility::Hexqid2str(qid_hex));
  EXPECT_TRUE(StreamingUtility::Qid2EdgeInfo(q_id) == "1-2");
  auto id = new ray::ObjectID(q_id);
  EXPECT_TRUE(StreamingUtility::Qid2EdgeInfo(*id) == "1-2");
}

TEST(StreamingUtilityTest, test_auto_spink_lock) {
  int test_num = 0;
  std::atomic_flag flag = ATOMIC_FLAG_INIT;
  std::vector<std::unique_ptr<std::thread>> threads;
  for (int i = 0; i < 10; ++i) {
    threads.push_back(std::unique_ptr<std::thread>(new std::thread([&test_num, &flag]() {
      for (int j = 0; j < 100; ++j) {
        AutoSpinLock lock(flag);
        test_num++;
      }
    })));
  }
  for (int i = 0; i < 10; ++i) {
    threads[i]->join();
  }
  EXPECT_TRUE(test_num == 1000);
}

TEST(StreamingUtilityTest, CountingSemaphoreTest) {
  CountingSemaphore semaphore;
  semaphore.Init(8);
  int worker1_count = 0;
  int worker2_count = 0;
  std::thread worker1 = std::thread([&] {
    for (int i = 0; i < 4; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      worker1_count++;
      semaphore.Notify();
    }
  });
  std::thread worker2 = std::thread([&] {
    for (int i = 0; i < 4; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      worker2_count++;
      semaphore.Notify();
    }
  });

  semaphore.Wait();
  EXPECT_EQ(worker1_count, 4);
  EXPECT_EQ(worker2_count, 4);
  worker1.join();
  worker2.join();
}

TEST(StreamingUtilityTest, test_online_env) {
  setenv("ALIPAY_APP_ENV", "prod", 1);
  EXPECT_TRUE(StreamingUtility::IsOnlineEnv());
  setenv("ALIPAY_APP_ENV", "", 1);
  EXPECT_FALSE(StreamingUtility::IsOnlineEnv());
  unsetenv("ALIPAY_APP_ENV");
  EXPECT_FALSE(StreamingUtility::IsOnlineEnv());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
