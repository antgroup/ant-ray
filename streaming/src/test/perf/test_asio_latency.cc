#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <chrono>
#include <string>
#include <thread>
#include "gtest/gtest.h"

#include "queue/transport.h"
#include "streaming/src/util/logging.h"

using namespace ray;
using namespace ray::streaming;

// static void AsioWorkerThread(boost::asio::io_service *io_service) {
//   STREAMING_LOG(INFO) << "AsioWorkerThread tid: " << std::this_thread::get_id();
//   io_service->run();
//   STREAMING_LOG(INFO) << "AsioWorkerThread done";
// }

void SetCPUAffinity(int random_cpu) {
  cpu_set_t *mask;
  // Why we use 256 here? cpu num is not continuous, so we assume the max cpu number of
  // one machine is less then 256.
  unsigned int core_num = 256;
  mask = CPU_ALLOC(core_num);
  int mask_size = CPU_ALLOC_SIZE(core_num);
  std::vector<int> masked_cpu_vec;
  CPU_ZERO_S(mask_size, mask);
  if (sched_getaffinity(0, mask_size, mask) != 0) {
    STREAMING_LOG(INFO) << "Get affinity failed";
  }
  std::string affinity_list = "|";
  for (uint32_t cpu = 0; cpu < core_num; ++cpu) {
    if (CPU_ISSET_S(cpu, mask_size, mask) != 0) {
      masked_cpu_vec.push_back(cpu);
      affinity_list += std::to_string(cpu) + "|";
    }
  }
  STREAMING_LOG(INFO) << "Cpu affinity list => " << affinity_list;
  // Only half of cpu affinity list will be added in random list that the final
  // affinity cpu number selected from.
  // int random_cpu = 0;
  // if (masked_cpu_vec.size() >= 2) {
  //   random_cpu = masked_cpu_vec[current_sys_time_ms() % (masked_cpu_vec.size() / 2)];
  // }
  CPU_ZERO_S(mask_size, mask);
  CPU_SET_S(random_cpu, mask_size, mask);
  if (sched_setaffinity(0, mask_size, mask) != 0) {
    STREAMING_LOG(INFO) << "Fail to sched_setaffinity " << random_cpu;
  } else {
    STREAMING_LOG(INFO) << "Affinity cpu num => " << random_cpu;
  }
  CPU_FREE(mask);
}

TEST(TESTASIO, LatencyTest) {
  SetCPUAffinity(1);
  boost::asio::io_service io_service_;
  boost::asio::io_service::work dummy_work_(io_service_);
  std::thread main_thread([&io_service_] {
    SetCPUAffinity(1);
    io_service_.run();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  uint64_t count = 200000;
  uint8_t data[100];
  while (count--) {
    uint64_t timestamp = current_sys_time_ms();
    memcpy(data, &timestamp, sizeof(uint64_t));
    std::shared_ptr<ray::streaming::LocalMemoryBuffer> buffer =
        std::make_shared<ray::streaming::LocalMemoryBuffer>(data, 100, true);

    io_service_.post([buffer] {
      static uint64_t total_count = 0;
      static uint64_t large_count = 0;
      static uint64_t qps_timestamp = current_sys_time_ms();
      total_count++;
      uint64_t current = current_sys_time_ms();
      uint8_t *data = buffer->Data();
      uint64_t *timestamp = (uint64_t *)data;
      if (current - *timestamp > 10) {
        large_count++;
      }
      if (total_count % 10000 == 0) {
        STREAMING_LOG(INFO) << "10000 tasks, QPS: "
                            << 10000 / ((float)(current - qps_timestamp) / 1000) << "/s"
                            << " larger than 10ms latency count: " << large_count;
        large_count = 0;
        qps_timestamp = current;
        STREAMING_LOG(INFO) << "total_count: " << total_count;
      }
    });
    std::this_thread::sleep_for(std::chrono::microseconds(1000));
  }
  EXPECT_TRUE(true);
  io_service_.stop();
  if (main_thread.joinable()) {
    main_thread.join();
  }
}

TEST(TESTASIO, LatencyTestDispatch) {
  SetCPUAffinity(1);
  boost::asio::io_service io_service_;
  boost::asio::io_service::work dummy_work_(io_service_);
  std::thread main_thread([&io_service_] {
    SetCPUAffinity(1);
    io_service_.run();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  uint64_t count = 200000;
  uint8_t data[100];
  while (count--) {
    uint64_t timestamp = current_sys_time_ms();
    memcpy(data, &timestamp, sizeof(uint64_t));
    std::shared_ptr<ray::streaming::LocalMemoryBuffer> buffer =
        std::make_shared<ray::streaming::LocalMemoryBuffer>(data, 100, true);

    io_service_.post([buffer, &io_service_] {
      io_service_.dispatch([buffer] {
        static uint64_t total_count = 0;
        static uint64_t large_count = 0;
        static uint64_t qps_timestamp = current_sys_time_ms();
        total_count++;
        uint64_t current = current_sys_time_ms();
        uint8_t *data = buffer->Data();
        uint64_t *timestamp = (uint64_t *)data;
        if (current - *timestamp > 10) {
          large_count++;
        }
        if (total_count % 10000 == 0) {
          STREAMING_LOG(INFO) << "10000 tasks, QPS: "
                              << 10000 / ((float)(current - qps_timestamp) / 1000) << "/s"
                              << " larger than 10ms latency count: " << large_count;
          large_count = 0;
          qps_timestamp = current;
          STREAMING_LOG(INFO) << "total_count: " << total_count;
        }
      });
    });
    std::this_thread::sleep_for(std::chrono::microseconds(1000));
  }
  EXPECT_TRUE(true);
  io_service_.stop();
  if (main_thread.joinable()) {
    main_thread.join();
  }
}

ray::Status TcpConnect(boost::asio::ip::tcp::socket &socket,
                       const std::string &ip_address_string, int port) {
  // Disable Nagle's algorithm, which caused transfer delays of 10s of ms in
  // certain cases.
  socket.open(boost::asio::ip::tcp::v4());
  boost::asio::ip::tcp::no_delay option(true);
  socket.set_option(option);

  boost::asio::ip::address ip_address =
      boost::asio::ip::address::from_string(ip_address_string);
  boost::asio::ip::tcp::endpoint endpoint(ip_address, port);
  boost::system::error_code error;
  socket.connect(endpoint, error);
  const auto status = boost_to_ray_status(error);
  if (!status.ok()) {
    // Close the socket if the connect failed.
    boost::system::error_code close_error;
    socket.close(close_error);
  }
  return status;
}

TEST(TESTASIO, LatencyTest2) {
  boost::asio::io_service io_service_;
  boost::asio::io_service::work dummy_work_(io_service_);
  std::thread main_thread([&io_service_] { io_service_.run(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  uint64_t count = 20000000;
  while (count--) {
    uint64_t timestamp = current_sys_time_ms();

    io_service_.post([timestamp] {
      static uint64_t total_count = 0;
      static uint64_t large_count = 0;
      static uint64_t qps_timestamp = current_sys_time_ms();
      total_count++;
      uint64_t current = current_sys_time_ms();
      if (current - timestamp > 10) {
        large_count++;
      }
      if (total_count % 10000 == 0) {
        STREAMING_LOG(INFO) << "10000 tasks, QPS: "
                            << 10000 / ((float)(current - qps_timestamp) / 1000) << "/s"
                            << " larger than 10ms latency count: " << large_count;
        large_count = 0;
        qps_timestamp = current;
      }
    });
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
  EXPECT_TRUE(true);
  io_service_.stop();
  if (main_thread.joinable()) {
    main_thread.join();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
