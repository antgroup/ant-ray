#pragma once

#include <thread>

#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

class PlasmaStats {
 public:
  PlasmaStats(const PlasmaStoreInfo &store_info, const int metrics_interval_ms = 1000);
  ~PlasmaStats();

 private:
  void StartIOService();
  void UpdateMetrics();

  boost::asio::io_service io_service_;
  std::thread io_thread_;
  boost::posix_time::milliseconds metrics_timer_interval_;
  std::unique_ptr<boost::asio::deadline_timer> metrics_timer_;
  std::function<void(const boost::system::error_code &error)> metrics_timer_func_;

  const PlasmaStoreInfo &store_info_;
};

}  // namespace plasma