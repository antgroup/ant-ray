#include "ray/object_manager/plasma/stats.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

namespace plasma {

PlasmaStats::PlasmaStats(const PlasmaStoreInfo &store_info, const int metrics_interval_ms)
    : metrics_timer_interval_(metrics_interval_ms), store_info_(store_info) {
  metrics_timer_func_ = [this](const boost::system::error_code &error) {
    if (error != boost::asio::error::operation_aborted) {
      UpdateMetrics();
      metrics_timer_->expires_from_now(metrics_timer_interval_);
      metrics_timer_->async_wait(metrics_timer_func_);
    } else {
      RAY_LOG(INFO) << "Plasma strore metrics timer aborted.";
    }
  };
  metrics_timer_.reset(new boost::asio::deadline_timer(io_service_));
  metrics_timer_->expires_from_now(metrics_timer_interval_);
  metrics_timer_->async_wait(metrics_timer_func_);
  io_thread_ = std::thread(&PlasmaStats::StartIOService, this);
}

PlasmaStats::~PlasmaStats() {
  io_service_.stop();
  if (io_thread_.joinable()) {
    io_thread_.join();
  }
}

void PlasmaStats::StartIOService() {
  SetThreadName("plasma_store.stats");
  io_service_.run();
}

void PlasmaStats::UpdateMetrics() {
  ray::stats::PlasmaObjectCount().Record(store_info_.objects.size());
  ray::stats::PlasmaAllocatedMemory().Record(PlasmaAllocator::Allocated() >> 20);
  ray::stats::PlasmaTotalMemory().Record(PlasmaAllocator::GetFootprintLimit() >> 20);
}

}  // namespace plasma