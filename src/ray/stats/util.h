// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <queue>

namespace ray {

namespace stats {

class Percentile {
 public:
  Percentile(uint32_t window, std::function<void(double)> p99_callback,
             std::function<void(double)> p95_callback,
             std::function<void(double)> p90_callback)
      : p99_(window * 0.01, window, p99_callback),
        p95_(window * 0.05, window, p95_callback),
        p90_(window * 0.1, window, p90_callback) {}

  void Record(double value) {
    absl::MutexLock lock(&mu_);
    p99_.Record(-1 * value);
    p95_.Record(-1 * value);
    p90_.Record(-1 * value);
  }

  std::string DebugString() {
    std::stringstream s;
    s << "p99 " << p99_.DebugString() << " p95 " << p95_.DebugString() << " p90 "
      << p90_.DebugString();
    return s.str();
  }

 private:
  class Impl {
   public:
    Impl(uint32_t capacity, uint32_t window, std::function<void(double)> callback)
        : capacity_(capacity), window_(window), cur_num_(0), callback_(callback) {}

    void Record(double value) {
      if (min_heap_.size() < capacity_) {
        min_heap_.push(value);
      } else if ((value) < min_heap_.top()) {
        min_heap_.pop();
        min_heap_.push(value);
      }

      if (++cur_num_ % window_ == 0) {
        callback_(-1 * min_heap_.top());
        cur_num_ = 0;
        min_heap_ = std::priority_queue<double>();
      }
    }

    std::string DebugString() {
      std::stringstream s;
      s << "capacity " << capacity_ << " window " << window_;
      return s.str();
    }

   private:
    std::priority_queue<double> min_heap_;
    uint32_t capacity_;
    uint32_t window_;
    uint32_t cur_num_;
    std::function<void(double)> callback_;
  };

 private:
  Impl p99_;
  Impl p95_;
  Impl p90_;
  mutable absl::Mutex mu_;
};
}  // namespace stats

}  // namespace ray
