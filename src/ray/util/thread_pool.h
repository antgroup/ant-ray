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

#include <boost/thread.hpp>
#include <iostream>

#include "ray/common/asio/instrumented_io_context.h"

namespace ray {
class thread_pool : public boost::asio::thread_pool {
 private:
  /// The number of thread pool.
  size_t thread_number_;

 public:
  /// By default, the thread pool is initialized with the number of computer CPU cores
  /// plus one.
  thread_pool() : thread_pool(boost::thread::physical_concurrency()) {}

  thread_pool(const size_t thread_number)
      : boost::asio::thread_pool(thread_number), thread_number_(thread_number) {}

  const size_t GetThreadNumber() const { return thread_number_; }
};
}  // namespace ray