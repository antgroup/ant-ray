// Copyright 2021 The Ray Authors.
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
namespace ray {
namespace core {


/// 类名再改一下？ 叫并发组执行池
template <typename ExecutorType>
class ConcurrencyGroupExecutorManager final {

public:

private:
  // Map from the name to their corresponding concurrency groups.
  std::unordered_map<std::string, std::shared_ptr<ExecutorType>>
      name_to_executor_index_;

  // Map from the FunctionDescriptors to their corresponding thread pools.
  std::unordered_map<std::string, std::shared_ptr<ExecutorType>>
      functions_to_executor_index_;

  // The thread pool for default concurrency group. It's nullptr if its max concurrency
  // is 1.
  std::shared_ptr<BoundedExecutor> default_thread_pool_ = nullptr;

};

} // namespace core
} // namespace ray
