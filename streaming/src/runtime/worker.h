#pragma once
#include <ray/api.h>

#include "task.h"
namespace ray {
namespace streaming {

class Worker {
 public:
  bool RegisterContext(const std::string conf);
  void RunLocalFunction(const std::string lib_path, const std::string function_creator);

 private:
  std::shared_ptr<StreamTask> task_;
};

Worker *CreateWorker();

RAY_REMOTE(CreateWorker, &ray::streaming::Worker::RegisterContext,
           &ray::streaming::Worker::RunLocalFunction);

}  // namespace streaming
}  // namespace ray