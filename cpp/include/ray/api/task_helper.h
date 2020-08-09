//
// Created by qicosmos on 2020/7/31.
//

#ifndef RAY_TASK_HELPER_H
#define RAY_TASK_HELPER_H

#include <memory>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

template<typename R = void>
class TaskHelper {
 public:
  TaskHelper() = delete;
  TaskHelper(const TaskHelper &) = delete;
  TaskHelper &operator=(const TaskHelper &) = delete;

  template <typename... Args>
  TaskHelper(std::string func_name, Args &&... args)
      : runtime_(Ray::Runtime()),
        func_name_(std::move(func_name)),
        buffer_(2048){
    msgpack::pack(buffer_, std::forward_as_tuple(func_name_, std::forward<Args>(args)...));
  }

  ObjectRef<R> Remote() {
    auto data = std::make_shared<msgpack::sbuffer>(std::move(buffer_));
    auto returned_object_id = runtime_->Call(std::move(data));
    return ObjectRef<R>(returned_object_id);
  }

  template<typename T>
  T Unpack(){
    msgpack::unpacked msg;
    try {
      msgpack::unpack(msg, buffer_.data(), buffer_.size());
      return msg.get().as<T>();
    } catch (...) { throw std::invalid_argument("unpack failed: Args not match!"); }
  }

 private:
  std::shared_ptr<RayRuntime> runtime_;
  std::string func_name_;
  msgpack::sbuffer buffer_;
};
}
}

#endif  // RAY_TASK_HELPER_H
