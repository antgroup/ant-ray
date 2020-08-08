#pragma once

#include <memory>
#include "absl/synchronization/mutex.h"
#include "invocation_spec.h"
#include "ray/core.h"
#include <boost/dll.hpp>

namespace ray {
namespace api {

class AbstractRayRuntime;

class ActorContext {
 public:
  std::shared_ptr<msgpack::sbuffer> current_actor = nullptr;

  std::shared_ptr<absl::Mutex> actor_mutex;

  ActorContext() { actor_mutex = std::shared_ptr<absl::Mutex>(new absl::Mutex); }
};

class TaskExecutor {
 public:
  TaskExecutor(AbstractRayRuntime &abstract_ray_tuntime_);

  /// TODO(Guyang Song): support multiple tasks execution
  std::unique_ptr<ObjectID> Execute(const InvocationSpec &invocation);

  static void Invoke(const TaskSpecification &task_spec,
                     std::shared_ptr<msgpack::sbuffer> actor,
                     AbstractRayRuntime *runtime);

  virtual ~TaskExecutor(){};

 private:
  static std::string handle_request(msgpack::sbuffer& buf){
    boost::dll::shared_library lib("/Users/yu/Documents/myso/cmake-build-debug/libmyso.dylib");
    if(!lib){
      return "load dll failed!";
    }
    //call function in so
    auto call_fn = lib.get<std::string(const char*, size_t)>("call_in_so0");
    //get resutl and send to the client
    auto s = call_fn(buf.data(), buf.size());

    return s;
  }

  AbstractRayRuntime &abstract_ray_tuntime_;
};
}  // namespace api
}  // namespace ray