#include <gtest/gtest.h>
#include <ray/api.h>
#include <msgpack.hpp>

using namespace ::ray::api;

int *cmd_argc = nullptr;
char ***cmd_argv = nullptr;

class ListWrapper {
 private:
  std::list<ObjectRef<std::string>> l_;
 public:
  ListWrapper() = default;
  ListWrapper(const std::list<ObjectRef<std::string>>& l) : l_(l) {
  }

  const std::list<ObjectRef<std::string>> &GetList() {
    return l_;
  }

  MSGPACK_DEFINE(l_);
};

class RemoteActor {
 public:
  std::string Get(ListWrapper value);
  std::string GetNested(std::list<ObjectRef<ObjectRef<std::string>>> value);
  std::string Test();

  static RemoteActor *FactoryCreate();
};

std::string RemoteActor::Get(ListWrapper value) {
  RAYLOG(INFO) << "RemoteActor::Get called";
  return *(Ray::Get(value.GetList().front()));
}

std::string RemoteActor::GetNested(std::list<ObjectRef<ObjectRef<std::string>>> value) {
  return *(Ray::Get(*(value.front().Get())));
}

std::string RemoteActor::Test() {
  return "Hello";
}

RemoteActor *RemoteActor::FactoryCreate() { return new RemoteActor(); }

RAY_REMOTE(RemoteActor::FactoryCreate, &RemoteActor::Get, &RemoteActor::GetNested, &RemoteActor::Test);

TEST(ObjectRefTransferTest, ObjectTransferTest) {
  ray::api::RayConfig config;
  config.local_mode = true;
  Ray::Init(config, cmd_argc, cmd_argv);

  ObjectRef<std::string> object_ref = Ray::Put(std::string("test"));
  std::list<ObjectRef<std::string>> data;
  data.push_back(object_ref);
  ListWrapper list_wrapper(data);

  ActorHandle<RemoteActor> handle = Ray::Actor(RAY_FUNC(RemoteActor::FactoryCreate)).Remote();
  // auto result = handle.Task(&RemoteActor::Test).Remote();
  // std::cout << "Result: "<< *result.Get() << std::endl;
  auto result_ref = handle.Task(&RemoteActor::Get).Remote(list_wrapper);
  std::cout << "Result: " << *(Ray::Get(result_ref)) << std::endl;

  Ray::Shutdown();
}

TEST(ObjectRefTransferTest, NestedObjectIdTest) {

}


int main(int argc, char **argv) {
  cmd_argc = &argc;
  cmd_argv = &argv;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
