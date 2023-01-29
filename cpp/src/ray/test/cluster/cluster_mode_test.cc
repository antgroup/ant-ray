
#include <gtest/gtest.h>
#include <ray/api.h>

#include "../../runtime/abstract_ray_runtime.h"
#include "../../runtime/object/native_object_store.h"
#include "../../util/process_helper.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "counter.h"
#include "nlohmann/json.hpp"
#include "plus.h"

int cmd_argc = 0;
char **cmd_argv = nullptr;

ABSL_FLAG(bool, external_cluster, false, "");
ABSL_FLAG(std::string, redis_password, "12345678", "");
ABSL_FLAG(int32_t, redis_port, 6379, "");

TEST(RayClusterModeTest, Initialized) {
  ray::Init();
  EXPECT_TRUE(ray::IsInitialized());
  ray::Shutdown();
  EXPECT_TRUE(!ray::IsInitialized());
}

struct Person {
  std::string name;
  int age;
  MSGPACK_DEFINE(name, age);
};

TEST(RayClusterModeTest, FullTest) {
  ray::RayConfigCpp config;
  config.head_args = {"--num-cpus", "2", "--resources",
                      "{\"resource1\":1,\"resource2\":2}"};
  if (absl::GetFlag(FLAGS_external_cluster)) {
    auto port = absl::GetFlag(FLAGS_redis_port);
    std::string password = absl::GetFlag<std::string>(FLAGS_redis_password);
    ray::internal::ProcessHelper::GetInstance().StartRayNode(port, password);
    config.address = "127.0.0.1:" + std::to_string(port);
    config.redis_password_ = password;
  }
  ray::Init(config, cmd_argc, cmd_argv);
  /// put and get object
  auto obj = ray::Put(12345);
  auto get_result = *(ray::Get(obj));
  EXPECT_EQ(12345, get_result);

  auto named_obj =
      ray::Task(Return1).SetName("named_task").SetResources({{"CPU", 1.0}}).Remote();
  EXPECT_EQ(1, *named_obj.Get());

  /// common task without args
  auto task_obj = ray::Task(Return1).Remote();
  int task_result = *(ray::Get(task_obj));
  EXPECT_EQ(1, task_result);

  /// common task with args
  task_obj = ray::Task(Plus1).Remote(5);
  task_result = *(ray::Get(task_obj));
  EXPECT_EQ(6, task_result);

  ray::ActorHandleCpp<Counter> actor = ray::Actor(RAY_FUNC(Counter::FactoryCreate))
                                           .SetMaxRestarts(1)
                                           .SetName("named_actor")
                                           .Remote();
  auto initialized_obj = actor.Task(&Counter::Initialized).Remote();
  EXPECT_TRUE(*initialized_obj.Get());
  auto named_actor_obj = actor.Task(&Counter::Plus1)
                             .SetName("named_actor_task")
                             .SetResources({{"CPU", 1.0}})
                             .Remote();
  EXPECT_EQ(1, *named_actor_obj.Get());

  auto named_actor_handle_optional = ray::GetActor<Counter>("named_actor");
  EXPECT_TRUE(named_actor_handle_optional);
  auto &named_actor_handle = *named_actor_handle_optional;
  auto named_actor_obj1 = named_actor_handle.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(2, *named_actor_obj1.Get());
  EXPECT_FALSE(ray::GetActor<Counter>("not_exist_actor"));

  EXPECT_FALSE(
      *named_actor_handle.Task(&Counter::CheckRestartInActorCreationTask).Remote().Get());
  EXPECT_FALSE(
      *named_actor_handle.Task(&Counter::CheckRestartInActorTask).Remote().Get());
  named_actor_handle.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  auto named_actor_obj2 = named_actor_handle.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(1, *named_actor_obj2.Get());
  EXPECT_TRUE(
      *named_actor_handle.Task(&Counter::CheckRestartInActorCreationTask).Remote().Get());
  EXPECT_TRUE(*named_actor_handle.Task(&Counter::CheckRestartInActorTask).Remote().Get());

  named_actor_handle.Kill();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_THROW(named_actor_handle.Task(&Counter::Plus1).Remote().Get(),
               ray::internal::RayActorException);

  EXPECT_FALSE(ray::GetActor<Counter>("named_actor"));

  /// actor task without args
  auto actor1 = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto actor_object1 = actor1.Task(&Counter::Plus1).Remote();
  int actor_task_result1 = *(ray::Get(actor_object1));
  EXPECT_EQ(1, actor_task_result1);

  /// actor task with args
  auto actor2 = ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(1);
  auto actor_object2 = actor2.Task(&Counter::Add).Remote(5);
  int actor_task_result2 = *(ray::Get(actor_object2));
  EXPECT_EQ(6, actor_task_result2);

  /// actor task with args which pass by reference
  auto actor3 = ray::Actor(RAY_FUNC(Counter::FactoryCreate, int, int)).Remote(6, 0);
  auto actor_object3 = actor3.Task(&Counter::Add).Remote(actor_object2);
  int actor_task_result3 = *(ray::Get(actor_object3));
  EXPECT_EQ(12, actor_task_result3);

  /// general function remote call（args passed by value）
  auto r0 = ray::Task(Return1).Remote();
  auto r1 = ray::Task(Plus1).Remote(30);
  auto r2 = ray::Task(Plus).Remote(3, 22);

  std::vector<ray::ObjectRef<int>> objects = {r0, r1, r2};
  auto result = ray::Wait(objects, 3, 5000);
  EXPECT_EQ(result.ready.size(), 3);
  EXPECT_EQ(result.unready.size(), 0);

  auto result_vector = ray::Get(objects);
  int result0 = *(result_vector[0]);
  int result1 = *(result_vector[1]);
  int result2 = *(result_vector[2]);
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 31);
  EXPECT_EQ(result2, 25);

  /// general function remote call（args passed by reference）
  auto r3 = ray::Task(Return1).Remote();
  auto r4 = ray::Task(Plus1).Remote(r3);
  auto r5 = ray::Task(Plus).Remote(r4, r3);
  auto r6 = ray::Task(Plus).Remote(r4, 10);

  int result5 = *(ray::Get(r5));
  int result4 = *(ray::Get(r4));
  int result6 = *(ray::Get(r6));
  int result3 = *(ray::Get(r3));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result3, 1);
  EXPECT_EQ(result4, 2);
  EXPECT_EQ(result5, 3);
  EXPECT_EQ(result6, 12);

  /// create actor and actor function remote call with args passed by value
  auto actor4 = ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(10);
  auto r7 = actor4.Task(&Counter::Add).Remote(5);
  auto r8 = actor4.Task(&Counter::Add).Remote(1);
  auto r9 = actor4.Task(&Counter::Add).Remote(3);
  auto r10 = actor4.Task(&Counter::Add).Remote(8);

  int result7 = *(ray::Get(r7));
  int result8 = *(ray::Get(r8));
  int result9 = *(ray::Get(r9));
  int result10 = *(ray::Get(r10));
  EXPECT_EQ(result7, 15);
  EXPECT_EQ(result8, 16);
  EXPECT_EQ(result9, 19);
  EXPECT_EQ(result10, 27);

  /// create actor and task function remote call with args passed by reference
  auto actor5 = ray::Actor(RAY_FUNC(Counter::FactoryCreate, int, int)).Remote(r10, 0);

  auto r11 = actor5.Task(&Counter::Add).Remote(r0);
  auto r12 = actor5.Task(&Counter::Add).Remote(r11);
  auto r13 = actor5.Task(&Counter::Add).Remote(r10);
  auto r14 = actor5.Task(&Counter::Add).Remote(r13);
  auto r15 = ray::Task(Plus).Remote(r0, r11);
  auto r16 = ray::Task(Plus1).Remote(r15);

  int result12 = *(ray::Get(r12));
  int result14 = *(ray::Get(r14));
  int result11 = *(ray::Get(r11));
  int result13 = *(ray::Get(r13));
  int result16 = *(ray::Get(r16));
  int result15 = *(ray::Get(r15));

  EXPECT_EQ(result11, 28);
  EXPECT_EQ(result12, 56);
  EXPECT_EQ(result13, 83);
  EXPECT_EQ(result14, 166);
  EXPECT_EQ(result15, 29);
  EXPECT_EQ(result16, 30);

  uint64_t pid = *actor1.Task(&Counter::GetPid).Remote().Get();
  EXPECT_TRUE(Counter::IsProcessAlive(pid));

  auto actor_object4 = actor1.Task(&Counter::Exit).Remote();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_THROW(actor_object4.Get(), ray::internal::RayActorException);
  EXPECT_FALSE(Counter::IsProcessAlive(pid));
}

TEST(RayClusterModeTest, ActorHandleTest) {
  auto actor1 = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto obj1 = actor1.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(1, *obj1.Get());
  // Test `ActorHandle` type object as parameter.
  auto actor2 = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto obj2 = actor2.Task(&Counter::Plus1ForActor).Remote(actor1);
  EXPECT_EQ(2, *obj2.Get());
  // Test `ActorHandle` type object as return value.
  std::string child_actor_name = "child_actor_name";
  auto child_actor =
      actor1.Task(&Counter::CreateChildActor).Remote(child_actor_name).Get();
  EXPECT_EQ(1, *child_actor->Task(&Counter::Plus1).Remote().Get());

  auto named_actor_handle_optional = ray::GetActor<Counter>(child_actor_name);
  EXPECT_TRUE(named_actor_handle_optional);
  auto &named_actor_handle = *named_actor_handle_optional;
  auto named_actor_obj1 = named_actor_handle.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(2, *named_actor_obj1.Get());
}

TEST(RayClusterModeTest, PythonInvocationTest) {
  ray::Init();
  ray::ActorHandleXlang py_actor_handle =
      ray::Actor(ray::PyActorClass{"test_cross_language_invocation", "Counter"})
          .Remote(1);
  EXPECT_TRUE(!py_actor_handle.ID().empty());

  auto py_actor_ret =
      py_actor_handle.Task(ray::PyActorMethod<std::string>{"increase"}).Remote(1);
  EXPECT_EQ("2", *py_actor_ret.Get());

  auto py_obj =
      ray::Task(ray::PyFunction<int>{"test_cross_language_invocation", "py_return_val"})
          .Remote();
  EXPECT_EQ(42, *py_obj.Get());

  auto py_obj1 =
      ray::Task(ray::PyFunction<int>{"test_cross_language_invocation", "py_return_input"})
          .Remote(42);
  EXPECT_EQ(42, *py_obj1.Get());

  auto py_obj2 = ray::Task(ray::PyFunction<std::string>{"test_cross_language_invocation",
                                                        "py_return_input"})
                     .Remote("hello");
  EXPECT_EQ("hello", *py_obj2.Get());

  Person p{"tom", 20};
  auto py_obj3 = ray::Task(ray::PyFunction<Person>{"test_cross_language_invocation",
                                                   "py_return_input"})
                     .Remote(p);
  auto py_result = *py_obj3.Get();
  EXPECT_EQ(p.age, py_result.age);
  EXPECT_EQ(p.name, py_result.name);
}

TEST(RayClusterModeTest, JavaInvocationTest) {
  // Test java nested static class
  ray::ActorHandleXlang java_nested_class_actor_handle =
      ray::Actor(ray::JavaActorClass{"io.ray.test.Counter$NestedActor"}).Remote("hello");
  EXPECT_TRUE(!java_nested_class_actor_handle.ID().empty());
  auto java_actor_ret =
      java_nested_class_actor_handle.Task(ray::JavaActorMethod<std::string>{"concat"})
          .Remote("world");
  EXPECT_EQ("helloworld", *java_actor_ret.Get());

  // Test java static function
  auto java_task_ret =
      ray::Task(ray::JavaFunction<std::string>{"io.ray.test.CrossLanguageInvocationTest",
                                               "returnInputString"})
          .Remote("helloworld");
  EXPECT_EQ("helloworld", *java_task_ret.Get());

  // Test java normal class
  std::string actor_name = "java_actor";
  auto java_class_actor_handle = ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"})
                                     .SetName(actor_name)
                                     .Remote(0);
  auto ref2 =
      java_class_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref2.Get());

  // Test get java actor by actor name.
  boost::optional<ray::ActorHandleXlang> named_actor_handle_optional =
      ray::GetActor(actor_name);
  EXPECT_TRUE(named_actor_handle_optional);
  ray::ActorHandleXlang named_actor_handle = *named_actor_handle_optional;
  auto named_actor_obj1 =
      named_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *named_actor_obj1.Get());

  std::vector<std::byte> bytes = {std::byte{1}, std::byte{2}, std::byte{3}};
  auto ref_bytes = java_class_actor_handle
                       .Task(ray::JavaActorMethod<std::vector<std::byte>>{"echoBytes"})
                       .Remote(bytes);
  EXPECT_EQ(*ref_bytes.Get(), bytes);

  // Test get other java actor by actor name.
  auto ref_1 =
      java_class_actor_handle.Task(ray::JavaActorMethod<std::string>{"createChildActor"})
          .Remote("child_actor");
  EXPECT_EQ(*ref_1.Get(), "OK");
  boost::optional<ray::ActorHandleXlang> child_actor_optional =
      ray::GetActor("child_actor");
  EXPECT_TRUE(child_actor_optional);
  ray::ActorHandleXlang &child_actor = *child_actor_optional;
  auto ref_2 = child_actor.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref_2.Get());

  auto ref_3 =
      child_actor.Task(ray::JavaActorMethod<std::string>{"echo"}).Remote("C++ worker");
  EXPECT_EQ("C++ worker", *ref_3.Get());

  auto ref_4 = child_actor.Task(ray::JavaActorMethod<std::vector<std::byte>>{"echoBytes"})
                   .Remote(bytes);
  EXPECT_EQ(*ref_4.Get(), bytes);
}

TEST(RayClusterModeTest, GetXLangActorByNameTest) {
  // Create a named java actor in namespace `isolated_ns`.
  std::string actor_name_in_isolated_ns = "xlang_named_actor_in_isolated_ns";
  std::string isolated_ns_name = "xlang_isolated_ns";

  auto java_actor_handle = ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"})
                               .SetName(actor_name_in_isolated_ns, isolated_ns_name)
                               .Remote(0);
  auto ref = java_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());

  // It is invisible to job default namespace.
  boost::optional<ray::ActorHandleXlang> actor_optional =
      ray::GetActor(actor_name_in_isolated_ns);
  EXPECT_TRUE(!actor_optional);
  // It is invisible to any other namespaces.
  actor_optional = ray::GetActor(actor_name_in_isolated_ns, "other_ns");
  EXPECT_TRUE(!actor_optional);
  // It is visible to the namespace it belongs.
  actor_optional = ray::GetActor(actor_name_in_isolated_ns, isolated_ns_name);
  EXPECT_TRUE(actor_optional);
  ref = (*actor_optional).Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());

  // Create a named java actor in job default namespace.
  std::string actor_name_in_default_ns = "xlang_actor_name_in_default_ns";
  java_actor_handle = ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"})
                          .SetName(actor_name_in_default_ns)
                          .Remote(0);
  ref = java_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());
  // It is invisible to any other namespaces.
  actor_optional = ray::GetActor(actor_name_in_default_ns, isolated_ns_name);
  EXPECT_TRUE(!actor_optional);
  // It is visible to job default namespace.
  actor_optional = ray::GetActor(actor_name_in_default_ns);
  EXPECT_TRUE(actor_optional);
  ref = (*actor_optional).Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());
}

TEST(RayClusterModeTest, MaxConcurrentTest) {
  auto actor1 =
      ray::Actor(ActorConcurrentCall::FactoryCreate).SetMaxConcurrency(3).Remote();
  auto object1 = actor1.Task(&ActorConcurrentCall::CountDown).Remote();
  auto object2 = actor1.Task(&ActorConcurrentCall::CountDown).Remote();
  auto object3 = actor1.Task(&ActorConcurrentCall::CountDown).Remote();

  EXPECT_EQ(*object1.Get(), "ok");
  EXPECT_EQ(*object2.Get(), "ok");
  EXPECT_EQ(*object3.Get(), "ok");
}

TEST(RayClusterModeTest, ResourcesManagementTest) {
  auto actor1 =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetResources({{"CPU", 1.0}}).Remote();
  auto r1 = actor1.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(*r1.Get(), 1);

  auto actor2 = ray::Actor(RAY_FUNC(Counter::FactoryCreate))
                    .SetResources({{"CPU", 10000.0}})
                    .Remote();
  auto r2 = actor2.Task(&Counter::Plus1).Remote();
  std::vector<ray::ObjectRef<int>> objects{r2};
  auto result = ray::Wait(objects, 1, 1000);
  EXPECT_EQ(result.ready.size(), 0);
  EXPECT_EQ(result.unready.size(), 1);

  auto r3 = ray::Task(Return1).SetResource("CPU", 1.0).Remote();
  EXPECT_EQ(*r3.Get(), 1);

  auto r4 = ray::Task(Return1).SetResource("CPU", 100.0).Remote();
  std::vector<ray::ObjectRef<int>> objects1{r4};
  auto result2 = ray::Wait(objects1, 1, 1000);
  EXPECT_EQ(result2.ready.size(), 0);
  EXPECT_EQ(result2.unready.size(), 1);
}

TEST(RayClusterModeTest, ExceptionTest) {
  EXPECT_THROW(ray::Task(ThrowTask).Remote().Get(), ray::internal::RayTaskException);
  try {
    ray::Task(ThrowTask).Remote().Get();
  } catch (ray::internal::RayTaskException &e) {
    EXPECT_TRUE(std::string(e.what()).find("std::logic_error") != std::string::npos);
  }
  auto actor1 = ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(1);
  auto object1 = actor1.Task(&Counter::ExceptionFunc).Remote();
  EXPECT_THROW(object1.Get(), ray::internal::RayTaskException);
}

TEST(RayClusterModeTest, GetAllNodeInfoTest) {
  const auto &gcs_client =
      ray::internal::AbstractRayRuntime::GetInstance()->GetGlobalStateAccessor();
  auto all_node_info = gcs_client->GetAllNodeInfo();
  EXPECT_EQ(all_node_info.size(), 1);

  ray::rpc::GcsNodeInfo node_info;
  node_info.ParseFromString(all_node_info[0]);
  EXPECT_EQ(node_info.basic_gcs_node_info().state(), ray::rpc::BasicGcsNodeInfo::ALIVE);
}

bool CheckRefCount(
    std::unordered_map<ray::ObjectID, std::pair<size_t, size_t>> expected) {
  auto object_store = std::make_unique<ray::internal::NativeObjectStore>();
  auto map = object_store->GetAllReferenceCounts();
  return expected == map;
}

TEST(RayClusterModeTest, LocalRefrenceTest) {
  auto r1 = std::make_unique<ray::ObjectRef<int>>(ray::Task(Return1).Remote());
  auto object_id = ray::ObjectID::FromBinary(r1->ID());
  EXPECT_TRUE(CheckRefCount({{object_id, std::make_pair(1, 0)}}));
  auto r2 = std::make_unique<ray::ObjectRef<int>>(*r1);
  EXPECT_TRUE(CheckRefCount({{object_id, std::make_pair(2, 0)}}));
  r1.reset();
  EXPECT_TRUE(CheckRefCount({{object_id, std::make_pair(1, 0)}}));
  r2.reset();
  EXPECT_TRUE(CheckRefCount({}));
}

TEST(RayClusterModeTest, DependencyRefrenceTest) {
  auto r1 = std::make_unique<ray::ObjectRef<int>>(ray::Task(Return1).Remote());
  auto object_id = ray::ObjectID::FromBinary(r1->ID());
  EXPECT_TRUE(CheckRefCount({{object_id, std::make_pair(1, 0)}}));

  auto r2 = std::make_unique<ray::ObjectRef<int>>(ray::Task(Plus1).Remote(*r1));
  EXPECT_TRUE(
      CheckRefCount({{object_id, std::make_pair(1, 1)},
                     {ray::ObjectID::FromBinary(r2->ID()), std::make_pair(1, 0)}}));
  r2->Get();
  EXPECT_TRUE(
      CheckRefCount({{object_id, std::make_pair(1, 0)},
                     {ray::ObjectID::FromBinary(r2->ID()), std::make_pair(1, 0)}}));
  r1.reset();
  r2.reset();
  EXPECT_TRUE(CheckRefCount({}));
}

TEST(RayClusterModeTest, GetActorTest) {
  ray::ActorHandleCpp<Counter> actor = ray::Actor(RAY_FUNC(Counter::FactoryCreate))
                                           .SetMaxRestarts(1)
                                           .SetName("named_actor")
                                           .Remote();
  auto named_actor_obj = actor.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(1, *named_actor_obj.Get());

  auto named_actor_handle_optional = ray::GetActor<Counter>("named_actor");
  EXPECT_TRUE(named_actor_handle_optional);
  auto &named_actor_handle = *named_actor_handle_optional;
  auto named_actor_obj1 = named_actor_handle.Task(&Counter::Plus1).Remote();
  EXPECT_EQ(2, *named_actor_obj1.Get());
  EXPECT_FALSE(ray::GetActor<Counter>("not_exist_actor"));
}

ray::PlacementGroup CreateSimplePlacementGroup(const std::string &name) {
  std::vector<std::unordered_map<std::string, double>> bundles{
      {{"CPU", 1}, {"memory", 50}}};

  ray::PlacementGroupCreationOptionsCpp options{name, bundles,
                                                ray::PlacementStrategyCpp::PACK};
  return ray::CreatePlacementGroup(options);
}

TEST(RayClusterModeTest, CreateAndRemovePlacementGroup) {
  auto first_placement_group = CreateSimplePlacementGroup("first_placement_group");
  EXPECT_TRUE(first_placement_group.Wait(10));
  EXPECT_THROW(CreateSimplePlacementGroup("first_placement_group"),
               ray::internal::RayException);

  auto groups = ray::GetAllPlacementGroups();
  EXPECT_EQ(groups.size(), 1);

  auto placement_group = ray::GetPlacementGroupById(first_placement_group.GetID());
  EXPECT_EQ(placement_group.GetID(), first_placement_group.GetID());

  auto placement_group1 = ray::GetPlacementGroup("first_placement_group");
  EXPECT_EQ(placement_group1.GetID(), first_placement_group.GetID());

  ray::RemovePlacementGroup(first_placement_group.GetID());
  auto deleted_group = ray::GetPlacementGroupById(first_placement_group.GetID());
  EXPECT_EQ(deleted_group.GetState(), ray::PlacementGroupState::REMOVED);

  auto not_exist_group = ray::GetPlacementGroup("not_exist_placement_group");
  EXPECT_TRUE(not_exist_group.GetID().empty());

  ray::RemovePlacementGroup(first_placement_group.GetID());
}

TEST(RayClusterModeTest, CreatePlacementGroupExceedsClusterResource) {
  std::vector<std::unordered_map<std::string, double>> bundles{
      {{"CPU", 10000}, {"memory", 50}}};

  ray::PlacementGroupCreationOptionsCpp options{"first_placement_group", bundles,
                                                ray::PlacementStrategyCpp::PACK};
  auto first_placement_group = ray::CreatePlacementGroup(options);
  EXPECT_FALSE(first_placement_group.Wait(3));
  ray::RemovePlacementGroup(first_placement_group.GetID());
  auto deleted_group = ray::GetPlacementGroupById(first_placement_group.GetID());
  EXPECT_EQ(deleted_group.GetState(), ray::PlacementGroupState::REMOVED);

  auto not_exist_group = ray::GetPlacementGroup("not_exist_placement_group");
  EXPECT_TRUE(not_exist_group.GetID().empty());
}

TEST(RayClusterModeTest, CreateActorWithPlacementGroup) {
  auto placement_group = CreateSimplePlacementGroup("first_placement_group");
  EXPECT_TRUE(placement_group.Wait(10));

  auto actor1 = ray::Actor(RAY_FUNC(Counter::FactoryCreate))
                    .SetResources({{"CPU", 1.0}, {"memory", 20}})
                    .SetPlacementGroup(placement_group, 0)
                    .Remote();
  auto r1 = actor1.Task(&Counter::Plus1).Remote();
  std::vector<ray::ObjectRef<int>> objects{r1};
  auto result = ray::Wait(objects, 1, 1000);
  EXPECT_EQ(result.ready.size(), 1);
  EXPECT_EQ(result.unready.size(), 0);
  auto result_vector = ray::Get(objects);
  EXPECT_EQ(*(result_vector[0]), 1);

  // Exceeds the resources of PlacementGroup.
  auto actor2 = ray::Actor(RAY_FUNC(Counter::FactoryCreate))
                    .SetResources({{"CPU", 2.0}, {"memory", 20}})
                    .SetPlacementGroup(placement_group, 0)
                    .Remote();
  auto r2 = actor2.Task(&Counter::Plus1).Remote();
  std::vector<ray::ObjectRef<int>> objects2{r2};
  auto result2 = ray::Wait(objects2, 1, 1000);
  EXPECT_EQ(result2.ready.size(), 0);
  EXPECT_EQ(result2.unready.size(), 1);
  ray::RemovePlacementGroup(placement_group.GetID());
}

TEST(RayClusterModeTest, NamespaceTest) {
  // Create a named actor in namespace `isolated_ns`.
  std::string actor_name_in_isolated_ns = "named_actor_in_isolated_ns";
  std::string isolated_ns_name = "isolated_ns";
  ray::ActorHandleCpp<Counter> actor =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate))
          .SetName(actor_name_in_isolated_ns, isolated_ns_name)
          .Remote();
  auto initialized_obj = actor.Task(&Counter::Initialized).Remote();
  EXPECT_TRUE(*initialized_obj.Get());
  // It is invisible to job default namespace.
  auto actor_optional = ray::GetActor<Counter>(actor_name_in_isolated_ns);
  EXPECT_TRUE(!actor_optional);
  // It is visible to the namespace it belongs.
  actor_optional = ray::GetActor<Counter>(actor_name_in_isolated_ns, isolated_ns_name);
  EXPECT_TRUE(actor_optional);
  // It is invisible to any other namespaces.
  actor_optional = ray::GetActor<Counter>(actor_name_in_isolated_ns, "other_ns");
  EXPECT_TRUE(!actor_optional);

  // Create a named actor in job default namespace.
  std::string actor_name_in_default_ns = "actor_name_in_default_ns";
  actor = ray::Actor(RAY_FUNC(Counter::FactoryCreate))
              .SetName(actor_name_in_default_ns)
              .Remote();
  initialized_obj = actor.Task(&Counter::Initialized).Remote();
  EXPECT_TRUE(*initialized_obj.Get());
  // It is visible to job default namespace.
  actor_optional = ray::GetActor<Counter>(actor_name_in_default_ns);
  EXPECT_TRUE(actor_optional);
  // It is invisible to any other namespaces.
  actor_optional = ray::GetActor<Counter>(actor_name_in_default_ns, isolated_ns_name);
  EXPECT_TRUE(!actor_optional);
  ray::Shutdown();
}

TEST(RayClusterModeTest, GetNamespaceApiTest) {
  std::string ns = "test_get_current_namespace";
  ray::RayConfigCpp config;
  config.ray_namespace = ns;
  ray::Init(config, cmd_argc, cmd_argv);
  // Get namespace in driver.
  EXPECT_EQ(ray::GetNamespace(), ns);
  // Get namespace in task.
  auto task_ns = ray::Task(GetNamespaceInTask).Remote();
  EXPECT_EQ(*task_ns.Get(), ns);
  // Get namespace in actor.
  auto actor_handle = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto actor_ns = actor_handle.Task(&Counter::GetNamespaceInActor).Remote();
  EXPECT_EQ(*actor_ns.Get(), ns);
}

TEST(RayClusterModeTest, TaskWithPlacementGroup) {
  auto placement_group = CreateSimplePlacementGroup("first_placement_group");
  EXPECT_TRUE(placement_group.Wait(10));

  auto r = ray::Task(Return1)
               .SetResources({{"CPU", 1.0}})
               .SetPlacementGroup(placement_group, 0)
               .Remote();
  EXPECT_EQ(*r.Get(), 1);
  ray::RemovePlacementGroup(placement_group.GetID());
  ray::Shutdown();
}

TEST(RayClusterModeTest, TestJobDataDirAndWorkerEnv) {
  std::string expect_job_data_dir_base =
      boost::filesystem::current_path().string() + "/tmp";
  std::string expect_env_value = "123,:456,";
  nlohmann::json json;
  json[kEnvVarKeyJobDataDirBase] = expect_job_data_dir_base;
  json["MY_ENV"] = expect_env_value;
  ray::RayConfigCpp config;
  config.job_worker_env = json.dump();
  ray::Init(config);
  auto actor = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto job_data_dir = actor.Task(&Counter::GetJobDataDir).Remote().Get();
  auto actor_pid = actor.Task(&Counter::GetPid).Remote().Get();
  std::ostringstream ss;
  ss << *actor_pid;
  std::string expect_job_data_dir = expect_job_data_dir_base + "/" + ss.str();
  EXPECT_EQ(expect_job_data_dir, *job_data_dir);

  auto env_value = actor.Task(&Counter::GetEnv).Remote("MY_ENV").Get();
  EXPECT_EQ(expect_env_value, *env_value);
  ray::Shutdown();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  cmd_argc = argc;
  cmd_argv = argv;
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  ray::Shutdown();

  if (absl::GetFlag(FLAGS_external_cluster)) {
    ray::internal::ProcessHelper::GetInstance().StopRayNode();
  }

  return ret;
}
