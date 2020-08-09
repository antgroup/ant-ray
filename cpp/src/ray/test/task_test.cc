//
// Created by qicosmos on 2020/7/31.
//
#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/api/task_helper.h>
#include "absl/meta/type_traits.h"

#include <future>
#include <thread>

//#include "../runtime/task/task_executor.h"

using namespace ray::api;

int bb(int i){
  return i+1;
}

template <typename F, typename... Args>
TaskHelper<absl::result_of_t<F(Args...)>> &&CreateTask(F fn, Args &&... args) {
  using R = absl::result_of_t<F(Args...)>;
  TaskHelper<R> helper("Add", std::forward<Args>(args)...);
  return std::move(helper);
}

#define TASK1(f, ...) CreateTask(f, ##__VA_ARGS__).Remote();

#define TASK(f, ...) CreateTask(f, ##__VA_ARGS__)

TEST(TaskHelper, Init){
  TaskHelper<void> helper("Add");

  auto tp = helper.Unpack<std::tuple<std::string>>();
  EXPECT_EQ(std::get<0>(tp), "Add");
}

TEST(TaskHelper, InitArgs){
  TaskHelper<int> helper("Add", 1, 2);

  auto tp = helper.Unpack<std::tuple<std::string, int, int>>();
  EXPECT_EQ(std::get<0>(tp), "Add");
  EXPECT_EQ(std::get<1>(tp), 1);
  EXPECT_EQ(std::get<2>(tp), 2);
}

TEST(Runtime, Init){
  Ray::Init();
  auto runtime = Ray::Runtime();
  EXPECT_TRUE(runtime);
  auto obj1 = Ray::Put(1);
  auto i1 = obj1.Get();
  EXPECT_EQ(1, *i1);
}

TEST(TaskExecutor, Invoke){
  Ray::Init();

  std::string key = "bb";
  TaskHelper<int> helper("bb", 1);

  ObjectRef<int> task_id = helper.Remote();

  auto r = task_id.Get();
  std::cout<<key<<" result : "<<*r<<'\n';
  EXPECT_EQ(*r, 3);
}

TEST(TaskExecutor, Task){
  Ray::Init();

  {
    auto task_id = TASK(bb, 1).Remote();
    auto r = task_id.Get();
    std::cout<<" result : "<<*r<<'\n';
    EXPECT_EQ(*r, 3);
  }

  {
    auto&& helper = TASK(bb, 1);
    auto task_id = helper.Remote();
    auto r = task_id.Get();
    std::cout<<" result : "<<*r<<'\n';
    EXPECT_EQ(*r, 3);
  }

  {
    ObjectRef<int> task_id = TASK1(bb, 1);

    auto r = task_id.Get();
    std::cout<<" result : "<<*r<<'\n';
    EXPECT_EQ(*r, 3);
  }
}
