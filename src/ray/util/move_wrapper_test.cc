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

#include "ray/util/move_wrapper.h"
#include <gtest/gtest.h>
#include <functional>
#include <memory>

using namespace ray;

TEST(MoveWrapper, CopyAndMove) {
  std::shared_ptr<int> p = std::make_shared<int>(1);

  MoveWrapper<std::shared_ptr<int>> wrapper(std::move(p));
  EXPECT_EQ(p, nullptr);

  MoveWrapper<std::shared_ptr<int>> wrapper1 = wrapper;
  EXPECT_EQ(*wrapper, nullptr);
  EXPECT_EQ(*(*wrapper1), 1);

  MoveWrapper<std::shared_ptr<int>> wrapper2(wrapper1);
  EXPECT_EQ(*wrapper1, nullptr);
  EXPECT_EQ(*(*wrapper2), 1);

  MoveWrapper<std::shared_ptr<int>> wrapper3(std::move(wrapper2));
  EXPECT_EQ(*wrapper2, nullptr);
  EXPECT_EQ(*(wrapper3.move()), 1);

  MoveWrapper<std::shared_ptr<int>> wrapper4 = std::move(wrapper3);
  EXPECT_EQ(*wrapper3, nullptr);
  EXPECT_EQ(*(wrapper4.move()), 1);
}

TEST(MoveWrapper, MoveCapture) {
  {
    std::shared_ptr<int> p = std::make_shared<int>(1);

    auto wrapper = MakeMoveWrapper(std::move(p));
    auto l = [wrapper]() { return *(*wrapper); };

    EXPECT_EQ(l(), 1);
  }

  {
    std::shared_ptr<int> p = std::make_shared<int>(1);

    auto wrapper = MakeMoveWrapper(std::move(p));
    auto l = [wrapper]() mutable { return *(wrapper.move()); };

    EXPECT_EQ(l(), 1);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
