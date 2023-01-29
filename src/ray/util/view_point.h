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

#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <ostream>
#include <stack>
#include <string>

namespace ray {

class ViewPoint {
 public:
  ViewPoint();

 public:
  void reset();
  void finish();
  uint64_t elapsed();

  const std::string &name();
  void name(const std::string &name);

  void put(ViewPoint *follow);
  void dump(std::ostream &ostr, uint64_t factor = 1);
  void gc(std::list<ViewPoint *> &pool);

  uint64_t consumed() const { return consumed_; }

 private:
  void dump(std::ostream &ostr, std::string &prefix, bool has_next, int depth,
            uint64_t factor);

 private:
  std::string name_;

  uint64_t start_;
  uint64_t consumed_;

  std::list<ViewPoint *> follow_;
};

template <typename T>
class ViewPointWrapper {
 public:
  ViewPointWrapper(ViewPointWrapper &&wrapper) {
    vp_ = wrapper.vp_;
    deleter_ = std::move(wrapper.deleter_);

    wrapper.vp_ = nullptr;
    wrapper.deleter_ = nullptr;
  }

  ViewPointWrapper(T *vp, std::function<void(T *)> &&deleter) {
    vp_ = vp;
    deleter_ = std::move(deleter);
  }

  ~ViewPointWrapper() {
    if (deleter_) {
      deleter_(vp_);
    }
  }

  T *operator->() const { return vp_; }

 private:
  ViewPointWrapper(const ViewPointWrapper &);
  ViewPointWrapper &operator=(const ViewPointWrapper &);

 private:
  T *vp_;
  std::function<void(T *)> deleter_;
};

class ViewPointManager {
 public:
  ViewPointManager();
  ~ViewPointManager();

 public:
  static ViewPointManager &instance();
  ViewPointWrapper<ViewPoint> newViewPoint(std::string &&point);

 private:
  bool fast_flush_ = false;
  uint64_t minimum_consumed_vp_to_print_us_ = 1000;
  // ns -- 1
  // us -- 1000
  // ms -- 1000000
  //  s -- 1000000000
  uint64_t unit_ratio_ = 1000;
  std::string log_dir_ = "/tmp";

  thread_local static std::list<ViewPoint *> pool_;
  thread_local static std::stack<ViewPoint *> stack_;
  thread_local static std::ofstream ostr_;
};

}  // namespace ray

#define ENABLE_VIEW_POINT
#ifdef ENABLE_VIEW_POINT

#define __SPLITE__(X, Y) X##Y
#define __DECLARE_NAME__(X, Y) __SPLITE__(X, Y)
#define LINE_NO_BASED_VAR __DECLARE_NAME__(var, __LINE__)

#define INSTALL_VIEW_POINT(V)                                                 \
  auto LINE_NO_BASED_VAR = ray::ViewPointManager::instance().newViewPoint(V); \
  LINE_NO_BASED_VAR->reset();

#else

#define INSTALL_VIEW_POINT(V)

#endif
