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

#include "ray/util/view_point.h"
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/filesystem.hpp>
#include "ray/util/util.h"

#if defined(__linux__)
#include <sys/syscall.h>
#elif defined(__FreeBSD__)
#include <sys/thr.h>
#elif defined(__APPLE__)
#ifndef _MACH_PORT_T
#define _MACH_PORT_T
#include <sys/_types.h> /* __darwin_mach_port_t */
typedef __darwin_mach_port_t mach_port_t;
#include <pthread.h>
extern "C" mach_port_t pthread_mach_thread_np(pthread_t);
#endif /* _MACH_PORT_T */
#endif /* __APPLE__ */

namespace ray {

void time_us_to_string(uint64_t ts_us, char *str) {
  ts_us += 8 * 3600ULL * 1000000ULL;
  auto us = static_cast<uint32_t>(ts_us % 1000000);
  auto t = (time_t)(ts_us / 1000000);

  struct tm tmp;
  auto ret = gmtime_r(&t, &tmp);

  if (ret) {
    sprintf(str, "%02d:%02d:%02d.%06u", ret->tm_hour, ret->tm_min, ret->tm_sec, us);
  } else {
    sprintf(str, "%02d:%02d:%02d.%06u", 0, 0, 0, 0);
  }
}

int get_current_tid() {
#if defined(_WIN32)
  return static_cast<int>(::GetCurrentThreadId());
#elif defined(__linux__)
  // return static_cast<int>(gettid());
  return static_cast<int>(syscall(SYS_gettid));
#elif defined(__FreeBSD__)
  long lwpid;
  thr_self(&lwpid);
  return static_cast<int>(lwpid);
#elif defined(__APPLE__)
  return static_cast<int>(pthread_mach_thread_np(pthread_self()));
#else
#error not implemented yet
#endif
}

ViewPoint::ViewPoint() : start_(0), consumed_(0) {}

void ViewPoint::reset() {
  consumed_ = 0;
  start_ = current_sys_time_ns();
}

void ViewPoint::finish() { consumed_ = elapsed(); }

uint64_t ViewPoint::elapsed() { return (current_sys_time_ns() - start_); }

void ViewPoint::name(const std::string &name) { name_ = name; }

const std::string &ViewPoint::name() { return name_; }

void ViewPoint::put(ViewPoint *follow) { follow_.emplace_back(follow); }

void ViewPoint::gc(std::list<ViewPoint *> &pool) {
  if (follow_.empty()) {
    return;
  }

  for (auto &vp : follow_) {
    vp->gc(pool);
  }

  pool.splice(pool.end(), follow_);
}

void ViewPoint::dump(std::ostream &ostr, uint64_t factor /* = 1*/) {
  std::string prefix;
  dump(ostr, prefix, false, 0, factor);
}

void ViewPoint::dump(std::ostream &ostr, std::string &prefix, bool has_next, int depth,
                     uint64_t factor) {
  int len = 3 * depth;

  char timestamp[20] = {0};
  time_us_to_string(start_ / 1000, timestamp);

  const char *unit =
      factor == 1
          ? "ns"
          : factor == 1000 ? "us"
                           : factor == 1000000 ? "ms" : factor == 1000000000 ? "s" : "xx";
  ostr.write(prefix.data(), len) << "---" << name_ << " " << timestamp << " [ "
                                 << consumed_ / factor << " " << unit << " ]\n";

  if (!follow_.empty()) {
    if (!has_next && len > 0) {
      prefix[len - 1] = ' ';
    }

    prefix.resize(len + 3, ' ');
    prefix[len + 2] = '|';
  }

  for (auto iter = follow_.begin(); iter != follow_.end();) {
    auto cur = iter++;
    (*cur)->dump(ostr, prefix, (iter != follow_.end()), depth + 1, factor);
  }
  prefix.resize(len);
}

/****************************************************************************************/
thread_local std::list<ViewPoint *> ViewPointManager::pool_;
thread_local std::stack<ViewPoint *> ViewPointManager::stack_;
thread_local std::ofstream ViewPointManager::ostr_;

ViewPointManager::ViewPointManager() {
  // TODO(Shanly): load from configuration.
}

ViewPointManager::~ViewPointManager() {}

ViewPointManager &ViewPointManager::instance() {
  static ViewPointManager view_point_manager;
  return view_point_manager;
}

ViewPointWrapper<ViewPoint> ViewPointManager::newViewPoint(std::string &&point) {
  ViewPoint *vp = nullptr;
  if (pool_.empty()) {
    vp = new ViewPoint;
  } else {
    vp = pool_.back();
    pool_.pop_back();
  }

  vp->name(std::move(point));

  stack_.push(vp);
  return ViewPointWrapper<ViewPoint>(vp, [this](ViewPoint *vp) {
    vp->finish();
    stack_.pop();

    if (!stack_.empty()) {
      stack_.top()->put(vp);
    } else {
      if (!ostr_.is_open()) {
        if (!boost::filesystem::exists(log_dir_) &&
            !boost::filesystem::create_directory(log_dir_)) {
          std::cerr << "failed to create view poin log dir " << log_dir_
                    << ", just exit ~~" << std::endl;
          exit(1);
        }

        std::string file_name(log_dir_);
        const int thread_name_length = 64;
        char thread_name[thread_name_length] = {0};
        pthread_getname_np(pthread_self(), thread_name, thread_name_length);
        file_name.append("/ray_view_point_")
            .append(std::to_string(getpid()))
            .append("_")
            .append(std::to_string(get_current_tid()))
            .append("_")
            .append(boost::trim_copy(std::string(thread_name)));
        ostr_.open(file_name);
      }

      if (vp->consumed() >= minimum_consumed_vp_to_print_us_ * unit_ratio_) {
        vp->dump(ostr_, unit_ratio_);
        if (fast_flush_) {
          ostr_.flush();
        }
      }
      vp->gc(pool_);
      pool_.push_back(vp);
    }
  });
}
}  // namespace ray
