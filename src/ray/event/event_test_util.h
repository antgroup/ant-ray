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

#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "src/ray/protobuf/event.pb.h"

namespace ray {

#define PARALLEL_FOR(nthreads, LOOP_END, E, O)                                         \
  {                                                                                    \
    if (nthreads > 1) {                                                                \
      std::vector<std::thread> threads(nthreads);                                      \
      for (int t = 0; t < nthreads; t++) {                                             \
        threads[t] = std::thread(std::bind(                                            \
            [&](const int bi, const int ei, const int t) {                             \
              E;                                                                       \
              for (int loop_i = bi; loop_i < ei; loop_i++) {                           \
                O;                                                                     \
              }                                                                        \
            },                                                                         \
            t * LOOP_END / nthreads,                                                   \
            (t + 1) == nthreads ? LOOP_END : (t + 1) * LOOP_END / nthreads, t));       \
      }                                                                                \
      std::for_each(threads.begin(), threads.end(), [](std::thread &x) { x.join(); }); \
    } else {                                                                           \
      E;                                                                               \
      for (int loop_i = 0; loop_i < LOOP_END; loop_i++) {                              \
        O;                                                                             \
      }                                                                                \
    }                                                                                  \
  }

void CheckEventDetail(rpc::Event &event, std::string job_id, std::string node_id,
                      std::string task_id, std::string source_type, std::string severity,
                      std::string label, std::string message) {
  int custom_key_num = 0;
  auto mp = (*event.mutable_custom_fields());

  if (job_id != "") {
    EXPECT_EQ(mp["job_id"], job_id);
    custom_key_num++;
  }
  if (node_id != "") {
    EXPECT_EQ(mp["node_id"], node_id);
    custom_key_num++;
  }
  if (task_id != "") {
    EXPECT_EQ(mp["task_id"], task_id);
    custom_key_num++;
  }
  EXPECT_EQ(mp.size(), custom_key_num);
  EXPECT_EQ(rpc::Event_SourceType_Name(event.source_type()), source_type);
  EXPECT_EQ(rpc::Event_Severity_Name(event.severity()), severity);

  if (label != "NULL") {
    EXPECT_EQ(event.label(), label);
  }

  if (message != "NULL") {
    EXPECT_EQ(event.message(), message);
  }

  EXPECT_EQ(event.source_pid(), getpid());
}

bool StringContains(std::string seq, std::string pattern) {
  if (seq.find(pattern) != seq.npos) {
    return true;
  } else {
    return false;
  }
}

}  // namespace ray