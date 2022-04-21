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

#include "ray/common/id.h"
#include "ray/util/util.h"

namespace ray {
namespace core {

namespace {
using ReplyCallback = std::function<void(const Status &status)>;
}

class ObjectBarrier {
 public:
  explicit ObjectBarrier() {}

  void AddReplyCallback(const ObjectID &object_id, ReplyCallback callback) {
    auto it = replying_objects_->find(object_id);
    if (it != replying_objects_->end()) {
      it->second.emplace_back(std::move(callback));
    } else {
      (*replying_objects_)[object_id] = {};
      (*replying_objects_)[object_id].emplace_back(std::move(callback));
    }
  }

  bool AsyncWaitForReplyFinish(const ObjectID &object_id, ReplyCallback callback) {
    auto it = replying_objects_->find(object_id);
    if (it == replying_objects_->end()) return false;
    it->second.emplace_back(std::move(callback));
    return true;
  }

  void RunAllReplyCallback(const ObjectID &object_id, const Status &status) {
    auto it = replying_objects_->find(object_id);
    if (it == replying_objects_->end()) return;
    std::vector<ReplyCallback> cbs = std::move(it->second);
    replying_objects_->erase(it);
    for (auto &cb : cbs) {
      cb(status);
    }
  }

  void ClearReplyCallback(const ObjectID &object_id) {
    auto it = replying_objects_->find(object_id);
    if (it != replying_objects_->end()) return;
    replying_objects_->erase(it);
  }

  bool IsReplyingObject(const ObjectID &object_id) {
    auto it = replying_objects_->find(object_id);
    return it != replying_objects_->end();
  }

 private:
  using ReplyingObjectType = absl::flat_hash_map<ObjectID, std::vector<ReplyCallback> >;
  ThreadPrivate<ReplyingObjectType> replying_objects_;
};

}  // namespace core
}  // namespace ray