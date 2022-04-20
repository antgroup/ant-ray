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
  explicit ObjectBarrier(
      std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool)
      : core_worker_client_pool_(std::move(core_worker_client_pool)) {}

  void PendingAssignOwnerRequest(const ObjectID &object_id,
                                 const rpc::Address &owner_address,
                                 const rpc::Address &current_address,
                                 const std::vector<ObjectID> &contained_object_ids,
                                 const std::string &current_call_site,
                                 const size_t data_size,
                                 ReplyCallback callback) {
    rpc::WorkerAddress owner_worker_address(owner_address);
    auto it = batch_assign_owner_requests_->find(owner_worker_address);
    size_t pending_object_number = 0;
    if (it == batch_assign_owner_requests_->end()) {
      rpc::BatchAssignObjectOwnerRequest request;
      request.add_batch_object_ids(object_id.Binary());
      request.add_batch_object_sizes(data_size);
      request.add_contained_object_numbers(contained_object_ids.size());
      request.mutable_borrower_address()->CopyFrom(current_address);
      request.set_call_site(current_call_site);
      for (auto &contained_object_id : contained_object_ids) {
        request.add_total_contained_object_ids(contained_object_id.Binary());
      }

      absl::flat_hash_set<ObjectID> objects_set = {object_id};
      (*batch_assign_owner_requests_)[owner_worker_address] =
          std::make_pair(request, objects_set);

      pending_object_number = 1;
    } else {
      it->second.first.add_batch_object_ids(object_id.Binary());
      it->second.first.add_batch_object_sizes(data_size);
      it->second.first.add_contained_object_numbers(contained_object_ids.size());
      for (auto &contained_object_id : contained_object_ids) {
        it->second.first.add_total_contained_object_ids(contained_object_id.Binary());
      }
      RAY_CHECK(it->second.second.insert(object_id).second);

      pending_object_number = it->second.second.size();
    }
    AddReplyCallback(object_id, std::move(callback));

    if (pending_object_number >= ::RayConfig::instance().batch_size_of_assign_owner())
      BatchAssignObjectOwner(owner_address);
  }

  void BatchAssignObjectOwner(const rpc::Address &owner_address) {
    rpc::WorkerAddress owner_worker_address(owner_address);
    auto it = batch_assign_owner_requests_->find(owner_worker_address);
    RAY_CHECK(it != batch_assign_owner_requests_->end());

    std::shared_ptr<std::vector<ObjectID> > object_ids =
        std::make_shared<std::vector<ObjectID> >();
    for (const auto &object_id_binary : it->second.first.batch_object_ids()) {
      object_ids->emplace_back(ObjectID::FromBinary(object_id_binary));
    }

    auto conn = core_worker_client_pool_->GetOrConnect(owner_address);
    conn->BatchAssignObjectOwner(
        it->second.first,
        [this, object_ids = std::move(object_ids)](
            const Status &status, const rpc::BatchAssignObjectOwnerReply &reply) {
          for (const auto &object_id : *object_ids) {
            RunAllReplyCallback(object_id, status);
          }
        });
  }

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

  bool IsPendingAssignOwnerRequest(const ObjectID &object_id,
                                   const rpc::Address &owner_address) {
    rpc::WorkerAddress owner_worker_address(owner_address);

    auto pending_request_it = batch_assign_owner_requests_->find(owner_worker_address);
    if (pending_request_it == batch_assign_owner_requests_->end()) return false;

    return pending_request_it->second.second.count(object_id);
  }

 private:
  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;
  using ReplyingObjectType = absl::flat_hash_map<ObjectID, std::vector<ReplyCallback> >;
  using BatchAssignOwnerRequestsType = absl::flat_hash_map<
      rpc::WorkerAddress,
      std::pair<rpc::BatchAssignObjectOwnerRequest, absl::flat_hash_set<ObjectID> > >;
  ThreadPrivate<ReplyingObjectType> replying_objects_;
  ThreadPrivate<BatchAssignOwnerRequestsType> batch_assign_owner_requests_;
};

}  // namespace core
}  // namespace ray