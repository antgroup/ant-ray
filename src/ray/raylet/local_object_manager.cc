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
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/raylet/local_object_manager.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet {

void LocalObjectManager::PinObjects(const std::vector<ObjectID> &object_ids,
                                    std::vector<std::unique_ptr<RayObject>> &&objects,
                                    const rpc::Address &owner_address) {
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    auto &object = objects[i];
    if (object == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }
    RAY_LOG(DEBUG) << "Pinning object " << object_id;
    pinned_objects_size_ += object->GetSize();
    pinned_objects_.emplace(object_id, std::make_pair(std::move(object), owner_address));
  }
}

void LocalObjectManager::WaitForObjectFree(const rpc::Address &owner_address,
                                           const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    // Send a subscription message.
    rpc::Address subscriber_address;
    subscriber_address.set_raylet_id(self_node_id_.Binary());
    subscriber_address.set_ip_address(self_node_address_);
    subscriber_address.set_port(self_node_port_);
    auto owner_client = owner_client_pool_.GetOrConnect(owner_address);
    rpc::SubscribeForObjectEvictionRequest wait_request;
    wait_request.set_object_id(object_id.Binary());
    wait_request.set_intended_worker_id(owner_address.worker_id());
    wait_request.mutable_subscriber_address()->CopyFrom(subscriber_address);
    owner_client->SubscribeForObjectEviction(
        wait_request,
        [this, owner_address, object_id](
            Status status, const rpc::SubscribeForObjectEvictionReply &reply) {
          if (!status.ok()) {
            RAY_LOG(DEBUG)
                << "Subscription request to Evicted objects have failed. Object id:"
                << object_id << " status:" << status.ToString();
            ReleaseFreedObject(object_id);
            return;
          }

          // If the subscription succeeds, register the subscription callback.
          // Callback that is invoked when the owner publishes the object to evict.
          auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
            RAY_CHECK(msg.has_worker_object_eviction_message());
            const auto object_eviction_msg = msg.worker_object_eviction_message();
            const auto object_id = ObjectID::FromBinary(object_eviction_msg.object_id());
            ReleaseFreedObject(object_id);
            core_worker_subscriber_->Unsubscribe(rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                                 owner_address, object_id.Binary());
          };

          // Callback that is invoked when the owner of the object id is dead.
          auto owner_dead_callback = [this, object_id]() {
            ReleaseFreedObject(object_id);
          };
          core_worker_subscriber_->Subscribe(rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                             owner_address, object_id.Binary(),
                                             subscription_callback, owner_dead_callback);
        });
  }
}

void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "Unpinning object " << object_id;
  // The object should be in one of these stats. pinned, spilling, or spilled.
  RAY_CHECK((pinned_objects_.count(object_id) > 0) ||
            (spilled_objects_url_.count(object_id) > 0) ||
            (objects_pending_spill_.count(object_id) > 0));
  if (automatic_object_deletion_enabled_) {
    spilled_object_pending_delete_.push(object_id);
  }
  if (pinned_objects_.count(object_id)) {
    pinned_objects_size_ -= pinned_objects_[object_id].first->GetSize();
    pinned_objects_.erase(object_id);
  }

  // Try to evict all copies of the object from the cluster.
  if (free_objects_period_ms_ >= 0) {
    objects_to_free_.push_back(object_id);
  }
  if (objects_to_free_.size() == free_objects_batch_size_ ||
      free_objects_period_ms_ == 0) {
    FlushFreeObjects();
  }
}

void LocalObjectManager::FlushFreeObjects() {
  if (!objects_to_free_.empty()) {
    RAY_LOG(DEBUG) << "Freeing " << objects_to_free_.size() << " out-of-scope objects";
    on_objects_freed_(objects_to_free_);
    objects_to_free_.clear();
  }
  if (automatic_object_deletion_enabled_) {
    // Deletion wouldn't work when the object pinning is not enabled.
    ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_);
  }
  last_free_objects_at_ms_ = current_time_ms();
}

void LocalObjectManager::SpillObjectUptoMaxThroughput() {
  if (RayConfig::instance().object_spilling_config().empty() ||
      !RayConfig::instance().automatic_object_spilling_enabled()) {
    return;
  }

  // Spill as fast as we can using all our spill workers.
  bool can_spill_more = true;
  while (can_spill_more) {
    if (!SpillObjectsOfSize(min_spilling_size_)) {
      break;
    }
    {
      absl::MutexLock lock(&mutex_);
      num_active_workers_ += 1;
      can_spill_more = num_active_workers_ < max_active_workers_;
    }
  }
}

bool LocalObjectManager::IsSpillingInProgress() {
  absl::MutexLock lock(&mutex_);
  return num_active_workers_ > 0;
}

bool LocalObjectManager::SpillObjectsOfSize(int64_t num_bytes_to_spill) {
  if (RayConfig::instance().object_spilling_config().empty() ||
      !RayConfig::instance().automatic_object_spilling_enabled()) {
    return false;
  }

  RAY_LOG(DEBUG) << "Choosing objects to spill of total size " << num_bytes_to_spill;
  int64_t bytes_to_spill = 0;
  auto it = pinned_objects_.begin();
  std::vector<ObjectID> objects_to_spill;
  int64_t counts = 0;
  while (bytes_to_spill <= num_bytes_to_spill && it != pinned_objects_.end() &&
         counts < max_fused_object_count_) {
    if (is_plasma_object_spillable_(it->first)) {
      bytes_to_spill += it->second.first->GetSize();
      objects_to_spill.push_back(it->first);
    }
    it++;
    counts += 1;
  }
  if (!objects_to_spill.empty()) {
    RAY_LOG(DEBUG) << "Spilling objects of total size " << bytes_to_spill
                   << " num objects " << objects_to_spill.size();
    auto start_time = absl::GetCurrentTimeNanos();
    SpillObjectsInternal(objects_to_spill, [this, bytes_to_spill, objects_to_spill,
                                            start_time](const Status &status) {
      if (!status.ok()) {
        RAY_LOG(INFO) << "Failed to spill objects: " << status.ToString();
      } else {
        auto now = absl::GetCurrentTimeNanos();
        RAY_LOG(DEBUG) << "Spilled " << bytes_to_spill << " bytes in "
                       << (now - start_time) / 1e6 << "ms";
        spilled_bytes_total_ += bytes_to_spill;
        spilled_objects_total_ += objects_to_spill.size();
        // Adjust throughput timing to account for concurrent spill operations.
        spill_time_total_s_ += (now - std::max(start_time, last_spill_finish_ns_)) / 1e9;
        if (now - last_spill_log_ns_ > 1e9) {
          last_spill_log_ns_ = now;
          RAY_LOG(INFO) << "Spilled "
                        << static_cast<int>(spilled_bytes_total_ / (1024 * 1024))
                        << " MiB, " << spilled_objects_total_
                        << " objects, write throughput "
                        << static_cast<int>(spilled_bytes_total_ / (1024 * 1024) /
                                            spill_time_total_s_)
                        << " MiB/s";
        }
        last_spill_finish_ns_ = now;
      }
    });
    return true;
  }
  return false;
}

void LocalObjectManager::SpillObjects(const std::vector<ObjectID> &object_ids,
                                      std::function<void(const ray::Status &)> callback) {
  SpillObjectsInternal(object_ids, callback);
}

void LocalObjectManager::SpillObjectsInternal(
    const std::vector<ObjectID> &object_ids,
    std::function<void(const ray::Status &)> callback) {
  std::vector<ObjectID> objects_to_spill;
  // Filter for the objects that can be spilled.
  for (const auto &id : object_ids) {
    // We should not spill an object that we are not the primary copy for, or
    // objects that are already being spilled.
    if (pinned_objects_.count(id) == 0 && objects_pending_spill_.count(id) == 0) {
      if (callback) {
        callback(
            Status::Invalid("Requested spill for object that is not marked as "
                            "the primary copy."));
      }
      return;
    }

    // Add objects that we are the primary copy for, and that we are not
    // already spilling.
    auto it = pinned_objects_.find(id);
    if (it != pinned_objects_.end()) {
      RAY_LOG(DEBUG) << "Spilling object " << id;
      objects_to_spill.push_back(id);

      // Move a pinned object to the pending spill object.
      auto object_size = it->second.first->GetSize();
      num_bytes_pending_spill_ += object_size;
      objects_pending_spill_[id] = std::move(it->second);

      pinned_objects_size_ -= object_size;
      pinned_objects_.erase(it);
    }
  }

  if (objects_to_spill.empty()) {
    if (callback) {
      callback(Status::Invalid("All objects are already being spilled."));
    }
    return;
  }
  io_worker_pool_.PopSpillWorker(
      [this, objects_to_spill, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::SpillObjectsRequest request;
        for (const auto &object_id : objects_to_spill) {
          RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
          request.add_object_ids_to_spill(object_id.Binary());
          auto it = objects_pending_spill_.find(object_id);
          RAY_CHECK(it != objects_pending_spill_.end());
          request.add_owner_addresses()->MergeFrom(it->second.second);
        }
        io_worker->rpc_client()->SpillObjects(
            request, [this, objects_to_spill, callback, io_worker](
                         const ray::Status &status, const rpc::SpillObjectsReply &r) {
              {
                absl::MutexLock lock(&mutex_);
                num_active_workers_ -= 1;
              }
              io_worker_pool_.PushSpillWorker(io_worker);
              size_t num_objects_spilled = status.ok() ? r.spilled_objects_url_size() : 0;
              // Object spilling is always done in the order of the request.
              // For example, if an object succeeded, it'll guarentee that all objects
              // before this will succeed.
              RAY_CHECK(num_objects_spilled <= objects_to_spill.size());
              for (size_t i = num_objects_spilled; i != objects_to_spill.size(); ++i) {
                const auto &object_id = objects_to_spill[i];
                auto it = objects_pending_spill_.find(object_id);
                RAY_CHECK(it != objects_pending_spill_.end());
                pinned_objects_size_ += it->second.first->GetSize();
                pinned_objects_.emplace(object_id, std::move(it->second));
                objects_pending_spill_.erase(it);
              }

              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send object spilling request: "
                               << status.ToString();
                if (callback) {
                  callback(status);
                }
              } else {
                AddSpilledUrls(objects_to_spill, r, callback);
              }
            });
      });

  ray::stats::PlasmaSpilledObjectCount().Record(objects_to_spill.size());
}

void LocalObjectManager::UnpinSpilledObjectCallback(
    const ObjectID &object_id, const std::string &object_url,
    std::shared_ptr<size_t> num_remaining,
    std::function<void(const ray::Status &)> callback, ray::Status status) {
  if (!status.ok()) {
    RAY_LOG(INFO) << "Failed to send spilled url for object " << object_id
                  << " to object directory, considering the object to have been freed: "
                  << status.ToString();
  } else {
    RAY_LOG(DEBUG) << "Object " << object_id << " spilled to " << object_url
                   << " and object directory has been informed";
  }
  RAY_LOG(DEBUG) << "Unpinning pending spill object " << object_id;
  // Unpin the object.
  auto it = objects_pending_spill_.find(object_id);
  RAY_CHECK(it != objects_pending_spill_.end());
  num_bytes_pending_spill_ -= it->second.first->GetSize();
  objects_pending_spill_.erase(it);

  // Update the object_id -> url_ref_count to use it for deletion later.
  // We need to track the references here because a single file can contain
  // multiple objects, and we shouldn't delete the file until
  // all the objects are gone out of scope.
  // object_url is equivalent to url_with_offset.
  auto parsed_url = ParseURL(object_url);
  const auto base_url_it = parsed_url->find("url");
  RAY_CHECK(base_url_it != parsed_url->end());
  if (!url_ref_count_.contains(base_url_it->second)) {
    url_ref_count_[base_url_it->second] = 1;
  } else {
    url_ref_count_[base_url_it->second] += 1;
  }

  (*num_remaining)--;
  if (*num_remaining == 0 && callback) {
    callback(status);
  }
}

void LocalObjectManager::AddSpilledUrls(
    const std::vector<ObjectID> &object_ids, const rpc::SpillObjectsReply &worker_reply,
    std::function<void(const ray::Status &)> callback) {
  auto num_remaining = std::make_shared<size_t>(object_ids.size());
  for (size_t i = 0; i < static_cast<size_t>(worker_reply.spilled_objects_url_size());
       ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.spilled_objects_url(i);
    RAY_LOG(DEBUG) << "Object " << object_id << " spilled at " << object_url;
    // Choose a node id to report. If an external storage type is not a filesystem, we
    // don't need to report where this object is spilled.
    const auto node_id_object_spilled =
        is_external_storage_type_fs_ ? self_node_id_ : NodeID::Nil();

    auto it = objects_pending_spill_.find(object_id);
    RAY_CHECK(it != objects_pending_spill_.end());

    // There are times that restore request comes before the url is added to the object
    // directory. By adding the spilled url "before" adding it to the object directory, we
    // can process the restore request before object directory replies.
    spilled_objects_url_.emplace(object_id, object_url);
    auto unpin_callback =
        std::bind(&LocalObjectManager::UnpinSpilledObjectCallback, this, object_id,
                  object_url, num_remaining, callback, std::placeholders::_1);
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      // TODO(Clark): Don't send RPC to owner if we're fulfilling an owner-initiated
      // spill RPC.
      rpc::AddSpilledUrlRequest request;
      request.set_object_id(object_id.Binary());
      request.set_spilled_url(object_url);
      request.set_spilled_node_id(node_id_object_spilled.Binary());
      request.set_size(it->second.first->GetSize());

      auto owner_client = owner_client_pool_.GetOrConnect(it->second.second);
      RAY_LOG(DEBUG) << "Sending spilled URL " << object_url << " for object "
                     << object_id << " to owner "
                     << WorkerID::FromBinary(it->second.second.worker_id());
      // Send spilled URL, spilled node ID, and object size to owner.
      owner_client->AddSpilledUrl(
          request, [unpin_callback](Status status, const rpc::AddSpilledUrlReply &reply) {
            unpin_callback(status);
          });
    } else {
      // Write to object directory. Wait for the write to finish before
      // releasing the object to make sure that the spilled object can
      // be retrieved by other raylets.
      RAY_CHECK_OK(object_info_accessor_.AsyncAddSpilledUrl(
          object_id, object_url, node_id_object_spilled, it->second.first->GetSize(),
          unpin_callback));
    }
  }
}

std::string LocalObjectManager::GetSpilledObjectURL(const ObjectID &object_id) {
  auto entry = spilled_objects_url_.find(object_id);
  if (entry != spilled_objects_url_.end()) {
    return entry->second;
  } else {
    return "";
  }
}

std::string LocalObjectManager::GetDumpedObjectURL(const ObjectID &object_id) {
  auto entry = dumped_objects_url_.find(object_id);
  if (entry != dumped_objects_url_.end()) {
    return entry->second;
  } else {
    return "";
  }
}

void LocalObjectManager::AsyncRestoreSpilledObject(
    const ObjectID &object_id, const std::string &object_url,
    std::function<void(const ray::Status &)> callback) {
  if (objects_pending_restore_.count(object_id) > 0) {
    // If the same object is restoring, we dedup here.
    return;
  }

  RAY_CHECK(objects_pending_restore_.emplace(object_id).second)
      << "Object dedupe wasn't done properly. Please report if you see this issue.";
  io_worker_pool_.PopRestoreWorker([this, object_id, object_url, callback](
                                       std::shared_ptr<WorkerInterface> io_worker) {
    auto start_time = absl::GetCurrentTimeNanos();
    RAY_LOG(DEBUG) << "Sending restore spilled object request";
    rpc::RestoreSpilledObjectsRequest request;
    request.add_spilled_objects_url(std::move(object_url));
    request.add_object_ids_to_restore(object_id.Binary());
    io_worker->rpc_client()->RestoreSpilledObjects(
        request,
        [this, start_time, object_id, callback, io_worker](
            const ray::Status &status, const rpc::RestoreSpilledObjectsReply &r) {
          io_worker_pool_.PushRestoreWorker(io_worker);
          objects_pending_restore_.erase(object_id);
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send restore spilled object request: "
                           << status.ToString();
          } else {
            auto now = absl::GetCurrentTimeNanos();
            auto restored_bytes = r.bytes_restored_total();
            RAY_LOG(DEBUG) << "Restored " << restored_bytes << " in "
                           << (now - start_time) / 1e6 << "ms. Object id:" << object_id;
            restored_bytes_total_ += restored_bytes;
            restored_objects_total_ += 1;
            // Adjust throughput timing to account for concurrent restore operations.
            restore_time_total_s_ +=
                (now - std::max(start_time, last_restore_finish_ns_)) / 1e9;
            if (now - last_restore_log_ns_ > 1e9) {
              last_restore_log_ns_ = now;
              RAY_LOG(INFO) << "Restored "
                            << static_cast<int>(restored_bytes_total_ / (1024 * 1024))
                            << " MiB, " << restored_objects_total_
                            << " objects, read throughput "
                            << static_cast<int>(restored_bytes_total_ / (1024 * 1024) /
                                                restore_time_total_s_)
                            << " MiB/s";
            }
            last_restore_finish_ns_ = now;
          }
          if (callback) {
            callback(status);
          }
        });
  });
}

void LocalObjectManager::ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size) {
  std::vector<std::string> object_urls_to_delete;

  // Process upto batch size of objects to delete.
  while (!spilled_object_pending_delete_.empty() &&
         object_urls_to_delete.size() < max_batch_size) {
    auto &object_id = spilled_object_pending_delete_.front();
    // If the object is still spilling, do nothing. This will block other entries to be
    // processed, but it should be fine because the spilling will be eventually done,
    // and deleting objects is the low priority tasks. This will instead enable simpler
    // logic after this block.
    if (objects_pending_spill_.contains(object_id)) {
      break;
    }

    // Object id is either spilled or not spilled at this point.
    const auto spilled_objects_url_it = spilled_objects_url_.find(object_id);
    if (spilled_objects_url_it != spilled_objects_url_.end()) {
      // If the object was spilled, see if we can delete it. We should first check the
      // ref count.
      std::string &object_url = spilled_objects_url_it->second;
      // Note that here, we need to parse the object url to obtain the base_url.
      auto parsed_url = ParseURL(object_url);
      const auto base_url_it = parsed_url->find("url");
      RAY_CHECK(base_url_it != parsed_url->end());
      const auto &url_ref_count_it = url_ref_count_.find(base_url_it->second);
      RAY_CHECK(url_ref_count_it != url_ref_count_.end())
          << "url_ref_count_ should exist when spilled_objects_url_ exists. Please "
             "submit a Github issue if you see this error.";
      url_ref_count_it->second -= 1;

      // If there's no more refs, delete the object.
      if (url_ref_count_it->second == 0) {
        url_ref_count_.erase(url_ref_count_it);
        object_urls_to_delete.emplace_back(object_url);
      }
      spilled_objects_url_.erase(spilled_objects_url_it);
    }
    spilled_object_pending_delete_.pop();
  }
  if (object_urls_to_delete.size() > 0) {
    DeleteSpilledObjects(object_urls_to_delete);
  }
}

void LocalObjectManager::DeleteSpilledObjects(std::vector<std::string> &urls_to_delete) {
  io_worker_pool_.PopDeleteWorker(
      [this, urls_to_delete](std::shared_ptr<WorkerInterface> io_worker) {
        RAY_LOG(DEBUG) << "Sending delete spilled object request. Length: "
                       << urls_to_delete.size();
        rpc::DeleteSpilledObjectsRequest request;
        for (const auto &url : urls_to_delete) {
          request.add_spilled_objects_url(std::move(url));
        }
        io_worker->rpc_client()->DeleteSpilledObjects(
            request, [this, io_worker](const ray::Status &status,
                                       const rpc::DeleteSpilledObjectsReply &reply) {
              io_worker_pool_.PushDeleteWorker(io_worker);
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send delete spilled object request: "
                               << status.ToString();
              }
            });
      });
}

void LocalObjectManager::FillObjectSpillingStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_spill_time_total_s(spill_time_total_s_);
  stats->set_spilled_bytes_total(spilled_bytes_total_);
  stats->set_spilled_objects_total(spilled_objects_total_);
  stats->set_restore_time_total_s(restore_time_total_s_);
  stats->set_restored_bytes_total(restored_bytes_total_);
  stats->set_restored_objects_total(restored_objects_total_);
}

void LocalObjectManager::RecordObjectSpillingStats() const {
  if (spilled_bytes_total_ != 0 && spill_time_total_s_ != 0) {
    stats::SpillingBandwidthMB.Record(spilled_bytes_total_ / 1024 / 1024 /
                                      spill_time_total_s_);
  }
  if (restored_bytes_total_ != 0 && restore_time_total_s_ != 0) {
    stats::RestoringBandwidthMB.Record(restored_bytes_total_ / 1024 / 1024 /
                                       restore_time_total_s_);
  }
}

void LocalObjectManager::DumpObjects(const std::vector<ObjectID> &object_ids,
                                     const std::vector<uint64_t> &object_sizes,
                                     const rpc::Address &owner_address,
                                     std::function<void(const ray::Status &)> callback) {
  // A subset of objects_pending_dump_ that maintains objects in the current request.
  std::vector<ObjectID> objects_to_dump;
  // Filter for the objects that can be dumped.
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    const uint64_t object_size = object_sizes[i];
    // std::unique_ptr<RayObject> &object = objects[i];

    // Only dump objects that are neither in the dumping process nor already dumped.
    if (objects_pending_dump_.count(object_id) == 0 &&
        dumped_objects_url_.count(object_id) == 0) {
      RAY_LOG(DEBUG) << "Dumping object " << object_id;
      objects_to_dump.push_back(object_id);

      // Move a pinned object to the pending dump object.
      num_bytes_pending_dump_ += object_size;
      objects_pending_dump_[object_id] = std::make_pair(object_size, owner_address);
    }
  }

  if (objects_to_dump.empty()) {
    if (callback) {
      callback(Status::Invalid("All objects are already being dumped."));
    }
    return;
  }
  io_worker_pool_.PopDumpWorker(
      [this, objects_to_dump, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::DumpObjectsRequest request;
        for (const auto &object_id : objects_to_dump) {
          RAY_LOG(DEBUG) << "Sending dump request for object " << object_id;
          request.add_object_ids_to_dump(object_id.Binary());
          auto it = objects_pending_dump_.find(object_id);
          RAY_CHECK(it != objects_pending_dump_.end());
          request.add_owner_addresses()->MergeFrom(it->second.second);
        }
        // In raylet, the `io_worker` refers to the I/O worker process.
        // Here the `rpc_client` is used to send a dump rpc request from
        // current core_worker, i.e. raylet's core_worker, to the core_worker
        // of the io worker.
        io_worker->rpc_client()->DumpObjects(
            request, [this, objects_to_dump, callback, io_worker](
                         const ray::Status &status, const rpc::DumpObjectsReply &r) {
              {
                absl::MutexLock lock(&mutex_);
                num_active_workers_ -= 1;
              }
              io_worker_pool_.PushDumpWorker(io_worker);
              size_t num_objects_dumped = status.ok() ? r.dumped_objects_url_size() : 0;
              // Object dumping is always done in the order of the request.
              // For example, if an object is successfull dumped, the process guarentees
              // that all objects before it have been dumped.
              RAY_CHECK(num_objects_dumped <= objects_to_dump.size());
              // Below objects are failed to dump, erase them from pending queue (others
              // that are successfully dumped will be erased in another callback after
              // add urls to local, i.e. `AddDumpedUrls`).
              for (size_t i = num_objects_dumped; i != objects_to_dump.size(); ++i) {
                const auto &object_id = objects_to_dump[i];
                auto it = objects_pending_dump_.find(object_id);
                RAY_CHECK(it != objects_pending_dump_.end());
                // pinned_objects_size_ += it->second.first->GetSize();
                // pinned_objects_.emplace(object_id, std::move(it->second));
                objects_pending_dump_.erase(it);
              }

              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send object dumping request: "
                               << status.ToString();
                if (callback) {
                  callback(status);
                }
              } else {
                AddDumpedUrls(objects_to_dump, r, callback);
              }
            });
      });
}

void LocalObjectManager::AddDumpedUrlsCallback(
    const ObjectID &object_id, const std::string &object_url,
    std::shared_ptr<size_t> num_remaining,
    std::function<void(const ray::Status &)> callback, ray::Status status) {
  if (!status.ok()) {
    RAY_LOG(INFO) << "Failed to send dumped url for object " << object_id
                  << " to object directory, considering the object to have been freed: "
                  << status.ToString();
  } else {
    RAY_LOG(DEBUG) << "Object " << object_id << " dumped to " << object_url
                   << " and object directory has been informed";
  }

  // Complete the whole dump process, erasing objects from the pending queue.
  auto it = objects_pending_dump_.find(object_id);
  RAY_CHECK(it != objects_pending_dump_.end());
  num_bytes_pending_dump_ -= it->second.first;
  objects_pending_dump_.erase(it);

  // Update the object_id -> url_ref_count to use it for deletion later.
  // We need to track the references here because a single file can contain
  // multiple objects, and we shouldn't delete the file until
  // all the objects are gone out of scope.
  // object_url is equivalent to url_with_offset.
  auto parsed_url = ParseURL(object_url);
  const auto base_url_it = parsed_url->find("url");
  RAY_CHECK(base_url_it != parsed_url->end());
  if (!url_ref_count_.contains(base_url_it->second)) {
    url_ref_count_[base_url_it->second] = 1;
  } else {
    url_ref_count_[base_url_it->second] += 1;
  }

  (*num_remaining)--;
  if (*num_remaining == 0 && callback) {
    callback(status);
  }
}

void LocalObjectManager::AddDumpedUrls(
    const std::vector<ObjectID> &object_ids, const rpc::DumpObjectsReply &worker_reply,
    std::function<void(const ray::Status &)> callback) {
  auto num_remaining = std::make_shared<size_t>(object_ids.size());
  for (size_t i = 0; i < static_cast<size_t>(worker_reply.dumped_objects_url_size());
       ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.dumped_objects_url(i);
    RAY_LOG(DEBUG) << "Object " << object_id << " dumped at " << object_url;
    // Choose a node id to report. If an external storage type is not a filesystem, we
    // don't need to report where this object is spilled.
    const auto node_id_object_dumped =
        is_external_storage_type_fs_ ? self_node_id_ : NodeID::Nil();

    auto it = objects_pending_dump_.find(object_id);
    RAY_CHECK(it != objects_pending_dump_.end());

    // There are times that restore request comes before the url is added to the object
    // directory. By adding the dumped url "before" adding it to the object directory, we
    // can process the restore request before object directory replies.
    dumped_objects_url_.emplace(object_id, object_url);
    // No need to unpin, because dumping is irrelevant to pin.
    auto add_dumped_url_callback =
        std::bind(&LocalObjectManager::AddDumpedUrlsCallback, this, object_id, object_url,
                  num_remaining, callback, std::placeholders::_1);
    rpc::AddDumpedUrlRequest request;
    request.set_object_id(object_id.Binary());
    request.set_dumped_url(object_url);
    request.set_dumped_node_id(node_id_object_dumped.Binary());
    request.set_size(it->second.first);

    auto owner_client = owner_client_pool_.GetOrConnect(it->second.second);
    RAY_LOG(DEBUG) << "Sending dumped URL " << object_url << " for object " << object_id
                   << " to owner " << WorkerID::FromBinary(it->second.second.worker_id());
    // Send dumped URL, spilled node ID, and object size to owner.
    owner_client->AddDumpedUrl(
        request,
        [add_dumped_url_callback](Status status, const rpc::AddDumpedUrlReply &reply) {
          add_dumped_url_callback(status);
        });
  }
}

void LocalObjectManager::LoadDumpedObject(
    const ObjectID &object_id, const std::string &object_url,
    std::function<void(const ray::Status &)> callback) {
  if (objects_pending_load_.count(object_id) > 0) {
    // If the same object is restoring, we dedup here.
    return;
  }

  RAY_CHECK(objects_pending_load_.emplace(object_id).second)
      << "Object dedupe wasn't done properly. Please report if you see this issue.";
  io_worker_pool_.PopLoadWorker([this, object_id, object_url,
                                 callback](std::shared_ptr<WorkerInterface> io_worker) {
    auto start_time = absl::GetCurrentTimeNanos();
    RAY_LOG(DEBUG) << "Sending load dumped object request";
    rpc::LoadDumpedObjectsRequest request;
    request.add_dumped_objects_url(std::move(object_url));
    request.add_object_ids_to_load(object_id.Binary());
    io_worker->rpc_client()->LoadDumpedObjects(
        request, [this, start_time, object_id, callback, io_worker](
                     const ray::Status &status, const rpc::LoadDumpedObjectsReply &r) {
          io_worker_pool_.PushLoadWorker(io_worker);
          objects_pending_load_.erase(object_id);
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send load dumped object request: "
                           << status.ToString();
          } else {
            auto now = absl::GetCurrentTimeNanos();
            auto loaded_bytes = r.bytes_loaded_total();
            RAY_LOG(DEBUG) << "Loaded " << loaded_bytes << " in "
                           << (now - start_time) / 1e6 << "ms. Object id:" << object_id;
          }
          if (callback) {
            callback(status);
          }
        });
  });
}

std::string LocalObjectManager::DebugString() const {
  std::stringstream result;
  result << "LocalObjectManager:\n";
  result << "- num pinned objects: " << pinned_objects_.size() << "\n";
  result << "- pinned objects size: " << pinned_objects_size_ << "\n";
  result << "- num objects pending restore: " << objects_pending_restore_.size() << "\n";
  result << "- num objects pending spill: " << objects_pending_spill_.size() << "\n";
  result << "- num bytes pending spill: " << num_bytes_pending_spill_ << "\n";
  return result.str();
}

void LocalObjectManager::RecordMetrics() const {
  RecordObjectSpillingStats();
  ray::stats::PlasmaPinnedObjectCount().Record(pinned_objects_.size());
  ray::stats::PlasmaPinObjectSize().Record(pinned_objects_size_);
  ray::stats::PlasmaSpillObjectSize().Record(spilled_bytes_total_);
  ray::stats::PlasmaSpillObjectCount().Record(spilled_objects_total_);
  ray::stats::PlasmaRestoreObjectSize().Record(restored_bytes_total_);
  ray::stats::PlasmaRestoreObjectCount().Record(restored_objects_total_);
}
};  // namespace raylet
};  // namespace ray
