#include "ray/gcs/gcs_server/gcs_actor_migration_manager.h"

#include "ray/common/asio/asio_util.h"

namespace ray {
namespace gcs {

void GcsActorMigrationManager::HandleMigrateActorsInNode(
    const rpc::MigrateActorsInNodeRequest &request, rpc::MigrateActorsInNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  int64_t migration_id = current_sys_time_ns();
  RAY_LOG(INFO) << "Handling Migration request, node_name_list_size = "
                << request.node_name_list().size() << ", migration_id = " << migration_id;
  auto &migration_node_info = migration_id_to_nodes_info_[migration_id];
  for (auto &node_name : request.node_name_list()) {
    Status status = MigrateActorsInNode(node_name, migration_id);
    rpc::MigrateActorsInNodeReply_ResultTuple result_tuple;
    result_tuple.set_success(status.ok());
    if (status.ok()) {
      migration_node_info.number_of_nodes_with_inflight_tasks_++;
      result_tuple.set_message("Migration started, migration info will be published.");
    } else {
      result_tuple.set_message(status.message());
    }
    (*reply->mutable_results())[node_name] = std::move(result_tuple);
  }
  if (migration_node_info.number_of_nodes_with_inflight_tasks_ !=
      request.node_name_list().size()) {
    RAY_LOG(INFO) << "Only " << migration_node_info.number_of_nodes_with_inflight_tasks_
                  << " nodes will be migrated, migration_id = " << migration_id;
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

Status GcsActorMigrationManager::MigrateActorsInNode(const std::string &node_name,
                                                     int64_t migration_id) {
  auto node = GetUniqueNodeByName(node_name);
  if (!node) {
    std::stringstream ss;
    ss << "[Migration] Can't find node with name " << node_name;
    RAY_LOG(WARNING) << ss.str();
    return Status::MigrationFailure(ss.str());
  }
  const NodeID node_id = NodeID::FromBinary((*node)->basic_gcs_node_info().node_id());
  if (HasAnyDriversInNode(node_id)) {
    std::stringstream ss;
    ss << "[Migration] node " << node_name << ", id:" << node_id
       << " has job driver, this node can't be migrated.";
    RAY_LOG(WARNING) << ss.str();
    return Status::MigrationFailure(ss.str());
  }
  if (node_id_to_migration_info_.contains(node_id)) {
    std::stringstream ss;
    bool has_in_flight_pgs = IsPlacementGroupSchedulingInProgress();
    ss << "[Migration] node " << node_name << ", id:" << node_id
       << " is already in migration process, skip this migration request."
       << " In-flight actor num: "
       << node_id_to_migration_info_[node_id].inflight_actors_.size()
       << ", has in-flight PG: " << has_in_flight_pgs;
    RAY_LOG(WARNING) << ss.str();
    return Status::MigrationFailure(ss.str());
  }
  RAY_LOG(INFO) << "[Migration] Freezing node " << node_name;
  FreezeNode(node_name);
  // Will then check whether there are no in-flight actors in this node by a peridical
  // task.
  auto &migration_info = node_id_to_migration_info_[node_id];
  migration_info.migration_id_ = migration_id;
  migration_info.inflight_actors_ =
      FilterInflightActors(node_id, GetAllActorsInNode(node_id));
  return Status::OK();
}

void GcsActorMigrationManager::HandleCheckIfMigrationIsComplete(
    const rpc::CheckIfMigrationIsCompleteRequest &request,
    rpc::CheckIfMigrationIsCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::ostringstream ostr;
  std::copy(request.node_name_list().begin(), request.node_name_list().end(),
            std::ostream_iterator<std::string>(ostr, ", "));
  RAY_LOG(INFO) << "Checking if the migration is complete, [" << ostr.str() << "]";
  for (const auto &node_name : request.node_name_list()) {
    if (IsNodeMigrationComplete(node_name)) {
      reply->add_completed_node_name_list(node_name);
    } else {
      reply->add_uncompleted_node_name_list(node_name);
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsActorMigrationManager::PeriodicallyCheckInFlightActorsAndPG() {
  RAY_LOG(DEBUG) << "[Migration] PeriodicallyCheckInFlightActorsAndPG, there are "
                 << node_id_to_migration_info_.size() << " nodes need to be migrated.";
  absl::flat_hash_set<int64_t> valid_migration_ids;
  for (auto iter = node_id_to_migration_info_.begin();
       iter != node_id_to_migration_info_.end();) {
    const NodeID &node_id = iter->first;
    auto &migration_info = iter->second;
    auto &inflight_actors = migration_info.inflight_actors_;
    // Collect valid migration id.
    valid_migration_ids.emplace(migration_info.migration_id_);

    auto optional_node = GetAliveNode(iter->first);
    if (!optional_node) {
      RAY_LOG(INFO)
          << "Node " << node_id
          << " is already dead, remove it from the migration info, migration_id = "
          << migration_info.migration_id_;
      UpdateReadyNode(migration_info.migration_id_, node_id, /*is_dead_node=*/true);
      node_id_to_migration_info_.erase(iter++);
      continue;
    }
    auto &node_name = (*optional_node)->basic_gcs_node_info().node_name();

    // TODO(lixin): IsSchedulingInProgress is a superset of 'has in-flight PGs', we
    // should change this to a more specific checking.
    inflight_actors = FilterInflightActors(node_id, inflight_actors);
    bool has_in_flight_pgs = IsPlacementGroupSchedulingInProgress();
    bool has_in_flight_actors = !inflight_actors.empty();
    RAY_LOG(DEBUG) << "[Migration] Checking node " << node_name << ", id=" << node_id
                   << ", has_in_flight_actors=" << has_in_flight_actors
                   << ", has_in_flight_pgs=" << has_in_flight_pgs;
    if (!has_in_flight_actors && !has_in_flight_pgs) {
      UpdateReadyNode(migration_info.migration_id_, node_id, /*is_dead_node=*/false);
      node_id_to_migration_info_.erase(iter++);
      continue;
    }
    iter++;
  }

  // Remove invalid migration info.
  for (auto iter = migration_id_to_nodes_info_.begin();
       iter != migration_id_to_nodes_info_.end();) {
    auto current = iter++;
    if (!valid_migration_ids.contains(current->first)) {
      migration_id_to_nodes_info_.erase(current);
    }
  }
}

void GcsActorMigrationManager::UpdateReadyNode(int64_t migration_id,
                                               const NodeID &node_id, bool is_node_dead) {
  auto iter = migration_id_to_nodes_info_.find(migration_id);
  if (iter == migration_id_to_nodes_info_.end()) {
    return;
  }

  auto &migration_node_info = iter->second;
  migration_node_info.number_of_nodes_with_inflight_tasks_--;
  if (!is_node_dead) {
    migration_node_info.ready_nodes_.emplace_back(node_id);
  }
  RAY_LOG(DEBUG) << "[Migration] migration_id = " << migration_id
                 << ", node_id = " << node_id << ", is_dead_node = " << is_node_dead
                 << ", " << migration_node_info.ready_nodes_.size()
                 << " nodes ready to be migrated, "
                 << migration_node_info.number_of_nodes_with_inflight_tasks_
                 << " nodes still have inflight tasks.";
  if (migration_node_info.number_of_nodes_with_inflight_tasks_ <= 0) {
    if (!migration_node_info.ready_nodes_.empty()) {
      PubMigrationNotification(migration_node_info.ready_nodes_, migration_id);
    }
    migration_id_to_nodes_info_.erase(iter);
  }
}

std::vector<std::weak_ptr<GcsActor>> GcsActorMigrationManager::FilterInflightActors(
    const NodeID &node_id, const std::vector<std::weak_ptr<GcsActor>> &actors) {
  // Check whether there is in-flight actors and remove all non-in-flight actors from the
  // list.
  std::vector<std::weak_ptr<GcsActor>> res;
  for (const auto &actor_ptr : actors) {
    if (const std::shared_ptr<GcsActor> gcs_actor = actor_ptr.lock()) {
      // Note: if node ID has changed, it means this actor has been rescheduled to another
      // node because of node freezing.
      if (gcs_actor->GetNodeID() != node_id) {
        continue;
      }
      auto state = gcs_actor->GetState();
      if (state == rpc::ActorTableData::ALIVE || state == rpc::ActorTableData::DEAD) {
        continue;
      }
      res.push_back(actor_ptr);
    }
  }
  return res;
}

void GcsActorMigrationManager::PubMigrationNotification(
    const std::vector<NodeID> &node_id_list, int64_t migration_id) {
  rpc::ActorMigrationNotificationBatch notification_batch;
  notification_batch.set_migration_id(migration_id);
  for (auto &node_id : node_id_list) {
    auto optional_node = GetAliveNode(node_id);
    auto &node_name = (*optional_node)->basic_gcs_node_info().node_name();

    RAY_LOG(INFO) << "[Migration] Preparing migration notification, node_name="
                  << node_name << ", node_id=" << node_id;

    rpc::ActorMigrationNotification *notification =
        notification_batch.add_notification_list();

    // Add actors.
    const auto &created_actors_map = GetCreatedActors();
    auto it = created_actors_map.find(node_id);
    if (it == created_actors_map.end()) {
      RAY_LOG(INFO) << "[Migration] No actor found, node_name=" << node_name
                    << ", node_id=" << node_id;
    } else {
      for (auto &pair : it->second) {
        notification->add_actor_id_set(pair.second.Binary());
      }
    }

    // Set node name
    notification->set_node_name(node_name);

    RAY_LOG(INFO) << "[Migration] Actor list size=" << notification->actor_id_set_size()
                  << ", node_name=" << node_name << ", node_id=" << node_id;
  }
  RAY_LOG(INFO) << "[Migration] Publishing migration notification, batch size="
                << notification_batch.notification_list_size();
  RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_MIGRATION_CHANNEL,
                                     CONST_KEY_ACTOR_MIGRATION_TABLE.Binary(),
                                     notification_batch.SerializeAsString(), nullptr));
}

bool GcsActorMigrationManager::IsNodeMigrationComplete(
    const std::string &node_name) const {
  if (auto node = GetUniqueNodeByName(node_name)) {
    auto node_id = NodeID::FromBinary(node.value()->basic_gcs_node_info().node_id());
    return gcs_resource_manager_->IsUnusedNode(node_id);
  }
  return true;
}

void GcsActorMigrationManager::FreezeNode(const std::string &node_name) {
  gcs_frozen_node_manager_->FreezeNode(node_name);
}

absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GcsActorMigrationManager::GetAliveNode(
    const NodeID &node_id) const {
  return gcs_node_manager_->GetAliveNode(node_id);
}

boost::optional<std::shared_ptr<rpc::GcsNodeInfo>>
GcsActorMigrationManager::GetUniqueNodeByName(const std::string &node_name) const {
  return gcs_node_manager_->GetUniqueNodeByName(node_name);
}

bool GcsActorMigrationManager::IsPlacementGroupSchedulingInProgress() const {
  return gcs_placement_group_manager_->IsSchedulingInProgress();
}

bool GcsActorMigrationManager::HasAnyDriversInNode(const NodeID &node_id) const {
  return gcs_job_manager_->HasAnyDriversInNode(node_id);
}

std::vector<std::weak_ptr<GcsActor>> GcsActorMigrationManager::GetAllActorsInNode(
    const NodeID &node_id) const {
  return gcs_actor_manager_->GetAllActorsInNode(node_id);
}

const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
    &GcsActorMigrationManager::GetCreatedActors() const {
  return gcs_actor_manager_->GetCreatedActors();
}

}  // namespace gcs
}  // namespace ray