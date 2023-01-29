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

#include "ray/gcs/gcs_server/gcs_init_data.h"

#include "ray/event/event.h"

namespace ray {
namespace gcs {
void GcsInitData::AsyncLoad(const EmptyCallback &on_done) {
  // There are 11 kinds of table data need to be loaded.
  auto count_down = std::make_shared<int>(11);
  auto on_load_finished = [count_down, on_done] {
    if (--(*count_down) == 0) {
      if (on_done) {
        on_done();
      }
    }
  };

  AsyncLoadJobTableData(on_load_finished);

  AsyncLoadNodeTableData(on_load_finished);

  AsyncLoadObjectTableData(on_load_finished);

  AsyncLoadResourceTableData(on_load_finished);

  AsyncLoadNodegroupTableData(on_load_finished);

  AsyncLoadActorTableData(on_load_finished);

  AsyncLoadActorTaskSpecTableData(on_load_finished);

  AsyncLoadWorkerProcessRuntimeResourceTableData(on_load_finished);

  AsyncLoadPlacementGroupTableData(on_load_finished);

  AsyncLoadFrozenNodesTableData(on_load_finished);

  AsyncLoadWorkerTableData(on_load_finished);
}

void GcsInitData::AsyncLoadJobTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading job table data.";
  auto load_job_table_data_callback =
      [this, on_done](std::unordered_map<JobID, rpc::JobTableData> &&result) {
        job_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading job table data, size = "
                      << job_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->JobTable().GetAll(load_job_table_data_callback));
}

void GcsInitData::AsyncLoadNodeTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading node table data.";
  auto load_node_table_data_callback =
      [this, on_done](std::unordered_map<NodeID, rpc::GcsNodeInfo> &&result) {
        if (!result.empty()) {
          RAY_EVENT(ERROR, EVENT_LABEL_GCS_RESTART)
              << "GCS server restarts and recovers from storage.";
        }
        node_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading node table data, size = "
                      << node_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->NodeTable().GetAll(load_node_table_data_callback));
}

void GcsInitData::AsyncLoadObjectTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading object table data.";
  auto load_object_table_data_callback =
      [this, on_done](std::unordered_map<ObjectID, rpc::ObjectLocationInfo> &&result) {
        object_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading object table data, size = "
                      << object_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->ObjectTable().GetAll(load_object_table_data_callback));
}

void GcsInitData::AsyncLoadResourceTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading cluster resources table data.";
  auto load_resource_table_data_callback =
      [this, on_done](std::unordered_map<NodeID, rpc::ResourceMap> &&result) {
        resource_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading cluster resources table data, size = "
                      << resource_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(
      gcs_table_storage_->NodeResourceTable().GetAll(load_resource_table_data_callback));
}

void GcsInitData::AsyncLoadNodegroupTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading nodegroup table data.";
  auto load_nodegroup_table_data_callback =
      [this, on_done](std::unordered_map<NodegroupID, rpc::NodegroupData> &&result) {
        nodegroup_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading nodegroup table data, size = "
                      << nodegroup_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(
      gcs_table_storage_->NodegroupTable().GetAll(load_nodegroup_table_data_callback));
}

void GcsInitData::AsyncLoadPlacementGroupTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading placement group table data.";
  auto load_placement_group_table_data_callback =
      [this, on_done](
          std::unordered_map<PlacementGroupID, rpc::PlacementGroupTableData> &&result) {
        placement_group_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading placement group table data, size = "
                      << placement_group_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().GetAll(
      load_placement_group_table_data_callback));
}

void GcsInitData::AsyncLoadActorTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading actor table data.";
  auto load_actor_table_data_callback =
      [this, on_done](std::unordered_map<ActorID, ActorTableData> &&result) {
        actor_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading actor table data, size = "
                      << actor_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().GetAll(load_actor_table_data_callback));
}

void GcsInitData::AsyncLoadActorTaskSpecTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading actor task spec table data.";
  auto load_actor_task_spec_table_data_callback =
      [this, on_done](std::unordered_map<ActorID, TaskSpec> &&result) {
        actor_task_spec_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading actor task spec table data, size = "
                      << actor_task_spec_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->ActorTaskSpecTable().GetAll(
      load_actor_task_spec_table_data_callback));
}

void GcsInitData::AsyncLoadFrozenNodesTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading frozen nodes table data.";
  RAY_CHECK_OK(gcs_table_storage_->FrozenNodeTable().Get(
      CONST_KEY_FROZEN_TABLE,
      [this, on_done](Status status,
                      const boost::optional<rpc::FrozenNodesNotification> &result) {
        if (result) {
          for (auto &host : result.value().frozen_node_set()) {
            frozen_nodes_table_data_.emplace(host);
          }
        }
        RAY_LOG(INFO) << "Finished loading frozen nodes table data, size = "
                      << frozen_nodes_table_data_.size();
        on_done();
      }));
}

void GcsInitData::AsyncLoadWorkerTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading worker table data.";
  auto load_worker_table_data_callback =
      [this, on_done](std::unordered_map<WorkerID, rpc::WorkerTableData> &&result) {
        worker_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading worker table data, size = "
                      << worker_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->WorkerTable().GetAll(load_worker_table_data_callback));
}

void GcsInitData::AsyncLoadWorkerProcessRuntimeResourceTableData(
    const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading worker process runtime resource table data.";
  auto load_worker_process_runtime_resource_table_data_callback =
      [this, on_done](
          std::unordered_map<UniqueID, rpc::GcsWorkerProcessRuntimeResourceTableData>
              &&result) {
        worker_process_runtime_resource_table_data_ = std::move(result);
        RAY_LOG(INFO)
            << "Finished loading worker process runtime resource table data, size = "
            << worker_process_runtime_resource_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->WorkerProcessRuntimeResourceTable().GetAll(
      load_worker_process_runtime_resource_table_data_callback));
}

}  // namespace gcs
}  // namespace ray