// Copyright  The Ray Authors.
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

namespace ray {
namespace gcs {

class MockGcsActor : public GcsActor {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsActorManager : public GcsActorManager {
 public:
  MockGcsActorManager(RuntimeEnvManager &runtime_env_manager,
                      GcsFunctionManager &function_manager)
      : GcsActorManager(
            /*scheduler=*/
            nullptr,
            /*gcs_table_storage=*/nullptr,
            /*io_context=*/mock_io_context_do_not_use_,
            /*gcs_publisher=*/nullptr,
            runtime_env_manager,
            function_manager,
            /*gcs_virtual_cluster_manager=*/mock_virtual_cluster_manager_do_not_use_,
            [](const ActorID &) {},
            [](const rpc::Address &) { return nullptr; }) {}

  MOCK_METHOD(void,
              HandleRegisterActor,
              (rpc::RegisterActorRequest request,
               rpc::RegisterActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCreateActor,
              (rpc::CreateActorRequest request,
               rpc::CreateActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetActorInfo,
              (rpc::GetActorInfoRequest request,
               rpc::GetActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetNamedActorInfo,
              (rpc::GetNamedActorInfoRequest request,
               rpc::GetNamedActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleListNamedActors,
              (rpc::ListNamedActorsRequest request,
               rpc::ListNamedActorsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllActorInfo,
              (rpc::GetAllActorInfoRequest request,
               rpc::GetAllActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleKillActorViaGcs,
              (rpc::KillActorViaGcsRequest request,
               rpc::KillActorViaGcsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));

  instrumented_io_context mock_io_context_do_not_use_;
  GcsVirtualClusterManager mock_virtual_cluster_manager_do_not_use_{
      mock_io_context_do_not_use_, 
      *static_cast<GcsTableStorage*>(nullptr), 
      *static_cast<GcsPublisher*>(nullptr), 
      *static_cast<const ClusterResourceManager*>(nullptr)};
};

}  // namespace gcs
}  // namespace ray