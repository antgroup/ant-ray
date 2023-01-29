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

#include "io_ray_runtime_task_NativeTaskSubmitter.h"

#include <jni.h>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/util/logging.h"
#include "ray/util/resource_util.h"

/// A helper that computes the hash code of a Java object.
inline jint GetHashCodeOfJavaObject(JNIEnv *env, jobject java_object) {
  const jint hashcode = env->CallIntMethod(java_object, java_object_hash_code);
  RAY_CHECK_JAVA_EXCEPTION(env);
  return hashcode;
}

/// Store C++ instances of ray function in the cache to avoid unnessesary JNI operations.
thread_local std::unordered_map<jint, std::vector<std::pair<jobject, ray::RayFunction>>>
    submitter_function_descriptor_cache;

inline const ray::RayFunction &ToRayFunction(JNIEnv *env, jobject functionDescriptor,
                                             jint hash) {
  auto &fd_vector = submitter_function_descriptor_cache[hash];
  for (auto &pair : fd_vector) {
    if (env->CallBooleanMethod(pair.first, java_object_equals, functionDescriptor)) {
      return pair.second;
    }
  }

  std::vector<std::string> function_descriptor_list;
  jobject list =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_to_list);
  RAY_CHECK_JAVA_EXCEPTION(env);
  JavaStringListToNativeStringVector(env, list, &function_descriptor_list);
  jobject java_language =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_get_language);
  RAY_CHECK_JAVA_EXCEPTION(env);
  auto language = static_cast<::Language>(
      env->CallIntMethod(java_language, java_language_get_number));
  RAY_CHECK_JAVA_EXCEPTION(env);
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(language, function_descriptor_list);
  fd_vector.emplace_back(env->NewGlobalRef(functionDescriptor),
                         ray::RayFunction(language, function_descriptor));
  return fd_vector.back().second;
}

inline std::vector<std::unique_ptr<ray::TaskArg>> ToTaskArgs(JNIEnv *env, jobject args) {
  std::vector<std::unique_ptr<ray::TaskArg>> task_args;
  JavaListToNativeVector<std::unique_ptr<ray::TaskArg>>(
      env, args, &task_args, [](JNIEnv *env, jobject arg) {
        auto java_id = env->GetObjectField(arg, java_function_arg_id);
        if (java_id) {
          auto java_id_bytes = static_cast<jbyteArray>(
              env->CallObjectMethod(java_id, java_base_id_get_bytes));
          RAY_CHECK_JAVA_EXCEPTION(env);
          auto id = JavaByteArrayToId<ray::ObjectID>(env, java_id_bytes);
          auto java_owner_address =
              env->GetObjectField(arg, java_function_arg_owner_address);
          RAY_CHECK(java_owner_address);
          auto owner_address =
              JavaProtobufObjectToNativeProtobufObject<ray::rpc::Address>(
                  env, java_owner_address);
          return std::unique_ptr<ray::TaskArg>(
              new ray::TaskArgByReference(id, owner_address));
        }
        auto java_value =
            static_cast<jbyteArray>(env->GetObjectField(arg, java_function_arg_value));
        RAY_CHECK(java_value) << "Both id and value of FunctionArg are null.";
        auto value = JavaNativeRayObjectToNativeRayObject(env, java_value);
        return std::unique_ptr<ray::TaskArg>(new ray::TaskArgByValue(value));
      });
  return task_args;
}

inline std::unordered_map<std::string, double> ToResources(JNIEnv *env,
                                                           jobject java_resources) {
  auto resources = JavaMapToNativeMap<std::string, double>(
      env, java_resources,
      [](JNIEnv *env, jobject java_key) {
        return JavaStringToNativeString(env, (jstring)java_key);
      },
      [](JNIEnv *env, jobject java_value) {
        double value = env->CallDoubleMethod(java_value, java_double_double_value);
        RAY_CHECK_JAVA_EXCEPTION(env);
        return value;
      });
  auto iter = resources.find("memory");
  if (iter != resources.end()) {
    if (!IsMultipleOfMemoryUnit(static_cast<uint64_t>(iter->second))) {
      auto status = ray::Status::Invalid("The value of memory resource (" +
                                         std::to_string(iter->second) +
                                         ") must be multiple of 50M.");
      std::unordered_map<std::string, double> empty_resources;
      THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, empty_resources);
    }
    iter->second = ToMemoryUnits(static_cast<uint64_t>(iter->second));
  }
  return resources;
}

inline std::pair<ray::PlacementGroupID, int64_t> ToPlacementGroupOptions(
    JNIEnv *env, jobject callOptions) {
  auto placement_group_options = std::make_pair(ray::PlacementGroupID::Nil(), -1);
  auto group = env->GetObjectField(callOptions, java_task_creation_options_group);
  if (group) {
    auto placement_group_id = env->GetObjectField(group, java_placement_group_id);
    auto java_id_bytes = static_cast<jbyteArray>(
        env->CallObjectMethod(placement_group_id, java_base_id_get_bytes));
    RAY_CHECK_JAVA_EXCEPTION(env);
    auto id = JavaByteArrayToId<ray::PlacementGroupID>(env, java_id_bytes);
    auto index = env->GetIntField(callOptions, java_task_creation_options_bundle_index);
    placement_group_options = std::make_pair(id, index);
  }
  return placement_group_options;
}

inline ray::TaskOptions ToTaskOptions(JNIEnv *env, jint numReturns, jobject callOptions) {
  std::unordered_map<std::string, double> resources;
  bool ignore_return = false;
  jobject object_enable_task_fast_fail = nullptr;
  std::optional<bool> enable_task_fast_fail = std::nullopt;
  std::string name = "";
  if (callOptions) {
    if (env->IsInstanceOf(callOptions, java_base_task_options_class)) {
      jobject java_resources =
          env->GetObjectField(callOptions, java_base_task_options_resources);
      resources = ToResources(env, java_resources);
      auto java_name = (jstring)env->GetObjectField(callOptions, java_call_options_name);
      if (java_name) {
        name = JavaStringToNativeString(env, java_name);
      }
    }
    if (env->IsInstanceOf(callOptions, java_call_options_class)) {
      ignore_return = env->GetBooleanField(callOptions, java_call_options_ignore_return);
    }
    if (env->IsInstanceOf(callOptions, java_actor_call_options_class)) {
      ignore_return =
          env->GetBooleanField(callOptions, java_actor_call_options_ignore_return);
    }
    if (env->IsInstanceOf(callOptions, java_actor_call_options_class)) {
      object_enable_task_fast_fail =
          env->GetObjectField(callOptions, java_actor_call_options_enable_task_fast_fail);
      if (object_enable_task_fast_fail != 0) {
        enable_task_fast_fail = (bool)env->CallBooleanMethod(object_enable_task_fast_fail,
                                                             java_boolean_value);
      }
    }
  }

  ray::TaskOptions task_options{name,
                                numReturns,
                                resources,
                                /*serialized_runtime_env*/ "{}",
                                /*override_environment_variables=*/{},
                                ignore_return,
                                enable_task_fast_fail};
  return task_options;
}

inline std::vector<ray::rpc::ActorAffinityMatchExpression>
ToNativeActorAffinityMatchExpressions(JNIEnv *env,
                                      jobject java_actor_affinity_match_expressions) {
  std::vector<ray::rpc::ActorAffinityMatchExpression> actor_affinity_match_expressions;
  JavaListToNativeVector<ray::rpc::ActorAffinityMatchExpression>(
      env, java_actor_affinity_match_expressions, &actor_affinity_match_expressions,
      [](JNIEnv *env, jobject java_actor_affinity_match_expression) {
        RAY_CHECK(java_actor_affinity_match_expression != nullptr);
        auto key = JavaStringToNativeString(
            env, (jstring)env->GetObjectField(java_actor_affinity_match_expression,
                                              java_actor_affinity_match_expression_key));

        auto java_actor_affinity_operator =
            (jobject)env->GetObjectField(java_actor_affinity_match_expression,
                                         java_actor_affinity_match_expression_operator);
        jint value = env->GetIntField(java_actor_affinity_operator,
                                      java_actor_affinity_operator_value);
        auto actor_affinity_operator = ray::rpc::ActorAffinityOperator(value);

        std::vector<std::string> values;
        JavaStringListToNativeStringVector(
            env,
            env->GetObjectField(java_actor_affinity_match_expression,
                                java_actor_affinity_match_expression_values),
            &values);

        auto soft = env->GetBooleanField(java_actor_affinity_match_expression,
                                         java_actor_affinity_match_expression_soft);

        ray::rpc::ActorAffinityMatchExpression match_expression;
        match_expression.set_key(key);
        match_expression.set_actor_affinity_operator(actor_affinity_operator);
        for (const auto &value : values) {
          match_expression.add_values(value);
        }
        match_expression.set_soft(soft);
        return match_expression;
      });
  return actor_affinity_match_expressions;
}
inline void setNodeAffinitySchedulingStrategy(
    JNIEnv *env, const jobject &java_scheduling_strategy,
    ray::rpc::SchedulingStrategy &scheduling_strategy) {
  auto soft = env->GetBooleanField(java_scheduling_strategy,
                                   java_node_affinity_scheduling_strategy_soft);
  auto anti_affinity = env->GetBooleanField(
      java_scheduling_strategy, java_node_affinity_scheduling_strategy_anti_affinity);
  jobject java_nodes = env->GetObjectField(java_scheduling_strategy,
                                           java_node_affinity_scheduling_strategy_nodes);
  std::vector<NodeID> nodes;
  JavaListToNativeVector<NodeID>(
      env, java_nodes, &nodes, [](JNIEnv *env, jobject node_id_hex) {
        auto hex = JavaStringToNativeString(env, (jstring)(node_id_hex));
        return NodeID::FromBinary(ray::UniqueID::FromHex(hex).Binary());
      });
  auto node_affinity_strategy =
      scheduling_strategy.mutable_node_affinity_scheduling_strategy();
  node_affinity_strategy->set_soft(soft);
  node_affinity_strategy->set_anti_affinity(anti_affinity);
  for (const auto &node : nodes) {
    node_affinity_strategy->add_nodes(node.Binary());
  }
}

inline ray::ActorCreationOptions ToActorCreationOptions(JNIEnv *env,
                                                        jobject actorCreationOptions) {
  std::string name = "";
  std::shared_ptr<bool> is_detached = nullptr;

  int64_t max_restarts = 0;
  std::unordered_map<std::string, double> resources;
  std::vector<std::string> dynamic_worker_options;
  uint64_t max_concurrency = 1;
  std::string serialized_runtime_env = "{}";
  auto placement_options = std::make_pair(ray::PlacementGroupID::Nil(), -1);
  bool enable_task_fast_fail = false;
  bool is_async = false;
  std::vector<ray::ConcurrentGroup> concurrent_groups;
  std::unordered_map<std::string, std::string> extended_properties;

  if (actorCreationOptions) {
    auto java_name = (jstring)env->GetObjectField(actorCreationOptions,
                                                  java_actor_creation_options_name);
    if (java_name) {
      name = JavaStringToNativeString(env, java_name);
    }

    auto java_actor_lifetime = (jobject)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_lifetime);
    if (java_actor_lifetime != nullptr) {
      jint actor_lifetime_value =
          env->GetIntField(java_actor_lifetime, java_actor_lifetime_value);
      RAY_CHECK_JAVA_EXCEPTION(env);
      is_detached = std::make_shared<bool>(actor_lifetime_value == 1);
    }

    max_restarts =
        env->GetIntField(actorCreationOptions, java_actor_creation_options_max_restarts);
    jobject java_resources =
        env->GetObjectField(actorCreationOptions, java_base_task_options_resources);
    resources = ToResources(env, java_resources);
    jobject java_jvm_options = env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_jvm_options);
    if (java_jvm_options) {
      JavaStringListToNativeStringVector(env, java_jvm_options, &dynamic_worker_options);
    }
    max_concurrency = static_cast<uint64_t>(env->GetIntField(
        actorCreationOptions, java_actor_creation_options_max_concurrency));

    auto group =
        env->GetObjectField(actorCreationOptions, java_actor_creation_options_group);
    if (group) {
      auto placement_group_id = env->GetObjectField(group, java_placement_group_id);
      auto java_id_bytes = static_cast<jbyteArray>(
          env->CallObjectMethod(placement_group_id, java_base_id_get_bytes));
      RAY_CHECK_JAVA_EXCEPTION(env);
      auto id = JavaByteArrayToId<ray::PlacementGroupID>(env, java_id_bytes);
      auto index = env->GetIntField(actorCreationOptions,
                                    java_actor_creation_options_bundle_index);
      placement_options = std::make_pair(id, index);
    }

    enable_task_fast_fail = env->GetBooleanField(
        actorCreationOptions, java_actor_creation_options_enable_task_fast_fail);
    is_async =
        env->GetBooleanField(actorCreationOptions, java_actor_creation_options_is_async);
    jobject java_extended_properties = env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_extended_properties);
    extended_properties = JavaMapToNativeMap<std::string, std::string>(
        env, java_extended_properties,
        [](JNIEnv *env, jobject java_key) {
          return JavaStringToNativeString(env, (jstring)java_key);
        },
        [](JNIEnv *env, jobject java_value) {
          return JavaStringToNativeString(env, (jstring)java_value);
        });

    jstring java_serialized_runtime_env = (jstring)env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_serialized_runtime_env);
    if (java_serialized_runtime_env != nullptr) {
      serialized_runtime_env = JavaStringToNativeString(env, java_serialized_runtime_env);
    }

    // Convert concurrent groups from Java to native.
    jobject java_concurrent_groups_field = env->GetObjectField(
        actorCreationOptions, java_actor_creation_options_concurrent_groups);
    RAY_CHECK(java_concurrent_groups_field != nullptr);
    JavaListToNativeVector<ray::ConcurrentGroup>(
        env, java_concurrent_groups_field, &concurrent_groups,
        [](JNIEnv *env, jobject java_concurrency_group_impl) {
          RAY_CHECK(java_concurrency_group_impl != nullptr);
          jobject java_func_descriptors =
              env->CallObjectMethod(java_concurrency_group_impl,
                                    java_concurrency_group_impl_get_function_descriptors);
          RAY_CHECK_JAVA_EXCEPTION(env);
          std::vector<ray::FunctionDescriptor> native_func_descriptors;
          JavaListToNativeVector<ray::FunctionDescriptor>(
              env, java_func_descriptors, &native_func_descriptors,
              [](JNIEnv *env, jobject java_func_descriptor) {
                RAY_CHECK(java_func_descriptor != nullptr);
                const jint hashcode = GetHashCodeOfJavaObject(env, java_func_descriptor);
                ray::FunctionDescriptor native_func =
                    ToRayFunction(env, java_func_descriptor, hashcode)
                        .GetFunctionDescriptor();
                return native_func;
              });

          // Put func_descriptors into this task group.
          const std::string concurrent_group_name = JavaStringToNativeString(
              env, (jstring)env->GetObjectField(java_concurrency_group_impl,
                                                java_concurrency_group_impl_name));
          const uint32_t max_concurrency = env->GetIntField(
              java_concurrency_group_impl, java_concurrency_group_impl_max_concurrency);
          return ray::ConcurrentGroup{concurrent_group_name, max_concurrency,
                                      native_func_descriptors};
        });
  }

  // TODO(suquark): support passing namespace for Java. Currently
  // there is no use case.
  std::string ray_namespace = "";
  auto java_scheduling_strategy = env->GetObjectField(
      actorCreationOptions, java_actor_creation_options_scheduling_strategy);
  ray::rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  if (!placement_options.first.IsNil()) {
    auto placement_group_scheduling_strategy =
        scheduling_strategy.mutable_placement_group_scheduling_strategy();
    placement_group_scheduling_strategy->set_placement_group_id(
        placement_options.first.Binary());
    placement_group_scheduling_strategy->set_placement_group_bundle_index(
        placement_options.second);
    placement_group_scheduling_strategy->set_placement_group_capture_child_tasks(false);
  } else if (java_scheduling_strategy &&
             env->IsInstanceOf(java_scheduling_strategy,
                               java_actor_affinity_scheduling_strategy_class)) {
    auto actor_affinity_scheduling_strategy =
        scheduling_strategy.mutable_actor_affinity_scheduling_strategy();
    jobject java_actor_affinity_match_expressions = env->GetObjectField(
        java_scheduling_strategy,
        java_actor_affinity_scheduling_strategy_actor_affinity_match_expressions);
    auto actor_affinity_match_expressions =
        ToNativeActorAffinityMatchExpressions(env, java_actor_affinity_match_expressions);
    if (!actor_affinity_match_expressions.empty()) {
      for (const auto &expression : actor_affinity_match_expressions) {
        actor_affinity_scheduling_strategy->add_match_expressions()->CopyFrom(expression);
      }
    }
  } else if (java_scheduling_strategy &&
             env->IsInstanceOf(java_scheduling_strategy,
                               java_node_affinity_scheduling_strategy_class)) {
    setNodeAffinitySchedulingStrategy(env, java_scheduling_strategy, scheduling_strategy);
  }

  std::unordered_map<std::string, std::string> labels;
  labels = JavaMapToNativeMap<std::string, std::string>(
      env, env->GetObjectField(actorCreationOptions, java_actor_creation_options_labels),
      [](JNIEnv *env, jobject java_key) {
        return JavaStringToNativeString(env, (jstring)java_key);
      },
      [](JNIEnv *env, jobject java_value) {
        return JavaStringToNativeString(env, (jstring)java_value);
      });

  ray::ActorCreationOptions actor_creation_options{
      max_restarts,
      0,  // TODO: Allow setting max_task_retries from Java.
      static_cast<int>(max_concurrency),
      resources,
      resources,
      dynamic_worker_options,
      is_detached,
      name,
      ray_namespace,
      is_async,
      /*scheduling_strategy=*/scheduling_strategy,
      serialized_runtime_env,
      /*override_environment_variables=*/{},
      enable_task_fast_fail,
      labels,
      extended_properties,
      concurrent_groups};
  return actor_creation_options;
}

inline ray::PlacementStrategy ConvertStrategy(jint java_strategy) {
  switch (java_strategy) {
  case 0:
    return ray::rpc::PACK;
  case 1:
    return ray::rpc::SPREAD;
  case 2:
    return ray::rpc::STRICT_PACK;
  default:
    return ray::rpc::STRICT_SPREAD;
  }
}

inline ray::PlacementGroupCreationOptions ToPlacementGroupCreationOptions(
    JNIEnv *env, jobject placementGroupCreationOptions) {
  // We have make sure the placementGroupCreationOptions is not null in java api.
  std::string name = "";
  jstring java_name = (jstring)env->GetObjectField(
      placementGroupCreationOptions, java_placement_group_creation_options_name);
  if (java_name) {
    name = JavaStringToNativeString(env, java_name);
  }
  jobject java_obj_strategy = env->GetObjectField(
      placementGroupCreationOptions, java_placement_group_creation_options_strategy);
  jint java_strategy = env->CallIntMethod(
      java_obj_strategy, java_placement_group_creation_options_strategy_value);
  jobject java_bundles = env->GetObjectField(
      placementGroupCreationOptions, java_placement_group_creation_options_bundles);
  std::vector<std::unordered_map<std::string, double>> bundles;
  JavaListToNativeVector<std::unordered_map<std::string, double>>(
      env, java_bundles, &bundles, [](JNIEnv *env, jobject java_bundle) {
        return JavaMapToNativeMap<std::string, double>(
            env, java_bundle,
            [](JNIEnv *env, jobject java_key) {
              return JavaStringToNativeString(env, (jstring)java_key);
            },
            [](JNIEnv *env, jobject java_value) {
              double value = env->CallDoubleMethod(java_value, java_double_double_value);
              RAY_CHECK_JAVA_EXCEPTION(env);
              return value;
            });
      });
  return ray::PlacementGroupCreationOptions(name, ConvertStrategy(java_strategy), bundles,
                                            /*is_detached=*/false);
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobject JNICALL Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitTask(
    JNIEnv *env, jclass p, jobject functionDescriptor, jint functionDescriptorHash,
    jobject args, jint numReturns, jobject callOptions) {
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto task_options = ToTaskOptions(env, numReturns, callOptions);
  auto placement_group_options = ToPlacementGroupOptions(env, callOptions);
  jobject java_scheduling_strategy = nullptr;
  if (env->IsInstanceOf(callOptions, java_call_options_class)) {
    java_scheduling_strategy =
        env->GetObjectField(callOptions, java_call_options_scheduling_strategy);
  }

  std::vector<ObjectID> return_ids;
  ray::rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  if (!placement_group_options.first.IsNil()) {
    auto placement_group_scheduling_strategy =
        scheduling_strategy.mutable_placement_group_scheduling_strategy();
    placement_group_scheduling_strategy->set_placement_group_id(
        placement_group_options.first.Binary());
    placement_group_scheduling_strategy->set_placement_group_bundle_index(
        placement_group_options.second);
    placement_group_scheduling_strategy->set_placement_group_capture_child_tasks(false);
  } else if (java_scheduling_strategy &&
             env->IsInstanceOf(java_scheduling_strategy,
                               java_node_affinity_scheduling_strategy_class)) {
    setNodeAffinitySchedulingStrategy(env, java_scheduling_strategy, scheduling_strategy);
  }
  // TODO (kfstorm): Allow setting `max_retries` via `CallOptions`.
  auto status = ray::CoreWorkerProcess::GetCoreWorker().SubmitTask(
      ray_function, task_args, task_options, &return_ids,
      /*max_retries=*/0,
      /*retry_exceptions=*/false,
      /*scheduling_strategy=*/scheduling_strategy,
      /*debugger_breakpoint*/ "");
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

  // This is to avoid creating an empty java list and boost performance.
  if (return_ids.empty()) {
    return nullptr;
  }

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreateActor(
    JNIEnv *env, jclass p, jobject functionDescriptor, jint functionDescriptorHash,
    jobject args, jobject actorCreationOptions) {
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  auto task_args = ToTaskArgs(env, args);
  auto actor_creation_options = ToActorCreationOptions(env, actorCreationOptions);

  ActorID actor_id;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().CreateActor(
      ray_function, task_args, actor_creation_options,
      /*extension_data*/ "", &actor_id);

  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ActorID>(env, actor_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitActorTask(
    JNIEnv *env, jclass p, jbyteArray actorId, jobject functionDescriptor,
    jint functionDescriptorHash, jobject args, jint numReturns, jobject callOptions) {
  auto actor_id = JavaByteArrayToId<ray::ActorID>(env, actorId);
  const auto &ray_function =
      ToRayFunction(env, functionDescriptor, functionDescriptorHash);
  RAY_LOG(DEBUG) << "[JNI]Start converting function args to task args for function: "
                 << ray_function.GetFunctionDescriptor()->ToString();
  auto task_args = ToTaskArgs(env, args);
  RAY_LOG(DEBUG) << "[JNI]Finished converting function args to task args, size: "
                 << task_args.size();
  auto task_options = ToTaskOptions(env, numReturns, callOptions);

  std::vector<ObjectID> return_ids;
  ray::Status status = ray::CoreWorkerProcess::GetCoreWorker().SubmitActorTask(
      actor_id, ray_function, task_args, task_options, &return_ids);

  if (status.IsTryAgain()) {
    return nullptr;
  }
  // This is to avoid creating an empty java list and boost performance.
  if (return_ids.empty()) {
    return nullptr;
  }

  return NativeIdVectorToJavaByteArrayList(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreatePlacementGroup(
    JNIEnv *env, jclass, jobject placementGroupCreationOptions) {
  auto options = ToPlacementGroupCreationOptions(env, placementGroupCreationOptions);
  ray::PlacementGroupID placement_group_id;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().CreatePlacementGroup(
      options, &placement_group_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::PlacementGroupID>(env, placement_group_id);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeRemovePlacementGroup(
    JNIEnv *env, jclass p, jbyteArray placement_group_id_bytes) {
  const auto placement_group_id =
      JavaByteArrayToId<ray::PlacementGroupID>(env, placement_group_id_bytes);
  auto status =
      ray::CoreWorkerProcess::GetCoreWorker().RemovePlacementGroup(placement_group_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jboolean JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeWaitPlacementGroupReady(
    JNIEnv *env, jclass p, jbyteArray placement_group_id_bytes, jint timeout_seconds) {
  const auto placement_group_id =
      JavaByteArrayToId<ray::PlacementGroupID>(env, placement_group_id_bytes);
  auto status = ray::CoreWorkerProcess::GetCoreWorker().WaitPlacementGroupReady(
      placement_group_id, timeout_seconds);
  if (status.IsNotFound()) {
    env->ThrowNew(java_ray_exception_class, status.message().c_str());
  }
  return status.ok();
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeAddBundlesForPlacementGroup(
    JNIEnv *env, jclass p, jbyteArray placement_group_id_bytes, jobject java_bundles) {
  const auto placement_group_id =
      JavaByteArrayToId<ray::PlacementGroupID>(env, placement_group_id_bytes);
  std::vector<std::unordered_map<std::string, double>> bundles;
  JavaListToNativeVector<std::unordered_map<std::string, double>>(
      env, java_bundles, &bundles, [](JNIEnv *env, jobject java_bundle) {
        return JavaMapToNativeMap<std::string, double>(
            env, java_bundle,
            [](JNIEnv *env, jobject java_key) {
              return JavaStringToNativeString(env, (jstring)java_key);
            },
            [](JNIEnv *env, jobject java_value) {
              double value = env->CallDoubleMethod(java_value, java_double_double_value);
              RAY_CHECK_JAVA_EXCEPTION(env);
              return value;
            });
      });
  auto status = ray::CoreWorkerProcess::GetCoreWorker().AddPlacementGroupBundles(
      placement_group_id, bundles);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeRemoveBundlesForPlacementGroup(
    JNIEnv *env, jclass p, jbyteArray placement_group_id_bytes, jobject java_bundles) {
  const auto placement_group_id =
      JavaByteArrayToId<ray::PlacementGroupID>(env, placement_group_id_bytes);
  std::vector<int> bundle_indexes;
  JavaListToNativeVector<int>(
      env, java_bundles, &bundle_indexes, [](JNIEnv *env, jobject java_bundle) {
        int value = env->CallIntMethod(java_bundle, java_integer_int_value);
        RAY_CHECK_JAVA_EXCEPTION(env);
        return value;
      });
  auto status = ray::CoreWorkerProcess::GetCoreWorker().RemovePlacementGroupBundles(
      placement_group_id, bundle_indexes);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
