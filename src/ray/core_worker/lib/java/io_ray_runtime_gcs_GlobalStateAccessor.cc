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

#include "io_ray_runtime_gcs_GlobalStateAccessor.h"

#include <jni.h>

#include "jni_utils.h"
#include "ray/core_worker/common.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT jlong JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeCreateGlobalStateAccessor(
    JNIEnv *env, jobject o, jstring j_redis_address, jstring j_redis_passowrd) {
  std::string redis_address = JavaStringToNativeString(env, j_redis_address);
  std::string redis_password = JavaStringToNativeString(env, j_redis_passowrd);
  ray::gcs::GlobalStateAccessor *gcs_accessor =
      new ray::gcs::GlobalStateAccessor(redis_address, redis_password);
  return reinterpret_cast<jlong>(gcs_accessor);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeDestroyGlobalStateAccessor(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  delete gcs_accessor;
}

JNIEXPORT jboolean JNICALL Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeConnect(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  return gcs_accessor->Connect();
}

JNIEXPORT jobject JNICALL Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllJobInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto job_info_list = gcs_accessor->GetAllJobInfo();
  return NativeVectorToJavaList<std::string>(
      env, job_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetNextJobID(JNIEnv *env, jobject o,
                                                               jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  const auto &job_id = gcs_accessor->GetNextJobID();
  return IdToJavaByteArray<ray::JobID>(env, job_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllNodeInfo(JNIEnv *env, jobject o,
                                                                 jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto node_info_list = gcs_accessor->GetAllNodeInfo();
  return NativeVectorToJavaList<std::string>(
      env, node_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllNodeInfoByNodegroup(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jstring j_nodegroup_id) {
  std::string nodegroup_id = JavaStringToNativeString(env, j_nodegroup_id);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto node_info_list = gcs_accessor->GetAllNodeInfoByNodegroup(nodegroup_id);
  return NativeVectorToJavaList<std::string>(
      env, node_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetNodeResourceInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray node_id_bytes) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto node_id = JavaByteArrayToId<ray::NodeID>(env, node_id_bytes);
  auto node_resource_info = gcs_accessor->GetNodeResourceInfo(node_id);
  return static_cast<jbyteArray>(NativeStringToJavaByteArray(env, node_resource_info));
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllActorInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto actor_info_list = gcs_accessor->GetAllActorInfo();
  return NativeVectorToJavaList<std::string>(
      env, actor_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetActorInfo(JNIEnv *env, jobject o,
                                                               jlong gcs_accessor_ptr,
                                                               jbyteArray actorId) {
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto actor_info = gcs_accessor->GetActorInfo(actor_id);
  if (actor_info) {
    return NativeStringToJavaByteArray(env, *actor_info);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetPlacementGroupInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray placement_group_id_bytes) {
  const auto placement_group_id =
      JavaByteArrayToId<ray::PlacementGroupID>(env, placement_group_id_bytes);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto placement_group = gcs_accessor->GetPlacementGroupInfo(placement_group_id);
  if (placement_group) {
    return NativeStringToJavaByteArray(env, *placement_group);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetResourceUsage(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray node_id_bytes) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto node_id = JavaByteArrayToId<ray::NodeID>(env, node_id_bytes);
  auto usage = gcs_accessor->GetResourceUsage(node_id);
  if (usage) {
    return NativeStringToJavaByteArray(env, *usage);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetPlacementGroupInfoByName(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jstring name, jstring ray_namespace) {
  std::string placement_group_name = JavaStringToNativeString(env, name);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto placement_group = gcs_accessor->GetPlacementGroupByName(
      placement_group_name, JavaStringToNativeString(env, ray_namespace));
  if (placement_group) {
    return NativeStringToJavaByteArray(env, *placement_group);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllPlacementGroupInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto placement_group_info_list = gcs_accessor->GetAllPlacementGroupInfo();
  return NativeVectorToJavaList<std::string>(
      env, placement_group_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jboolean JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeUpdateJobResourceRequirements(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray job_id_bytes,
    jobject min_resource_name_list, jdoubleArray min_resource_value_array,
    jobject max_resource_name_list, jdoubleArray max_resource_value_array) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, job_id_bytes);

  std::vector<std::string> min_resource_names;
  JavaStringListToNativeStringVector(env, min_resource_name_list, &min_resource_names);

  std::vector<double> min_resource_values;
  JavaDoubleArrayToNativeDoubleVector(env, min_resource_value_array,
                                      &min_resource_values);

  if (min_resource_names.size() != min_resource_values.size()) {
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(
        env,
        ray::Status::Invalid("The size of min_resource_name_list and "
                             "min_resource_value_array must be equal."),
        false);
  }

  std::vector<std::string> max_resource_names;
  JavaStringListToNativeStringVector(env, max_resource_name_list, &max_resource_names);

  std::vector<double> max_resource_values;
  JavaDoubleArrayToNativeDoubleVector(env, max_resource_value_array,
                                      &max_resource_values);

  if (max_resource_names.size() != max_resource_values.size()) {
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(
        env,
        ray::Status::Invalid("The size of max_resource_name_list and "
                             "max_resource_value_array must be equal."),
        false);
  }

  std::unordered_map<std::string, double> min_resource_requirements;
  for (size_t i = 0; i < min_resource_names.size(); ++i) {
    min_resource_requirements.emplace(min_resource_names[i], min_resource_values[i]);
  }

  std::unordered_map<std::string, double> max_resource_requirements;
  for (size_t i = 0; i < max_resource_names.size(); ++i) {
    max_resource_requirements.emplace(max_resource_names[i], max_resource_values[i]);
  }

  bool output = false;
  try {
    output = gcs_accessor->UpdateJobResourceRequirements(
        job_id, min_resource_requirements, max_resource_requirements);
  } catch (std::runtime_error &e) {
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, ray::Status::Invalid(e.what()), false);
  }
  return output;
}

JNIEXPORT void JNICALL Java_io_ray_runtime_gcs_GlobalStateAccessor_nativePutJobData(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray job_id_bytes,
    jbyteArray key_bytes, jbyteArray value_bytes) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, job_id_bytes);
  auto key = JavaByteArrayToNativeString(env, key_bytes);
  auto value = JavaByteArrayToNativeString(env, value_bytes);
  std::pair<bool, std::string> output;
  output = gcs_accessor->PutJobData(job_id, key, value);
  if (!output.first) {
    THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, ray::Status::Invalid(output.second),
                                         void(0));
  }
}

JNIEXPORT jbyteArray JNICALL Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetJobData(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray job_id_bytes,
    jbyteArray key_bytes) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto job_id = JavaByteArrayToId<ray::JobID>(env, job_id_bytes);
  auto key = JavaByteArrayToNativeString(env, key_bytes);
  auto data = gcs_accessor->GetJobData(job_id, key);
  return NativeStringToJavaByteArray(env, data);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetApiServerAddress(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto data = gcs_accessor->GetApiServerAddress();
  return NativeStringToJavaByteArray(env, data);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetNodeToConnectForDriver(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jstring nodeIpAddress) {
  std::string node_ip_address = JavaStringToNativeString(env, nodeIpAddress);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  std::string node_to_connect;
  auto status =
      gcs_accessor->GetNodeToConnectForDriver(node_ip_address, &node_to_connect);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeStringToJavaByteArray(env, node_to_connect);
}

#ifdef __cplusplus
}
#endif
