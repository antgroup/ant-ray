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

#include "io_ray_runtime_context_NativeWorkerContext.h"

#include <jni.h>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentTaskType(JNIEnv *env,
                                                                         jclass) {
  auto task_spec =
      ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentTask();
  RAY_CHECK(task_spec) << "Current task is not set.";
  return static_cast<int>(task_spec->GetMessage().type());
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentTaskId(JNIEnv *env,
                                                                       jclass,
                                                                       jlong ptr) {
  const ray::TaskID &task_id =
      ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentTaskID();
  auto dst = reinterpret_cast<uint8_t *>(ptr);
  std::copy(task_id.Data(), task_id.Data() + task_id.Size(), dst);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentJobId(JNIEnv *env, jclass,
                                                                      jlong ptr) {
  const auto &job_id =
      ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentJobID();
  auto dst = reinterpret_cast<uint8_t *>(ptr);
  std::copy(job_id.Data(), job_id.Data() + job_id.Size(), dst);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentWorkerId(JNIEnv *env,
                                                                         jclass,
                                                                         jlong ptr) {
  const auto &worker_id =
      ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetWorkerID();
  auto dst = reinterpret_cast<uint8_t *>(ptr);
  std::copy(worker_id.Data(), worker_id.Data() + worker_id.Size(), dst);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentActorId(JNIEnv *env,
                                                                        jclass,
                                                                        jlong ptr) {
  const auto &actor_id =
      ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
  auto dst = reinterpret_cast<uint8_t *>(ptr);
  std::copy(actor_id.Data(), actor_id.Data() + actor_id.Size(), dst);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetRpcAddress(JNIEnv *env, jclass) {
  const auto &rpc_address = ray::CoreWorkerProcess::GetCoreWorker().GetRpcAddress();
  return NativeStringToJavaByteArray(env, rpc_address.SerializeAsString());
}

JNIEXPORT jstring JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentNodeName(JNIEnv *env,
                                                                         jclass) {
  const std::string &node_name =
      ray::CoreWorkerProcess::GetCoreWorker().GetCurrentNodeName();
  return env->NewStringUTF(node_name.c_str());
}

#ifdef __cplusplus
}
#endif
