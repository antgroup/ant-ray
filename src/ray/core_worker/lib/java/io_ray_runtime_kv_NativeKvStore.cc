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

#include "io_ray_runtime_kv_NativeKvStore.h"

#include "jni_utils.h"
#include "ray/core_worker/core_worker.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jboolean JNICALL Java_io_ray_runtime_kv_NativeKvStore_nativePut(
    JNIEnv *env, jclass, jbyteArray key, jbyteArray value, jboolean overwrite,
    jboolean isGlobal) {
  const auto native_key = JavaByteArrayToNativeString(env, key);
  const auto native_value = JavaByteArrayToNativeString(env, value);
  const auto pair = ray::CoreWorkerProcess::GetCoreWorker().PutKV(
      native_key, native_value, overwrite, isGlobal);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, pair.second, false);
  if (overwrite) {
    return true;
  } else {
    return pair.first;
  }
}

JNIEXPORT jbyteArray JNICALL Java_io_ray_runtime_kv_NativeKvStore_nativeGet(
    JNIEnv *env, jclass, jbyteArray key, jboolean isGlobal) {
  const auto native_key = JavaByteArrayToNativeString(env, key);
  const auto pair = ray::CoreWorkerProcess::GetCoreWorker().GetKV(native_key, isGlobal);
  if (pair.second.IsNotFound()) {
    return nullptr;
  }
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, pair.second, nullptr);
  return NativeStringToJavaByteArray(env, pair.first);
}

JNIEXPORT jboolean JNICALL Java_io_ray_runtime_kv_NativeKvStore_nativeExists(
    JNIEnv *env, jclass, jbyteArray key, jboolean isGlobal) {
  const auto native_key = JavaByteArrayToNativeString(env, key);
  const auto pair =
      ray::CoreWorkerProcess::GetCoreWorker().ExistsKV(native_key, isGlobal);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, pair.second, false);
  return pair.first;
}

JNIEXPORT jboolean JNICALL Java_io_ray_runtime_kv_NativeKvStore_nativeDelete(
    JNIEnv *env, jclass, jbyteArray key, jboolean isGlobal) {
  const auto native_key = JavaByteArrayToNativeString(env, key);
  const auto pair =
      ray::CoreWorkerProcess::GetCoreWorker().DeleteKV(native_key, isGlobal);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, pair.second, false);
  return pair.first;
}

#ifdef __cplusplus
}
#endif
