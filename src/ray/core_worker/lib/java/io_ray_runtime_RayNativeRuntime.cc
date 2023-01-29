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

#include "io_ray_runtime_RayNativeRuntime.h"

#include <jni.h>

#include <sstream>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/core_worker.h"
#include "ray/event/event.h"

thread_local JNIEnv *local_env = nullptr;
jobject java_task_executor = nullptr;

/// Store Java instances of function descriptor in the cache to avoid unnessesary JNI
/// operations.
thread_local std::unordered_map<size_t,
                                std::vector<std::pair<ray::FunctionDescriptor, jobject>>>
    executor_function_descriptor_cache;

inline ray::gcs::GcsClientOptions ToGcsClientOptions(JNIEnv *env,
                                                     jobject gcs_client_options) {
  std::string ip = JavaStringToNativeString(
      env, (jstring)env->GetObjectField(gcs_client_options, java_gcs_client_options_ip));
  int port = env->GetIntField(gcs_client_options, java_gcs_client_options_port);
  std::string password = JavaStringToNativeString(
      env,
      (jstring)env->GetObjectField(gcs_client_options, java_gcs_client_options_password));
  return ray::gcs::GcsClientOptions(ip, port, password);
}

jobject ToJavaArgs(JNIEnv *env, jbooleanArray java_check_results,
                   const std::vector<std::shared_ptr<ray::RayObject>> &args) {
  if (java_check_results == nullptr) {
    // If `java_check_results` is null, it means that `checkByteBufferArguments`
    // failed. In this case, just return null here. The args won't be used anyway.
    return nullptr;
  } else {
    jboolean *check_results = env->GetBooleanArrayElements(java_check_results, nullptr);
    size_t i = 0;
    jobject args_array_list = NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
        env, args,
        [check_results, &i](JNIEnv *env,
                            const std::shared_ptr<ray::RayObject> &native_object) {
          if (*(check_results + (i++))) {
            // If the type of this argument is ByteBuffer, we create a
            // DirectByteBuffer here To avoid data copy.
            // TODO: Check native_object->GetMetadata() == "RAW"
            jobject obj = env->NewDirectByteBuffer(native_object->GetData()->Data(),
                                                   native_object->GetData()->Size());
            RAY_CHECK(obj);
            return obj;
          }
          return NativeRayObjectToJavaNativeRayObject(env, native_object);
        });
    env->ReleaseBooleanArrayElements(java_check_results, check_results, JNI_ABORT);
    return args_array_list;
  }
}

JNIEnv *GetJNIEnv() {
  JNIEnv *env = local_env;
  if (!env) {
    // Attach the native thread to JVM.
    auto status =
        jvm->AttachCurrentThreadAsDaemon(reinterpret_cast<void **>(&env), nullptr);
    RAY_CHECK(status == JNI_OK) << "Failed to get JNIEnv. Return code: " << status;
    local_env = env;
  }
  RAY_CHECK(env);
  return env;
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeInitialize(
    JNIEnv *env, jclass, jint workerMode, jstring nodeIpAddress, jint nodeManagerPort,
    jstring driverName, jstring storeSocket, jstring rayletSocket, jbyteArray jobId,
    jobject gcsClientOptions, jint numWorkersPerProcess, jstring logDir,
    jbyteArray jobConfig, jint worker_shim_pid) {
  auto task_execution_callback =
      [](ray::TaskType task_type, const std::string task_name,
         const ray::RayFunction &ray_function,
         const std::unordered_map<std::string, double> &required_resources,
         const std::vector<std::shared_ptr<ray::RayObject>> &args,
         const std::vector<ObjectID> &arg_reference_ids,
         const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
         std::vector<std::shared_ptr<ray::RayObject>> *results,
         std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb,
         bool *is_application_level_error) {
        // TODO(jjyao): Support retrying application-level errors for Java
        *is_application_level_error = false;

        JNIEnv *env = GetJNIEnv();
        RAY_CHECK(java_task_executor);

        // convert RayFunction
        auto function_descriptor = ray_function.GetFunctionDescriptor();
        size_t fd_hash = function_descriptor->Hash();
        auto &fd_vector = executor_function_descriptor_cache[fd_hash];
        jobject ray_function_array_list = nullptr;
        for (auto &pair : fd_vector) {
          if (pair.first == function_descriptor) {
            ray_function_array_list = pair.second;
            break;
          }
        }
        if (!ray_function_array_list) {
          ray_function_array_list =
              NativeRayFunctionDescriptorToJavaStringList(env, function_descriptor);
          fd_vector.emplace_back(function_descriptor, ray_function_array_list);
        }

        // convert args
        // TODO (kfstorm): Avoid copying binary data from Java to C++
        RAY_LOG(DEBUG)
            << "[JNI]Start invoking Java parse function arguments for function: "
            << function_descriptor->ToString();
        jbooleanArray java_check_results =
            static_cast<jbooleanArray>(env->CallObjectMethod(
                java_task_executor, java_task_executor_parse_function_arguments,
                ray_function_array_list));
        RAY_LOG(DEBUG) << "[JNI]Finished invoking Java parse function arguments.";
        RAY_CHECK_JAVA_EXCEPTION(env);
        jobject args_array_list = ToJavaArgs(env, java_check_results, args);

        // invoke Java method
        RAY_LOG(DEBUG) << "[JNI]Start invoking Java task executor.";
        jobject java_return_objects =
            env->CallObjectMethod(java_task_executor, java_task_executor_execute,
                                  ray_function_array_list, args_array_list);
        RAY_LOG(DEBUG) << "[JNI]Finished invoking Java task executor.";
        // Check whether the exception is `IntentionalSystemExit`.
        jthrowable throwable = env->ExceptionOccurred();
        if (throwable) {
          ray::Status status_to_return = ray::Status::OK();
          if (env->IsInstanceOf(throwable,
                                java_ray_intentional_system_exit_exception_class)) {
            status_to_return = ray::Status::IntentionalSystemExit();
          } else if (env->IsInstanceOf(throwable, java_ray_actor_exception_class)) {
            creation_task_exception_pb = SerializeActorCreationException(env, throwable);
            status_to_return = ray::Status::CreationTaskError();
          } else {
            RAY_CHECK_JAVA_EXCEPTION(env);
          }
          env->ExceptionClear();
          return status_to_return;
        }
        RAY_CHECK_JAVA_EXCEPTION(env);

        // Process return objects.
        if (!return_ids.empty()) {
          std::vector<std::shared_ptr<ray::RayObject>> return_objects;
          JavaListToNativeVector<std::shared_ptr<ray::RayObject>>(
              env, java_return_objects, &return_objects,
              [](JNIEnv *env, jobject java_native_ray_object) {
                return JavaNativeRayObjectToNativeRayObject(env, java_native_ray_object);
              });
          results->resize(return_ids.size(), nullptr);
          for (size_t i = 0; i < return_objects.size(); i++) {
            auto &result_id = return_ids[i];
            size_t data_size =
                return_objects[i]->HasData() ? return_objects[i]->GetData()->Size() : 0;
            auto &metadata = return_objects[i]->GetMetadata();
            auto &contained_object_id = return_objects[i]->GetNestedIds();
            auto result_ptr = &(*results)[0];

            RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
                result_id, data_size, metadata, contained_object_id, result_ptr));

            // A nullptr is returned if the object already exists.
            auto result = *result_ptr;
            if (result != nullptr) {
              if (result->HasData()) {
                memcpy(result->GetData()->Data(), return_objects[i]->GetData()->Data(),
                       data_size);
              }
            }

            RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().SealReturnObject(
                result_id, result));
          }
        }

        env->DeleteLocalRef(java_check_results);
        env->DeleteLocalRef(java_return_objects);
        env->DeleteLocalRef(args_array_list);
        return ray::Status::OK();
      };

  auto gc_collect = [](bool triggered_by_global_gc) {
    // A Java worker process usually contains more than one worker.
    // A LocalGC request is likely to be received by multiple workers in a short time.
    // Here we ensure that the 1 second interval of `System.gc()` execution is
    // guaranteed no matter how frequent the requests are received and how many workers
    // the process has.
    if (!triggered_by_global_gc) {
      RAY_LOG(DEBUG) << "Skipping non-global GC.";
      return;
    }

    static absl::Mutex mutex;
    static int64_t last_gc_time_ms = 0;
    absl::MutexLock lock(&mutex);
    int64_t start = current_time_ms();
    if (last_gc_time_ms + 1000 < start) {
      JNIEnv *env = GetJNIEnv();
      RAY_LOG(DEBUG) << "Calling System.gc() ...";
      env->CallStaticObjectMethod(java_system_class, java_system_gc);
      last_gc_time_ms = current_time_ms();
      RAY_LOG(DEBUG) << "GC finished in " << (double)(last_gc_time_ms - start) / 1000
                     << " seconds.";
    }
  };

  auto on_worker_shutdown = [](const ray::WorkerID &worker_id) {
    JNIEnv *env = GetJNIEnv();
    auto worker_id_bytes = IdToJavaByteArray<ray::WorkerID>(env, worker_id);
    if (java_task_executor) {
      env->CallVoidMethod(java_task_executor,
                          java_native_task_executor_on_worker_shutdown, worker_id_bytes);
      RAY_CHECK_JAVA_EXCEPTION(env);
    }
  };

  std::string serialized_job_config =
      (jobConfig == nullptr ? "" : JavaByteArrayToNativeString(env, jobConfig));
  ray::CoreWorkerOptions options;
  options.worker_type = static_cast<ray::WorkerType>(workerMode);
  options.language = ray::Language::JAVA;
  options.store_socket = JavaStringToNativeString(env, storeSocket);
  options.raylet_socket = JavaStringToNativeString(env, rayletSocket);
  options.job_id = JavaByteArrayToId<ray::JobID>(env, jobId);
  options.gcs_options = ToGcsClientOptions(env, gcsClientOptions);
  options.enable_logging = true;
  options.log_dir = JavaStringToNativeString(env, logDir);
  // TODO (kfstorm): JVM would crash if install_failure_signal_handler was set to true
  options.install_failure_signal_handler = false;
  options.node_ip_address = JavaStringToNativeString(env, nodeIpAddress);
  options.node_manager_port = static_cast<int>(nodeManagerPort);
  options.raylet_ip_address = JavaStringToNativeString(env, nodeIpAddress);
  options.driver_name = JavaStringToNativeString(env, driverName);
  options.stdout_file = "";
  options.stderr_file = "";
  options.task_execution_callback = task_execution_callback;
  options.on_worker_shutdown = on_worker_shutdown;
  options.check_signals = nullptr;
  options.gc_collect = gc_collect;
  options.get_lang_stack = nullptr;
  options.kill_main = nullptr;
  options.ref_counting_enabled = true;
  options.is_local_mode = false;
  options.num_workers = static_cast<int>(numWorkersPerProcess);
  options.terminate_asyncio_thread = nullptr;
  options.serialized_job_config = serialized_job_config;
  options.metrics_agent_port = -1;
  options.worker_shim_pid = worker_shim_pid;
  options.object_allocator =
      [](const ray::RayObject &object,
         const ObjectID &object_id) -> std::shared_ptr<ray::RayObject> {
    if (!object.HasData()) {
      /// This object only has metadata, and doesn't have data. In this case, we can
      /// just use the original RayObject and doesn't have to put in the JVM heap.
      return std::make_shared<ray::RayObject>(object.GetData(), object.GetMetadata(),
                                              object.GetNestedIds(), true);
    }

    JNIEnv *env = GetJNIEnv();
    auto java_byte_array = NativeBufferToJavaByteArray(env, object.GetData());
    auto raw_object_id_byte_array = NativeStringToJavaByteArray(env, object_id.Binary());
    RAY_LOG(DEBUG) << "Allocating Java byte array for object " << object_id;
    env->CallStaticVoidMethod(java_object_ref_impl_class,
                              java_object_ref_impl_class_on_memory_store_object_allocated,
                              raw_object_id_byte_array, java_byte_array);
    auto java_weak_ref = CreateJavaWeakRef(env, java_byte_array);
    // This shared_ptr will be captured by the data_factory. So when the data_factory
    // is destructed, we deference the java_weak_ref.
    std::shared_ptr<void> java_weak_ref_ptr{
        reinterpret_cast<void *>(java_weak_ref), [](auto p) {
          JNIEnv *env = GetJNIEnv();
          env->DeleteLocalRef(reinterpret_cast<jobject>(p));
        }};

    // Remove this local reference because this byte array is fate-sharing with the
    // ObjectRefImpl in Java frontend.
    env->DeleteLocalRef(java_byte_array);
    env->DeleteLocalRef(raw_object_id_byte_array);
    auto data_factory = [java_weak_ref_ptr, object_id]() -> std::shared_ptr<ray::Buffer> {
      JNIEnv *env = GetJNIEnv();
      jbyteArray java_byte_array = (jbyteArray)env->CallObjectMethod(
          reinterpret_cast<jobject>(java_weak_ref_ptr.get()), java_weak_reference_get);
      RAY_CHECK_JAVA_EXCEPTION(env);
      RAY_CHECK(java_byte_array != nullptr)
          << "The java byte array is null of object " << object_id;
      return std::make_shared<JavaByteArrayBuffer>(env, java_byte_array);
    };
    std::shared_ptr<ray::Buffer> metadata_buffer = object.GetMetadata();
    return std::make_shared<ray::RayObject>(metadata_buffer, object.GetNestedIds(),
                                            std::move(data_factory), /*copy_data=*/true);
  };

  ray::CoreWorkerProcess::Initialize(options);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeRunTaskExecutor(
    JNIEnv *env, jclass o, jobject javaTaskExecutor) {
  java_task_executor = javaTaskExecutor;
  ray::CoreWorkerProcess::RunTaskExecutionLoop();
  java_task_executor = nullptr;

  // NOTE(kfstorm): It's possible that users spawn non-daemon Java threads. If these
  // threads are not stopped before exiting `RunTaskExecutionLoop`, the JVM won't exit but
  // Raylet has unregistered this worker. In this case, even if the job has finished, the
  // worker process won't be killed by Raylet and it results in an orphan worker.
  // TO fix this, we explicitly quit the process here. This only affects worker processes,
  // not driver processes because only worker processes call `RunTaskExecutionLoop`.
  _Exit(0);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeShutdown(
    JNIEnv *env, jclass o, jboolean shutdownWithError) {
  ray::CoreWorkerProcess::Shutdown(static_cast<bool>(shutdownWithError));
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetResource(
    JNIEnv *env, jclass, jstring resourceName, jdouble capacity, jbyteArray nodeId) {
  const auto node_id = JavaByteArrayToId<NodeID>(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status = ray::CoreWorkerProcess::GetCoreWorker().SetResource(
      native_resource_name, static_cast<double>(capacity), node_id);
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeGetActorIdOfNamedActor(JNIEnv *env, jclass,
                                                                  jstring actor_name,
                                                                  jstring ray_namespace) {
  const char *native_actor_name = env->GetStringUTFChars(actor_name, JNI_FALSE);
  const char *native_ray_namespace =
      ray_namespace == nullptr
          ? ray::CoreWorkerProcess::GetCoreWorker().GetJobConfig().ray_namespace().c_str()
          : env->GetStringUTFChars(ray_namespace, JNI_FALSE);
  const auto pair = ray::CoreWorkerProcess::GetCoreWorker().GetNamedActorHandle(
      native_actor_name, /*ray_namespace=*/native_ray_namespace);
  const auto status = pair.second;
  if (status.IsNotFound()) {
    return IdToJavaByteArray<ActorID>(env, ActorID::Nil());
  }
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  const auto actor_handle = pair.first;
  return IdToJavaByteArray<ray::ActorID>(env, actor_handle->GetActorID());
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeKillActor(
    JNIEnv *env, jclass, jbyteArray actorId, jboolean noRestart) {
  auto status = ray::CoreWorkerProcess::GetCoreWorker().KillActor(
      JavaByteArrayToId<ActorID>(env, actorId),
      /*force_kill=*/true, noRestart);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jboolean JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeKillJob(
    JNIEnv *env, jclass, jbyteArray jobId) {
  return ray::CoreWorkerProcess::GetCoreWorker().KillJob(
      JavaByteArrayToId<JobID>(env, jobId));
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetCoreWorker(
    JNIEnv *env, jclass, jbyteArray workerId) {
  const auto worker_id = JavaByteArrayToId<ray::WorkerID>(env, workerId);
  ray::CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeReportEvent(
    JNIEnv *env, jclass, jstring severity, jstring label, jstring message) {
  std::string nativeSeverity = JavaStringToNativeString(env, severity);
  std::string nativeLabel = JavaStringToNativeString(env, label);
  std::string nativeMessage = JavaStringToNativeString(env, message);
  ray::RayEvent::ReportEvent(nativeSeverity, nativeLabel, nativeMessage);
}

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeSubscribeActorStateChangeEvents
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeSubscribeActorStateChangeEvents(JNIEnv *env,
                                                                           jclass) {
  auto on_actor_event_arrived = [](const ActorID &actor_id,
                                   const ray::rpc::ActorTableData &actor_data) {
    JNIEnv *env = GetJNIEnv();
    env->CallStaticVoidMethod(java_ray_native_runtime_class,
                              java_ray_native_runtime_class_on_actor_state_changed,
                              env->NewStringUTF(actor_id.Hex().c_str()),
                              static_cast<jint>(actor_data.state()));
    RAY_CHECK_JAVA_EXCEPTION(env);
  };
  return ray::CoreWorkerProcess::SubscribeActorStateChangeEvents(on_actor_event_arrived);
}

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeSubscribeActorMigrationEvents
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeSubscribeActorMigrationEvents(JNIEnv *,
                                                                         jclass) {
  auto on_event_arrived = [](const ray::rpc::ActorMigrationNotificationBatch &event) {
    JNIEnv *env = GetJNIEnv();
    std::string serialized = event.SerializeAsString();
    jbyteArray byte_array = env->NewByteArray(serialized.size());
    env->SetByteArrayRegion(byte_array, 0, serialized.size(),
                            reinterpret_cast<const jbyte *>(serialized.c_str()));
    // Call method
    env->CallStaticVoidMethod(java_ray_native_runtime_class,
                              java_ray_native_runtime_class_on_actor_migration_event,
                              byte_array);
    RAY_CHECK_JAVA_EXCEPTION(env);
  };
  return ray::CoreWorkerProcess::SubscribeActorMigrationEvents(on_event_arrived);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeGetResourceIds(JNIEnv *env, jclass) {
  auto key_converter = [](JNIEnv *env, const std::string &str) -> jstring {
    return env->NewStringUTF(str.c_str());
  };
  auto value_converter =
      [](JNIEnv *env, const std::vector<std::pair<int64_t, double>> &value) -> jobject {
    auto elem_converter = [](JNIEnv *env,
                             const std::pair<int64_t, double> &elem) -> jobject {
      jobject java_item =
          env->NewObject(java_resource_value_class, java_resource_value_init,
                         (jlong)elem.first, (jdouble)elem.second);
      RAY_CHECK_JAVA_EXCEPTION(env);
      return java_item;
    };
    return NativeVectorToJavaList<std::pair<int64_t, double>>(env, value,
                                                              std::move(elem_converter));
  };
  ResourceMappingType resource_mapping =
      (ray::CoreWorkerProcess::GetCoreWorker().GetResourceIDs());
  return NativeMapToJavaMap<std::string, std::vector<std::pair<int64_t, double>>>(
      env, resource_mapping, std::move(key_converter), std::move(value_converter));
}

JNIEXPORT jstring JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeGetNamespace(JNIEnv *env, jclass) {
  return env->NewStringUTF(
      ray::CoreWorkerProcess::GetCoreWorker().GetJobConfig().ray_namespace().c_str());
}

JNIEXPORT jobject JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeGetCurrentReturnIds(
    JNIEnv *env, jclass, jint numReturns, jbyteArray actorIdByteArray) {
  auto &core_worker = ray::CoreWorkerProcess::GetCoreWorker();
  auto return_ids = core_worker.GetCurrentReturnIds(
      static_cast<int>(numReturns),
      JavaByteArrayToId<ray::ActorID>(env, actorIdByteArray));
  return NativeIdVectorToJavaByteArrayList<ray::ObjectID>(env, return_ids);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeGetCurrentNodeId(JNIEnv *env, jclass) {
  const auto &node_id = ray::CoreWorkerProcess::GetCoreWorker().GetCurrentNodeId();
  return IdToJavaByteArray<ray::NodeID>(env, node_id);
}

#ifdef __cplusplus
}
#endif
