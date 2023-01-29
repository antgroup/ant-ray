#include "streaming_jni_common.h"

#include "ray/common/function_descriptor.h"

jclass java_direct_buffer_class;
jfieldID java_direct_buffer_address;
jfieldID java_direct_buffer_capacity;

std::vector<ray::ObjectID> jarray_to_plasma_object_id_vec(JNIEnv *env,
                                                          jobjectArray jarr) {
  int stringCount = env->GetArrayLength(jarr);
  std::vector<ray::ObjectID> object_id_vec;
  for (int i = 0; i < stringCount; i++) {
    auto jstr = (jbyteArray)(env->GetObjectArrayElement(jarr, i));
    UniqueIdFromJByteArray idFromJByteArray(env, jstr);
    object_id_vec.push_back(idFromJByteArray.PID);
  }
  return object_id_vec;
}

jint throwRuntimeException(JNIEnv *env, const char *message) {
  jclass exClass;
  char className[] = "java/lang/RuntimeException";
  exClass = env->FindClass(className);
  return env->ThrowNew(exClass, message);
}

jint throwQueueInitException(JNIEnv *env, const char *message,
                             const std::vector<ray::ObjectID> &abnormal_queues) {
  jclass array_list_class = env->FindClass("java/util/ArrayList");
  jmethodID array_list_constructor = env->GetMethodID(array_list_class, "<init>", "()V");
  jmethodID array_list_add =
      env->GetMethodID(array_list_class, "add", "(Ljava/lang/Object;)Z");
  jobject array_list = env->NewObject(array_list_class, array_list_constructor);

  for (auto &q_id : abnormal_queues) {
    jbyteArray jbyte_array = env->NewByteArray(kUniqueIDSize);
    env->SetByteArrayRegion(jbyte_array, 0, kUniqueIDSize,
                            reinterpret_cast<const jbyte *>(q_id.Data()));
    env->CallBooleanMethod(array_list, array_list_add, jbyte_array);
  }

  jclass ex_class = env->FindClass(
      "com/alipay/streaming/runtime/queue/impl/streamingqueue/exception/"
      "QueueInitException");
  jmethodID ex_constructor =
      env->GetMethodID(ex_class, "<init>", "(Ljava/lang/String;Ljava/util/List;)V");
  jstring message_jstr = env->NewStringUTF(message);
  jobject ex_obj = env->NewObject(ex_class, ex_constructor, message_jstr, array_list);
  env->DeleteLocalRef(message_jstr);
  return env->Throw((jthrowable)ex_obj);
}

jint throwQueueInterruptException(JNIEnv *env, const char *message) {
  jclass ex_class = env->FindClass(
      "com/alipay/streaming/runtime/queue/impl/streamingqueue/exception/"
      "QueueInterruptException");
  return env->ThrowNew(ex_class, message);
}

jclass LoadClass(JNIEnv *env, const char *class_name) {
  jclass tempLocalClassRef = env->FindClass(class_name);
  jclass ret = (jclass)env->NewGlobalRef(tempLocalClassRef);
  STREAMING_CHECK(ret) << "Can't load Java class " << class_name;
  env->DeleteLocalRef(tempLocalClassRef);
  return ret;
}

/// Convert a Java List to C++ std::vector.
template <typename NativeT>
void JavaListToNativeVector(JNIEnv *env, jobject java_list,
                            std::vector<NativeT> *native_vector,
                            std::function<NativeT(JNIEnv *, jobject)> element_converter) {
  jclass java_list_class = LoadClass(env, "java/util/List");
  jmethodID java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  jmethodID java_list_get =
      env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  int size = env->CallIntMethod(java_list, java_list_size);
  native_vector->clear();
  for (int i = 0; i < size; i++) {
    native_vector->emplace_back(
        element_converter(env, env->CallObjectMethod(java_list, java_list_get, (jint)i)));
  }
}

/// Convert a Java String to C++ std::string.
std::string JavaStringToNativeString(JNIEnv *env, jstring jstr) {
  const char *c_str = env->GetStringUTFChars(jstr, nullptr);
  std::string result(c_str);
  env->ReleaseStringUTFChars(static_cast<jstring>(jstr), c_str);
  return result;
}

/// Convert a Java List<String> to C++ std::vector<std::string>.
void JavaStringListToNativeStringVector(JNIEnv *env, jobject java_list,
                                        std::vector<std::string> *native_vector) {
  JavaListToNativeVector<std::string>(
      env, java_list, native_vector, [](JNIEnv *env, jobject jstr) {
        return JavaStringToNativeString(env, static_cast<jstring>(jstr));
      });
}

/// Convert a Java byte array to a C++ UniqueID.
template <typename ID>
inline ID JavaByteArrayToId(JNIEnv *env, const jbyteArray &bytes) {
  std::string id_str(ID::Size(), 0);
  env->GetByteArrayRegion(bytes, 0, ID::Size(),
                          reinterpret_cast<jbyte *>(&id_str.front()));
  return ID::FromBinary(id_str);
}

std::shared_ptr<ray::RayFunction> FunctionDescriptorToRayFunction(
    JNIEnv *env, jobject functionDescriptor) {
  jclass java_language_class = LoadClass(env, "io/ray/runtime/generated/Common$Language");
  jclass java_function_descriptor_class =
      LoadClass(env, "io/ray/runtime/functionmanager/FunctionDescriptor");
  jmethodID java_language_get_number =
      env->GetMethodID(java_language_class, "getNumber", "()I");
  jmethodID java_function_descriptor_get_language =
      env->GetMethodID(java_function_descriptor_class, "getLanguage",
                       "()Lio/ray/runtime/generated/Common$Language;");
  jobject java_language =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_get_language);
  ray::Language language = static_cast<::Language>(
      env->CallIntMethod(java_language, java_language_get_number));

  std::vector<std::string> function_descriptor;
  jmethodID java_function_descriptor_to_list =
      env->GetMethodID(java_function_descriptor_class, "toList", "()Ljava/util/List;");
  JavaStringListToNativeStringVector(
      env, env->CallObjectMethod(functionDescriptor, java_function_descriptor_to_list),
      &function_descriptor);

  return std::make_shared<ray::RayFunction>(
      language,
      ray::FunctionDescriptorBuilder::FromVector(language, function_descriptor));
}

void FunctionDescriptorListToRayFunctionVector(
    JNIEnv *env, jobject java_list,
    std::vector<std::shared_ptr<ray::RayFunction>> *native_vector) {
  JavaListToNativeVector<std::shared_ptr<ray::RayFunction>>(
      env, java_list, native_vector, [](JNIEnv *env, jobject func) {
        return FunctionDescriptorToRayFunction(env, func);
      });
}

void ParseStreamingQueueInitParameters(
    JNIEnv *env, jobject param_obj,
    std::vector<ray::streaming::StreamingQueueInitialParameter> &parameter_vec) {
  jclass java_streaming_queue_initial_parameters_class =
      LoadClass(env,
                "com/alipay/streaming/runtime/queue/impl/streamingqueue/"
                "StreamingQueueInitialParameters");
  jmethodID java_streaming_queue_initial_parameters_getParameters_method =
      env->GetMethodID(java_streaming_queue_initial_parameters_class, "getParameters",
                       "()Ljava/util/List;");
  STREAMING_CHECK(java_streaming_queue_initial_parameters_getParameters_method !=
                  nullptr);
  jclass java_streaming_queue_initial_parameters_parameter_class =
      LoadClass(env,
                "com/alipay/streaming/runtime/queue/impl/streamingqueue/"
                "StreamingQueueInitialParameters$Parameter");
  jmethodID java_getActorIdBytes_method = env->GetMethodID(
      java_streaming_queue_initial_parameters_parameter_class, "getActorIdBytes", "()[B");
  jmethodID java_getAsyncFunctionDescriptor_method =
      env->GetMethodID(java_streaming_queue_initial_parameters_parameter_class,
                       "getAsyncFunctionDescriptor",
                       "()Lio/ray/runtime/functionmanager/FunctionDescriptor;");
  jmethodID java_getSyncFunctionDescriptor_method =
      env->GetMethodID(java_streaming_queue_initial_parameters_parameter_class,
                       "getSyncFunctionDescriptor",
                       "()Lio/ray/runtime/functionmanager/FunctionDescriptor;");
  jmethodID java_getCyclic_method = env->GetMethodID(
      java_streaming_queue_initial_parameters_parameter_class, "getCyclic", "()Z");
  // Call getParameters method
  jobject parameter_list = env->CallObjectMethod(
      param_obj, java_streaming_queue_initial_parameters_getParameters_method);

  JavaListToNativeVector<ray::streaming::StreamingQueueInitialParameter>(
      env, parameter_list, &parameter_vec,
      [java_getActorIdBytes_method, java_getAsyncFunctionDescriptor_method,
       java_getSyncFunctionDescriptor_method,
       java_getCyclic_method](JNIEnv *env, jobject jobject_parameter) {
        ray::streaming::StreamingQueueInitialParameter native_parameter;
        jbyteArray jobject_actor_id_bytes = (jbyteArray)env->CallObjectMethod(
            jobject_parameter, java_getActorIdBytes_method);
        native_parameter.actor_id =
            JavaByteArrayToId<ray::ActorID>(env, jobject_actor_id_bytes);
        jobject jobject_async_func = env->CallObjectMethod(
            jobject_parameter, java_getAsyncFunctionDescriptor_method);
        native_parameter.async_function =
            FunctionDescriptorToRayFunction(env, jobject_async_func);
        jobject jobject_sync_func = env->CallObjectMethod(
            jobject_parameter, java_getSyncFunctionDescriptor_method);
        native_parameter.sync_function =
            FunctionDescriptorToRayFunction(env, jobject_sync_func);
        native_parameter.cyclic =
            env->CallBooleanMethod(jobject_parameter, java_getCyclic_method) == JNI_TRUE;
        return native_parameter;
      });
}

jstring NativeStringTOJavaString(JNIEnv *env, const std::string &native_str) {
  jclass jstrObj = env->FindClass("java/lang/String");
  jmethodID methodId = env->GetMethodID(jstrObj, "<init>", "([BLjava/lang/String;)V");
  jbyteArray byteArray = env->NewByteArray(native_str.length());
  jstring encode = env->NewStringUTF("utf-8");
  env->SetByteArrayRegion(byteArray, 0, native_str.length(), (jbyte *)native_str.c_str());

  return (jstring)env->NewObject(jstrObj, methodId, byteArray, encode);
}

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), CURRENT_JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  java_direct_buffer_class = FindClass(env, "java/nio/DirectByteBuffer");
  java_direct_buffer_address = env->GetFieldID(java_direct_buffer_class, "address", "J");
  STREAMING_CHECK(java_direct_buffer_address != nullptr);
  java_direct_buffer_capacity =
      env->GetFieldID(java_direct_buffer_class, "capacity", "I");
  STREAMING_CHECK(java_direct_buffer_capacity != nullptr);

  return CURRENT_JNI_VERSION;
}
