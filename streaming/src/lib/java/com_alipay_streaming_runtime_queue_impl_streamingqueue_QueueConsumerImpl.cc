#include "com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl.h"

#include <cstdlib>

#include "data_reader.h"
#include "queue/transport.h"
#include "runtime_context.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;
using namespace ray;

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_clearCheckpointNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jobjectArray qids, jlongArray offsets) {
  STREAMING_LOG(INFO) << "jni: clearCheckpoints.";
  STREAMING_LOG(INFO) << "clear checkpoint done.";
}

JNIEXPORT jboolean JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_getBundleNative(
    JNIEnv *env, jobject, jlong ptr, jlong timeoutMillis, jlong out, jlong meta_addr) {
  std::shared_ptr<ray::streaming::StreamingReaderBundle> msg;
  auto reader = reinterpret_cast<ray::streaming::DataReader *>(ptr);
  auto status = reader->GetBundle((uint32_t)timeoutMillis, msg);

  // over timeout, return empty array.
  if (StreamingStatus::Interrupted == status) {
    throwQueueInterruptException(env, "consumer interrupted.");
  } else if (StreamingStatus::GetBundleTimeOut == status) {
  } else if (StreamingStatus::InitQueueFailed == status) {
    throwRuntimeException(env, "init queue failed");
  }

  if (StreamingStatus::OK != status) {
    *reinterpret_cast<uint64_t *>(out) = 0;
    *reinterpret_cast<uint32_t *>(out + 8) = 0;
    return false;
  }

  // bundle data
  // In streaming queue, bundle data and metadata will be different args of direct call,
  // so we separate it here for future extensibility.

  *reinterpret_cast<uint64_t *>(out) = reinterpret_cast<uint64_t>(msg->DataBuffer());
  *reinterpret_cast<uint32_t *>(out + 8) = msg->DataSize();

  // bundle metadata
  auto meta = reinterpret_cast<uint8_t *>(meta_addr);
  // bundle header written by writer
  std::memcpy(meta, msg->MetaBuffer(), kMessageBundleHeaderSize);
  // append qid
  std::memcpy(meta + kMessageBundleHeaderSize, msg->from.Data(), kUniqueIDSize);
  return true;
}

JNIEXPORT jbyteArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_getOffsetsInfo(
    JNIEnv *env, jobject, jlong ptr) {
  auto reader = reinterpret_cast<ray::streaming::DataReader *>(ptr);
  std::unordered_map<ray::ObjectID, ConsumerChannelInfo> *offset_map = nullptr;
  reader->GetOffsetInfo(offset_map);
  STREAMING_CHECK(offset_map);
  // queue nums + (plasma queue id + seq id + message id) * queue nums
  int offset_data_size =
      sizeof(uint32_t) + (kUniqueIDSize + sizeof(uint64_t) * 2) * offset_map->size();
  jbyteArray offsets_info = env->NewByteArray(offset_data_size);
  int offset = 0;
  // total queue nums
  auto queue_nums = static_cast<uint32_t>(offset_map->size());
  env->SetByteArrayRegion(offsets_info, offset, sizeof(uint32_t),
                          reinterpret_cast<jbyte *>(&queue_nums));
  offset += sizeof(uint32_t);
  // queue name & offset
  for (auto &p : *offset_map) {
    env->SetByteArrayRegion(offsets_info, offset, kUniqueIDSize,
                            reinterpret_cast<const jbyte *>(p.first.Data()));
    offset += kUniqueIDSize;
    // msg_id
    env->SetByteArrayRegion(offsets_info, offset, sizeof(uint64_t),
                            reinterpret_cast<jbyte *>(&p.second.current_message_id));
    offset += sizeof(uint64_t);
  }
  return offsets_info;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_stopConsumerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  auto reader = reinterpret_cast<DataReader *>(ptr);
  reader->Stop();
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_closeConsumerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  delete reinterpret_cast<DataReader *>(ptr);
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_hotUpdateConsumerNative(
    JNIEnv *env, jobject obj, jlong ptr, jobjectArray id_array,
    jobjectArray to_collecting_id_array, jobject streaming_queue_initial_parameters) {
  auto reader = reinterpret_cast<DataReader *>(ptr);
  std::vector<ray::ObjectID> input_vec = jarray_to_plasma_object_id_vec(env, id_array);
  std::vector<ray::ObjectID> to_collect_vec =
      jarray_to_plasma_object_id_vec(env, to_collecting_id_array);

  std::vector<StreamingQueueInitialParameter> init_parameter_vec;
  ParseStreamingQueueInitParameters(env, streaming_queue_initial_parameters,
                                    init_parameter_vec);

  reader->Rescale(input_vec, to_collect_vec, init_parameter_vec);
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_clearPartialCheckpointNative(
    JNIEnv *env, jobject obj, jlong ptr, jlong global_barrier_id,
    jlong partial_barrier_id) {
  STREAMING_LOG(INFO) << "[Consumer] jni: clearPartialCheckpoint.";
  auto reader = reinterpret_cast<DataReader *>(ptr);
  reader->ClearPartialCheckpoint(global_barrier_id, partial_barrier_id);
  STREAMING_LOG(INFO) << "[Consumer] clearPartialCheckpoint done.";
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_rescaleRollbackNative(
    JNIEnv *env, jobject obj, jlong ptr) {
  STREAMING_LOG(INFO) << "[Consumer] jni: rescale rollback.";
  auto reader = reinterpret_cast<DataReader *>(ptr);
  reader->RescaleRollback();
  STREAMING_LOG(INFO) << "[Consumer] rescale rollback done.";
}

JNIEXPORT jlong JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_newConsumer(
    JNIEnv *env, jobject this_obj, jobject streaming_queue_initial_parameters,
    jobjectArray input_queue_id_array,  // byte[][]
    jlongArray msg_id_array, jlong timer_interval, jobject creation_status,
    jbyteArray pb_bytes) {
  STREAMING_LOG(INFO) << "[JNI]: newConsumer.";

  // parsing args
  std::vector<ray::ObjectID> input_queue_ids =
      jarray_to_plasma_object_id_vec(env, input_queue_id_array);
  std::vector<uint64_t> streaming_msg_ids =
      LongVectorFromJLongArray(env, msg_id_array).data;

  std::vector<ray::streaming::StreamingQueueInitialParameter> parameter_vec;
  // set conf
  const jbyte *pb_conf_bytes = env->GetByteArrayElements(pb_bytes, 0);
  uint32_t pb_len = env->GetArrayLength(pb_bytes);
  auto runtime_context = std::make_shared<RuntimeContext>();
  if (pb_conf_bytes) {
    STREAMING_LOG(INFO) << "jni : load config from protobuf, len => " << pb_len;
    runtime_context->SetConfig(reinterpret_cast<const uint8_t *>(pb_conf_bytes), pb_len);
  }
  if (runtime_context->GetConfig().IsStreamingQueueEnabled()) {
    ParseStreamingQueueInitParameters(env, streaming_queue_initial_parameters,
                                      parameter_vec);
  } else {
    STREAMING_LOG(INFO) << "Jni : use mock queue for test.";
    parameter_vec.resize(input_queue_ids.size());
  }

  // create reader
  auto *streaming_reader = new DataReader(runtime_context);

  // init reader
  std::vector<TransferCreationStatus> creation_status_vec;
  streaming_reader->Init(input_queue_ids, parameter_vec, streaming_msg_ids,
                         timer_interval, creation_status_vec);

  // report queue creation status
  jclass array_list_cls = env->GetObjectClass(creation_status);
  jclass integer_cls = env->FindClass("java/lang/Integer");
  jmethodID array_list_add =
      env->GetMethodID(array_list_cls, "add", "(Ljava/lang/Object;)Z");
  for (auto &status : creation_status_vec) {
    jmethodID integer_init = env->GetMethodID(integer_cls, "<init>", "(I)V");
    jobject integer_obj =
        env->NewObject(integer_cls, integer_init, static_cast<int>(status));
    env->CallBooleanMethod(creation_status, array_list_add, integer_obj);
  }

  // return pointer of reader
  return reinterpret_cast<jlong>(streaming_reader);
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_onReaderMessageNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jlong item_meta_ptr, jint item_meta_size,
    jlong bundle_item_ptr, jint bundle_item_size, jlong data_ptr, jint data_size) {
  auto reader = reinterpret_cast<DataReader *>(ptr);

  std::shared_ptr<ray::streaming::LocalMemoryBuffer> item_meta_buffer =
      std::make_shared<ray::streaming::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(item_meta_ptr), item_meta_size, true);
  std::shared_ptr<ray::streaming::LocalMemoryBuffer> bundle_meta_buffer =
      std::make_shared<ray::streaming::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(bundle_item_ptr), bundle_item_size, true);
  std::shared_ptr<ray::streaming::LocalMemoryBuffer> data_buffer =
      std::make_shared<ray::streaming::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(data_ptr), data_size, true);
  std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers{
      item_meta_buffer, bundle_meta_buffer, data_buffer};
  reader->OnMessage(buffers);
}

JNIEXPORT jbyteArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_onReaderMessageSyncNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jlong data_ptr, jint data_size) {
  auto reader = reinterpret_cast<DataReader *>(ptr);

  std::shared_ptr<ray::streaming::LocalMemoryBuffer> buffer =
      std::make_shared<ray::streaming::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(data_ptr), data_size, true);
  std::shared_ptr<ray::streaming::LocalMemoryBuffer> result_buffer =
      reader->OnMessageSync(buffer);

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(),
                          reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}

JNIEXPORT jstring JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueConsumerImpl_getConsumerProfilingInfoNative(
    JNIEnv *env, jobject this_obj, jlong ptr) {
  auto reader = reinterpret_cast<DataReader *>(ptr);
  std::string debug_infos = reader->GetProfilingInfo();
  return NativeStringTOJavaString(env, debug_infos);
}
