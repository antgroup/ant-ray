#include "com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl.h"

#include <memory>

#include "data_writer.h"
#include "queue/transport.h"
#include "runtime_context.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

JNIEXPORT jlongArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_getOutputSeqIdNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jobjectArray qIds) {
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  std::vector<ray::ObjectID> queue_vec = jarray_to_plasma_object_id_vec(env, qIds);

  std::vector<uint64_t> result;
  writer_client->GetChannelOffset(queue_vec, result);

  jlongArray jArray = env->NewLongArray(queue_vec.size());
  jlong jdata[queue_vec.size()];
  for (size_t i = 0; i < result.size(); ++i) {
    *(jdata + i) = result[i];
  }
  env->SetLongArrayRegion(jArray, 0, result.size(), jdata);
  return jArray;
}

JNIEXPORT jdoubleArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_getBackPressureRatioNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jobjectArray qIds) {
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  std::vector<ray::ObjectID> queue_vec = jarray_to_plasma_object_id_vec(env, qIds);

  std::vector<double> result;
  writer_client->GetChannelSetBackPressureRatio(queue_vec, result);

  jdoubleArray ratio_array = env->NewDoubleArray(queue_vec.size());
  jdouble jdata[queue_vec.size()];
  for (size_t i = 0; i < result.size(); ++i) {
    *(jdata + i) = result[i];
  }
  env->SetDoubleArrayRegion(ratio_array, 0, result.size(), jdata);
  return ratio_array;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_broadcastBarrierNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jlong checkpointId, jlong barrierId,
    jbyteArray bytes) {
  STREAMING_LOG(INFO) << "jni: broadcast barrier, cp_id=" << checkpointId
                      << ", barrier_id=" << barrierId;
  RawDataFromJByteArray raw_data(env, bytes);
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  writer_client->BroadcastBarrier(checkpointId, barrierId, raw_data.data,
                                  raw_data.data_size);
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_broadcastUDCMsgNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jlong udcMsgId, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "jni: broadcasting UDC msg, msg id=" << udcMsgId;
  RawDataFromJByteArray raw_data(env, bytes);
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  writer_client->BroadcastUDCMsg(udcMsgId, raw_data.data, raw_data.data_size);
};

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_getBufferNative(
    JNIEnv *env, jobject, jlong writer_ptr, jlong qid_ptr, jlong min_size, jlong out) {
  auto *writer_client = reinterpret_cast<DataWriter *>(writer_ptr);
  auto qid = *reinterpret_cast<ray::ObjectID *>(qid_ptr);
  ray::streaming::MemoryBuffer buffer;
  auto status =
      writer_client->GetMemoryBuffer(qid, kMessageHeaderSize + min_size, buffer, false);
  if (status != StreamingStatus::OK) {
    std::stringstream ss;
    ss << "get buffer failed, status code: " << status;
    throwRuntimeException(env, ss.str().c_str());
  }
  // preserve kMessageHeaderSize for message header.
  // The returned buffer shouldn't be used to write barrier.
  *reinterpret_cast<uint64_t *>(out) =
      reinterpret_cast<uint64_t>(buffer.Data()) + kMessageHeaderSize;
  *reinterpret_cast<uint32_t *>(out + 8) =
      static_cast<uint32_t>(buffer.Size() - kMessageHeaderSize);
}

JNIEXPORT jlong JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_writeMessageNative(
    JNIEnv *env, jobject, jlong writer_ptr, jlong qid_ptr, jlong address, jint size) {
  auto *writer_client = reinterpret_cast<DataWriter *>(writer_ptr);
  auto qid = *reinterpret_cast<ray::ObjectID *>(qid_ptr);
  // kMessageHeaderSize bytes is for message header.
  auto data = reinterpret_cast<uint8_t *>(address - kMessageHeaderSize);
  auto data_size = static_cast<uint32_t>(size + kMessageHeaderSize);
  ::ray::streaming::MemoryBuffer buffer(data, data_size);
  jlong result =
      writer_client->WriteMessageToBufferRing(qid, buffer, StreamingMessageType::Message);
  if (result == 0) {
    STREAMING_LOG(INFO) << "producer interrupted, return 0.";
    throwQueueInterruptException(env, "producer interrupted.");
  }
  return result;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_stopProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  STREAMING_LOG(INFO) << "jni: stop producer.";
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  writer_client->Stop();
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_closeProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  delete writer_client;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_clearCheckpointNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jlong state_cp_id, jlong checkpoint_id) {
  STREAMING_LOG(INFO) << "[Producer] jni: clearCheckpoints.";
  auto *writer = reinterpret_cast<DataWriter *>(ptr);
  writer->ClearCheckpoint(state_cp_id, checkpoint_id);
  STREAMING_LOG(INFO) << "[Producer] clear checkpoint done.";
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_hotUpdateProducerNative(
    JNIEnv *env, jobject obj, jlong ptr, jobjectArray id_array,
    jobjectArray update_target_ids, jobject streaming_queue_initial_parameters) {
  auto writer = reinterpret_cast<DataWriter *>(ptr);
  std::vector<ray::ObjectID> output_vect = jarray_to_plasma_object_id_vec(env, id_array);
  std::vector<ray::ObjectID> target_id_vec =
      jarray_to_plasma_object_id_vec(env, update_target_ids);

  std::vector<StreamingQueueInitialParameter> init_parameter_vec;
  ParseStreamingQueueInitParameters(env, streaming_queue_initial_parameters,
                                    init_parameter_vec);
  writer->Rescale(output_vect, target_id_vec, init_parameter_vec);
}

/*
 * Class:     com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl
 * Method:    broadcastPartialBarrierNative
 * Signature: (JJJ[B)V
 */
JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_broadcastPartialBarrierNative(
    JNIEnv *env, jobject obj, jlong ptr, jlong global_barrier_id,
    jlong partial_barrier_id, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "[Producer] jni: broadcastPartialBarrier.";
  auto writer = reinterpret_cast<DataWriter *>(ptr);
  RawDataFromJByteArray raw_data(env, bytes);
  writer->BroadcastPartialBarrier(global_barrier_id, partial_barrier_id, raw_data.data,
                                  raw_data.data_size);
  STREAMING_LOG(INFO) << "[Producer] broadcastPartialBarrier done.";
}

/*
 * Class:     com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl
 * Method:    clearPartialCheckpointNative
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_clearPartialCheckpointNative(
    JNIEnv *env, jobject obj, jlong ptr, jlong global_barrier_id,
    jlong partial_barrier_id) {
  STREAMING_LOG(INFO) << "[Producer] jni: clearPartialBarrier.";
  auto writer = reinterpret_cast<DataWriter *>(ptr);
  writer->ClearPartialCheckpoint(global_barrier_id, partial_barrier_id);
  STREAMING_LOG(INFO) << "[Producer] clearPartialBarrier done.";
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_rescaleRollbackNative(
    JNIEnv *env, jobject obj, jlong ptr) {
  STREAMING_LOG(INFO) << "[Producer] jni: rescale rollback.";
  auto writer = reinterpret_cast<DataWriter *>(ptr);
  writer->RescaleRollback();
  STREAMING_LOG(INFO) << "[Producer] rescale rollback done.";
}

JNIEXPORT jlong JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_newProducer(
    JNIEnv *env, jobject this_obj, jobject streaming_queue_initial_parameters,
    jobjectArray output_queue_ids,  // byte[][]
    jlongArray msg_ids, jlong queue_size, jbyteArray pb_bytes) {
  STREAMING_LOG(INFO) << "[JNI]: newProducer.";

  std::vector<ray::ObjectID> queue_id_vec =
      jarray_to_plasma_object_id_vec(env, output_queue_ids);
  for (auto qid : queue_id_vec) {
    STREAMING_LOG(INFO) << "output qid: " << qid.Hex();
  }
  STREAMING_LOG(INFO) << "total queue size: " << queue_size << "*" << queue_id_vec.size()
                      << "=" << queue_id_vec.size() * queue_size;
  LongVectorFromJLongArray long_array_obj(env, msg_ids);

  std::vector<uint64_t> queue_size_vec(long_array_obj.data.size(), queue_size);

  std::vector<ray::streaming::StreamingQueueInitialParameter> parameter_vec;
  const jbyte *pb_conf_bytes = env->GetByteArrayElements(pb_bytes, 0);
  uint32_t pb_len = env->GetArrayLength(pb_bytes);
  auto runtime_context = std::make_shared<RuntimeContext>();
  if (pb_conf_bytes) {
    STREAMING_LOG(INFO) << "jni : load config from protobuf, len => " << pb_len;
    runtime_context->SetConfig(reinterpret_cast<const uint8_t *>(pb_conf_bytes), pb_len);
  }
  if (runtime_context->config_.IsStreamingQueueEnabled()) {
    ParseStreamingQueueInitParameters(env, streaming_queue_initial_parameters,
                                      parameter_vec);
  } else {
    STREAMING_LOG(INFO) << "Jni : use mock queue for test.";
    parameter_vec.resize(queue_id_vec.size());
  }

  auto *streaming_writer = new DataWriter(runtime_context);
  StreamingStatus status = streaming_writer->Init(queue_id_vec, parameter_vec,
                                                  long_array_obj.data, queue_size_vec);
  STREAMING_LOG(INFO) << "init producer done, status =>" << static_cast<uint32_t>(status);
  streaming_writer->Run();

  return reinterpret_cast<jlong>(streaming_writer);
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_onWriterMessageNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jlong data_ptr, jint data_size) {
  auto writer = reinterpret_cast<DataWriter *>(ptr);

  std::shared_ptr<LocalMemoryBuffer> buffer = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data_ptr), data_size, true, true);
  std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers{buffer};
  writer->OnMessage(buffers);
}

JNIEXPORT jbyteArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_onWriterMessageSyncNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jlong data_ptr, jint data_size) {
  auto writer = reinterpret_cast<DataWriter *>(ptr);

  std::shared_ptr<LocalMemoryBuffer> buffer = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data_ptr), data_size, true, true);
  std::shared_ptr<LocalMemoryBuffer> result_buffer = writer->OnMessageSync(buffer);

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(),
                          reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_broadcastEndOfDataBarrierNative(
    JNIEnv *env, jobject obj, jlong ptr) {
  DataWriter *writer_client = reinterpret_cast<DataWriter *>(ptr);
  writer_client->BroadcastEndOfDataBarrier();
}

JNIEXPORT jstring JNICALL
Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_QueueProducerImpl_getProducerProfilingInfoNative(
    JNIEnv *env, jobject this_obj, jlong ptr) {
  auto writer = reinterpret_cast<DataWriter *>(ptr);
  std::string debug_infos = writer->GetProfilingInfo();
  return NativeStringTOJavaString(env, debug_infos);
}
