#include "message.h"

#include "queue_item.h"

namespace ray {
namespace streaming {

const uint32_t Message::MagicNum = 0xBABA0510;

std::unique_ptr<LocalMemoryBuffer> Message::ToBytes() {
  uint8_t *bytes = nullptr;

  flatbuffers::FlatBufferBuilder fbb;
  ConstructFlatBuf(fbb);
  int64_t fbs_length = fbb.GetSize();

  queue::flatbuf::MessageType type = Type();
  size_t total_len = kItemHeaderSize + fbs_length;
  if (buffer_ != nullptr) {
    total_len += buffer_->Size();
  }
  bytes = new uint8_t[total_len];
  STREAMING_CHECK(bytes != nullptr) << "allocate bytes fail.";

  uint8_t *p_cur = bytes;
  memcpy(p_cur, &Message::MagicNum, sizeof(Message::MagicNum));

  p_cur += sizeof(Message::MagicNum);
  memcpy(p_cur, &type, sizeof(type));

  p_cur += sizeof(type);
  uint64_t timestamp = current_sys_time_ms();
  memcpy(p_cur, &timestamp, sizeof(uint64_t));

  p_cur += sizeof(uint64_t);
  memcpy(p_cur, &fbs_length, sizeof(fbs_length));

  p_cur += sizeof(fbs_length);
  uint8_t *fbs_bytes = fbb.GetBufferPointer();
  memcpy(p_cur, fbs_bytes, fbs_length);
  p_cur += fbs_length;

  if (buffer_ != nullptr) {
    memcpy(p_cur, buffer_->Data(), buffer_->Size());
  }

  // COPY
  auto buffer = std::make_unique<LocalMemoryBuffer>(bytes, total_len, true, true);
  delete[] bytes;
  return buffer;
}

std::unique_ptr<LocalMemoryBuffer> Message::GetMetaBytes() {
  uint8_t *bytes = nullptr;

  flatbuffers::FlatBufferBuilder fbb;
  ConstructFlatBuf(fbb);
  int64_t fbs_length = fbb.GetSize();

  queue::flatbuf::MessageType type = Type();
  size_t total_len = kItemHeaderSize + fbs_length;
  bytes = new uint8_t[total_len];
  STREAMING_CHECK(bytes != nullptr) << "allocate bytes fail.";

  uint8_t *p_cur = bytes;
  memcpy(p_cur, &Message::MagicNum, sizeof(Message::MagicNum));

  p_cur += sizeof(Message::MagicNum);
  memcpy(p_cur, &type, sizeof(type));

  p_cur += sizeof(type);
  uint64_t timestamp = current_sys_time_ms();
  memcpy(p_cur, &timestamp, sizeof(uint64_t));

  p_cur += sizeof(uint64_t);
  memcpy(p_cur, &fbs_length, sizeof(fbs_length));

  p_cur += sizeof(fbs_length);
  uint8_t *fbs_bytes = fbb.GetBufferPointer();
  memcpy(p_cur, fbs_bytes, fbs_length);
  p_cur += fbs_length;

  // COPY
  auto buffer = std::make_unique<LocalMemoryBuffer>(bytes, total_len, true, true);
  delete[] bytes;
  return buffer;
}

flatbuffers::Offset<queue::flatbuf::CommonMsg> Message::ConstructCommonFlatBuf(
    flatbuffers::FlatBufferBuilder &builder) {
  return queue::flatbuf::CreateCommonMsg(builder,
                                         builder.CreateString(actor_id_.Binary()),
                                         builder.CreateString(peer_actor_id_.Binary()),
                                         builder.CreateString(queue_id_.Binary()));
}

void DataMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  std::vector<uint64_t> buffer_length_array;
  if (meta_buffer_) {
    buffer_length_array.push_back(meta_buffer_->Size());
  }
  if (Buffer()) {
    buffer_length_array.push_back(Buffer()->Size());
  }

  auto message = queue::flatbuf::CreateStreamingQueueDataMsg(
      builder, ConstructCommonFlatBuf(builder), seq_id_, msg_id_start_, msg_id_end_,
      timestamp_item_, buffer_length_array.size(),
      builder.CreateVector<uint64_t>(buffer_length_array), resend_, resend_id_,
      resend_reason_);
  builder.Finish(message);
}

DataMessage::DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                         const ObjectID &queue_id, QueueItem &item, bool resend,
                         uint64_t resend_id, queue::flatbuf::ResendReason reason)
    : Message(actor_id, peer_actor_id, queue_id, item.Buffer()),
      seq_id_(item.SeqId()),
      msg_id_start_(item.MsgIdStart()),
      msg_id_end_(item.MsgIdEnd()),
      timestamp_message_(item.TimestampMessage()),
      timestamp_item_(item.TimestampItem()),
      meta_buffer_(item.MetaBuffer()),
      resend_(resend),
      resend_id_(resend_id),
      resend_reason_(reason) {}

std::shared_ptr<DataMessage> DataMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  uint64_t *timestamp_message = (uint64_t *)bytes;
  bytes += sizeof(uint64_t);
  uint64_t *fbs_length = (uint64_t *)bytes;
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueDataMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t msg_id_start = message->msg_id_start();
  uint64_t msg_id_end = message->msg_id_end();
  uint64_t buffer_count = message->buffer_count();
  STREAMING_CHECK(buffer_count == 1) << buffer_count;
  const flatbuffers::Vector<uint64_t> *length_array = message->buffer_length();
  uint64_t timestamp_item = message->timestamp();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " seq_id:" << seq_id
                       << " msg_id_start: " << msg_id_start
                       << " msg_id_end: " << msg_id_end << " queue_id:" << queue_id
                       << " length:" << length_array->Get(0)
                       << " timestamp_message: " << timestamp_message
                       << " timestamp_item: " << timestamp_item;

  bytes += *fbs_length;
  /// COPY
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(bytes, length_array->Get(0), true, true);
  std::shared_ptr<DataMessage> data_msg =
      std::make_shared<DataMessage>(message, nullptr, buffer, *timestamp_message);

  return data_msg;
}

std::shared_ptr<DataMessage> DataMessage::FromBytes(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
  size_t buffer_count = buffers.size();
  STREAMING_CHECK(buffer_count == 2 || buffer_count == 3);
  uint8_t *item_meta = buffers[0]->Data();
  item_meta += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  uint64_t *timestamp_message = (uint64_t *)item_meta;
  item_meta += sizeof(uint64_t);
  item_meta += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueDataMsg>(item_meta);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t msg_id_start = message->msg_id_start();
  uint64_t msg_id_end = message->msg_id_end();
  std::vector<size_t> length_array;
  const flatbuffers::Vector<uint64_t> *length_array_fb = message->buffer_length();
  for (auto it = length_array_fb->begin(); it != length_array_fb->end(); it++) {
    length_array.push_back(*it);
  }
  uint64_t timestamp_item = message->timestamp();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " seq_id:" << seq_id
                       << " msg_id_start: " << msg_id_start
                       << " msg_id_end: " << msg_id_end << " queue_id:" << queue_id
                       << " timestamp_message: " << timestamp_message
                       << " timestamp_item: " << timestamp_item
                       << " buffers[1]->Size(): " << buffers[1]->Size()
                       << " buffers[2]->Size(): " << buffers[1]->Size();

  std::shared_ptr<LocalMemoryBuffer> meta_buffer = nullptr;
  std::shared_ptr<LocalMemoryBuffer> buffer = nullptr;
  if (buffer_count == 3) {
    meta_buffer = std::make_shared<LocalMemoryBuffer>(buffers[1]->Data(),
                                                      buffers[1]->Size(), true, true);
    buffer = std::make_shared<LocalMemoryBuffer>(buffers[2]->Data(), buffers[2]->Size(),
                                                 true, true);
  } else if (buffer_count == 2) {
    meta_buffer = std::make_shared<LocalMemoryBuffer>(buffers[1]->Data(),
                                                      buffers[1]->Size(), true, true);
  } else {
    STREAMING_CHECK(false) << "Unexpected buffer_count " << buffer_count;
  }

  std::shared_ptr<DataMessage> data_msg =
      std::make_shared<DataMessage>(message, meta_buffer, buffer, *timestamp_message);

  return data_msg;
}

void LocalDataMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueLocalDataMsg(
      builder, ConstructCommonFlatBuf(builder), seq_id_, msg_id_start_, msg_id_end_,
      timestamp_item_, data_offset_, data_size_, resend_, resend_id_, resend_reason_,
      builder.CreateString(object_id_.Binary()));
  builder.Finish(message);
}

std::shared_ptr<LocalDataMessage> LocalDataMessage::FromBytes(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
  size_t buffer_count = buffers.size();
  STREAMING_CHECK(buffer_count == 2 || buffer_count == 3)
      << "Unexpected buffer_count: " << buffer_count;
  uint8_t *item_meta = buffers[0]->Data();
  item_meta += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  uint64_t *timestamp_message = (uint64_t *)item_meta;
  item_meta += sizeof(uint64_t);
  item_meta += sizeof(uint64_t);

  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueLocalDataMsg>(item_meta);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t msg_id_start = message->msg_id_start();
  uint64_t msg_id_end = message->msg_id_end();
  uint64_t timestamp_item = message->timestamp();
  ObjectID object_id = ObjectID::FromBinary(message->object_id()->str());
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " seq_id:" << seq_id
                       << " msg_id_start: " << msg_id_start
                       << " msg_id_end: " << msg_id_end << " queue_id:" << queue_id
                       << " timestamp_message: " << timestamp_message
                       << " timestamp_item: " << timestamp_item
                       << " collocate object id: " << object_id;

  std::shared_ptr<LocalMemoryBuffer> meta_buffer = std::make_shared<LocalMemoryBuffer>(
      buffers[1]->Data(), buffers[1]->Size(), true, true);
  std::shared_ptr<LocalDataMessage> data_msg =
      std::make_shared<LocalDataMessage>(message, meta_buffer, *timestamp_message);

  return data_msg;
}

void NotificationMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueNotificationMsg(
      builder, ConstructCommonFlatBuf(builder), msg_id_, bundle_id_);
  builder.Finish(message);
}

std::shared_ptr<NotificationMessage> NotificationMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueNotificationMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t msg_id = message->msg_id();
  uint64_t bundle_id = message->bundle_id();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " msg_id:" << msg_id << " bundle_id:" << msg_id;

  std::shared_ptr<NotificationMessage> notify_msg = std::make_shared<NotificationMessage>(
      src_actor_id, dst_actor_id, queue_id, msg_id, bundle_id);

  return notify_msg;
}

void PullRequestMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueuePullRequestMsg(
      builder, ConstructCommonFlatBuf(builder), checkpoint_id_, msg_id_);
  builder.Finish(message);
}

std::shared_ptr<PullRequestMessage> PullRequestMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueuePullRequestMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t checkpoint_id = message->checkpoint_id();
  uint64_t msg_id = message->msg_id();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " checkpoint_id: " << checkpoint_id << " msg_id:" << msg_id;

  std::shared_ptr<PullRequestMessage> pull_msg = std::make_shared<PullRequestMessage>(
      src_actor_id, dst_actor_id, queue_id, checkpoint_id, msg_id);

  return pull_msg;
}

void PullResponseMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueuePullResponseMsg(
      builder, ConstructCommonFlatBuf(builder), seq_id_, msg_id_, count_, err_code_,
      is_upstream_first_pull_);
  builder.Finish(message);
}

std::shared_ptr<PullResponseMessage> PullResponseMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueuePullResponseMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t msg_id = message->msg_id();
  uint64_t count = message->count();
  queue::flatbuf::StreamingQueueError err_code = message->err_code();
  bool is_upstream_first_pull = message->is_upstream_first_pull();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " seq_id: " << seq_id << " msg_id: " << msg_id
                       << " count: " << count << " err_code:"
                       << queue::flatbuf::EnumNameStreamingQueueError(err_code)
                       << " is_upstream_first_pull: " << is_upstream_first_pull;

  std::shared_ptr<PullResponseMessage> pull_rsp_msg =
      std::make_shared<PullResponseMessage>(src_actor_id, dst_actor_id, queue_id, seq_id,
                                            msg_id, count, err_code,
                                            is_upstream_first_pull);

  return pull_rsp_msg;
}

void GetLastMsgIdMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueGetLastMsgId(
      builder, ConstructCommonFlatBuf(builder));
  builder.Finish(message);
}
std::shared_ptr<GetLastMsgIdMessage> GetLastMsgIdMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;
  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueGetLastMsgId>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;
  std::shared_ptr<GetLastMsgIdMessage> get_last_msg_id_msg =
      std::make_shared<GetLastMsgIdMessage>(src_actor_id, dst_actor_id, queue_id);
  return get_last_msg_id_msg;
}

void GetLastMsgIdRspMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueGetLastMsgIdRsp(
      builder, ConstructCommonFlatBuf(builder), seq_id_, msg_id_, err_code_);
  builder.Finish(message);
}

std::shared_ptr<GetLastMsgIdRspMessage> GetLastMsgIdRspMessage::FromBytes(
    uint8_t *bytes) {
  bytes += kItemHeaderSize;
  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueGetLastMsgIdRsp>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t msg_id = message->msg_id();
  queue::flatbuf::StreamingQueueError err_code = message->err_code();
  STREAMING_LOG(INFO) << "src_actor_id:" << src_actor_id
                      << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;
  std::shared_ptr<GetLastMsgIdRspMessage> get_last_msg_id_rsp_msg =
      std::make_shared<GetLastMsgIdRspMessage>(src_actor_id, dst_actor_id, queue_id,
                                               seq_id, msg_id, err_code);
  return get_last_msg_id_rsp_msg;
}

void GetHostnameMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueGetHostnameMsg(
      builder, ConstructCommonFlatBuf(builder));
  builder.Finish(message);
}

std::shared_ptr<GetHostnameMessage> GetHostnameMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueGetHostnameMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;
  std::shared_ptr<GetHostnameMessage> get_hostname_msg =
      std::make_shared<GetHostnameMessage>(src_actor_id, dst_actor_id, queue_id);
  return get_hostname_msg;
}

void GetHostnameRspMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueGetHostnameRspMsg(
      builder, ConstructCommonFlatBuf(builder), builder.CreateString(hostname_),
      err_code_);
  builder.Finish(message);
}

std::shared_ptr<GetHostnameRspMessage> GetHostnameRspMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;
  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueGetHostnameRspMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->common()->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->common()->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->common()->queue_id()->str());
  std::string ip = message->ip()->str();
  queue::flatbuf::StreamingQueueError err_code = message->err_code();
  STREAMING_LOG(INFO) << "src_actor_id:" << src_actor_id
                      << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;
  std::shared_ptr<GetHostnameRspMessage> get_hostname_rsp_msg =
      std::make_shared<GetHostnameRspMessage>(src_actor_id, dst_actor_id, queue_id, ip,
                                              err_code);
  return get_hostname_rsp_msg;
}

void StartLoggingMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueStartLoggingMsg(
      builder, ConstructCommonFlatBuf(builder), seq_id_, msg_id_, barrier_id_);
  builder.Finish(message);
}

std::shared_ptr<StartLoggingMessage> StartLoggingMessage::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueStartLoggingMsg>(bytes);
  std::shared_ptr<StartLoggingMessage> start_logging_msg =
      std::make_shared<StartLoggingMessage>(message);
  return start_logging_msg;
}

void StartLoggingRspMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueStartLoggingRspMsg(
      builder, ConstructCommonFlatBuf(builder));
  builder.Finish(message);
}

std::shared_ptr<StartLoggingRspMessage> StartLoggingRspMessage::FromBytes(
    uint8_t *bytes) {
  bytes += kItemHeaderSize;
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueStartLoggingRspMsg>(bytes);
  std::shared_ptr<StartLoggingRspMessage> start_logging_rsp_msg =
      std::make_shared<StartLoggingRspMessage>(message);
  return start_logging_rsp_msg;
}

void TestInitMsg::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> queue_id_strs;
  std::transform(queue_ids_.begin(), queue_ids_.end(), std::back_inserter(queue_id_strs),
                 [&builder](const ObjectID &queue_id) {
                   return builder.CreateString(queue_id.Binary());
                 });
  std::vector<flatbuffers::Offset<flatbuffers::String>> rescale_queue_id_strs;
  std::transform(rescale_queue_ids_.begin(), rescale_queue_ids_.end(),
                 std::back_inserter(rescale_queue_id_strs),
                 [&builder](const ObjectID &queue_id) {
                   return builder.CreateString(queue_id.Binary());
                 });
  auto message = queue::flatbuf::CreateStreamingQueueTestInitMsg(
      builder, role_, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(actor_handle_serialized_),
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(queue_id_strs),
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(
          rescale_queue_id_strs),
      builder.CreateString(test_suite_name_), builder.CreateString(test_name_), param_,
      builder.CreateString(plasma_store_socket_));
  builder.Finish(message);
}

std::shared_ptr<TestInitMsg> TestInitMsg::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueTestInitMsg>(bytes);
  queue::flatbuf::StreamingQueueTestRole role = message->role();
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  std::string actor_handle_serialized = message->actor_handle()->str();
  std::vector<ObjectID> queue_ids;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *queue_id_strs =
      message->queue_ids();
  for (auto it = queue_id_strs->begin(); it != queue_id_strs->end(); it++) {
    queue_ids.push_back(ObjectID::FromBinary(it->str()));
  }
  std::vector<ObjectID> rescale_queue_ids;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>
      *rescale_queue_id_strs = message->rescale_queue_ids();
  for (auto it = rescale_queue_id_strs->begin(); it != rescale_queue_id_strs->end();
       it++) {
    rescale_queue_ids.push_back(ObjectID::FromBinary(it->str()));
  }
  std::string test_suite_name = message->test_suite_name()->str();
  std::string test_name = message->test_name()->str();
  uint64_t param = message->param();
  std::string plasma_store_socket = message->plasma_store_socket()->str();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id
                       << " test_suite_name: " << test_suite_name
                       << " test_name: " << test_name;

  std::shared_ptr<TestInitMsg> test_init_msg = std::make_shared<TestInitMsg>(
      role, src_actor_id, dst_actor_id, actor_handle_serialized, queue_ids,
      rescale_queue_ids, test_suite_name, test_name, param, plasma_store_socket);

  return test_init_msg;
}

void TestCheckStatusRspMsg::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueTestCheckStatusRspMsg(
      builder, builder.CreateString(test_name_), status_);
  builder.Finish(message);
}

std::shared_ptr<TestCheckStatusRspMsg> TestCheckStatusRspMsg::FromBytes(uint8_t *bytes) {
  bytes += kItemHeaderSize;

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueTestCheckStatusRspMsg>(bytes);
  std::string test_name = message->test_name()->str();
  bool status = message->status();
  STREAMING_LOG(DEBUG) << "test_name: " << test_name << " status: " << status;

  std::shared_ptr<TestCheckStatusRspMsg> test_check_msg =
      std::make_shared<TestCheckStatusRspMsg>(test_name, status);

  return test_check_msg;
}

}  // namespace streaming
}  // namespace ray
