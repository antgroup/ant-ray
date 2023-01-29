#include "message_bundle.h"

#include <cstring>
#include <string>

#include "logging.h"
#include "streaming_config.h"

namespace ray {
namespace streaming {
StreamingMessageBundleMeta::StreamingMessageBundleMeta(const uint8_t *bytes) {
  std::memcpy(GetFirstMemberAddress(), bytes,
              kMessageBundleMetaHeaderSize - sizeof(uint32_t));
}

StreamingMessageBundleMeta::StreamingMessageBundleMeta(
    uint64_t message_bundle_ts, uint64_t first_message_id, uint64_t last_message_id,
    uint32_t message_list_size, StreamingMessageBundleType bundle_type)
    : message_bundle_ts_(message_bundle_ts),
      first_message_id_(first_message_id),
      last_message_id_(last_message_id),
      message_list_size_(message_list_size),
      bundle_type_(bundle_type) {
  STREAMING_CHECK(message_list_size <= StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE);
}

void StreamingMessageBundleMeta::ToBytes(uint8_t *bytes) {
  uint32_t magicNum = StreamingMessageBundleMeta::StreamingMessageBundleMagicNum;
  std::memcpy(bytes, reinterpret_cast<const uint8_t *>(&magicNum), sizeof(uint32_t));
  std::memcpy(bytes + sizeof(uint32_t), GetFirstMemberAddress(),
              kMessageBundleMetaHeaderSize - sizeof(uint32_t));
}

StreamingMessageBundleMetaPtr StreamingMessageBundleMeta::FromBytes(const uint8_t *bytes,
                                                                    bool check) {
  STREAMING_CHECK(bytes);

  uint32_t byte_offset = 0;
  STREAMING_CHECK(CheckBundleMagicNum(bytes));
  byte_offset += sizeof(uint32_t);

  auto result = std::make_shared<StreamingMessageBundleMeta>(bytes + byte_offset);
  STREAMING_CHECK(result->GetMessageListSize() <=
                  StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE);
  return result;
}

bool StreamingMessageBundleMeta::operator==(StreamingMessageBundleMeta &meta) const {
  return this->message_list_size_ == meta.GetMessageListSize() &&
         this->message_bundle_ts_ == meta.GetMessageBundleTs() &&
         this->bundle_type_ == meta.GetBundleType() &&
         this->last_message_id_ == meta.GetLastMessageId() &&
         this->first_message_id_ == meta.GetFirstMessageId();
}

bool StreamingMessageBundleMeta::operator==(StreamingMessageBundleMeta *meta) const {
  return operator==(*meta);
}

StreamingMessageBundleMeta::StreamingMessageBundleMeta(
    StreamingMessageBundleMeta *meta_ptr) {
  bundle_type_ = meta_ptr->bundle_type_;
  first_message_id_ = meta_ptr->first_message_id_;
  last_message_id_ = meta_ptr->last_message_id_;
  message_bundle_ts_ = meta_ptr->message_bundle_ts_;
  message_list_size_ = meta_ptr->message_list_size_;
}

StreamingMessageBundleMeta::StreamingMessageBundleMeta()
    : message_bundle_ts_(0),
      first_message_id_(0),
      last_message_id_(0),
      message_list_size_(0),
      bundle_type_(StreamingMessageBundleType::Empty) {}

std::ostream &operator<<(std::ostream &os, const StreamingMessageBundleMeta &meta) {
  os << "{"
     << ", first_message_id: " << meta.first_message_id_
     << ", last_message_id_: " << meta.last_message_id_
     << ", message_list_size_: " << meta.message_list_size_
     << ", bundle_type_: " << static_cast<int>(meta.bundle_type_) << "}";
  return os;
}

StreamingMessageBundle::StreamingMessageBundle(uint64_t first_message_id,
                                               uint64_t last_message_id,
                                               uint64_t message_bundle_ts)
    : StreamingMessageBundleMeta(message_bundle_ts, first_message_id, last_message_id, 0,
                                 StreamingMessageBundleType::Empty) {
  // empty message
  this->raw_bundle_size_ = 0;
}

StreamingMessageBundle::StreamingMessageBundle(
    std::list<StreamingMessagePtr> &&message_list, uint64_t message_ts,
    uint64_t first_message_id, uint64_t last_message_id,
    StreamingMessageBundleType bundle_type, uint32_t raw_data_size)
    : StreamingMessageBundleMeta(message_ts, first_message_id, last_message_id,
                                 message_list.size(), bundle_type),
      message_list_(message_list) {
  uint32_t data_size = 0;
  if (bundle_type_ != StreamingMessageBundleType::Empty) {
    STREAMING_CHECK(!message_list.empty());
    auto data_start = reinterpret_cast<uint64_t>(message_list.front()->Data());
    uint64_t last_msg_data_end = data_start;
    for (auto &msg_ptr : message_list) {
      auto start_addr = reinterpret_cast<uint64_t>(msg_ptr->Data());
      if (start_addr != last_msg_data_end) {
        size_t size = last_msg_data_end - data_start;
        MemoryBuffer buffer(reinterpret_cast<uint8_t *>(data_start), size);
        data_buffers_.AddBuffer(buffer);
        data_start = start_addr;
        last_msg_data_end = start_addr + msg_ptr->Size();
      } else {
        last_msg_data_end = start_addr + msg_ptr->Size();
      }
      data_size += msg_ptr->Size();
    }
    size_t size = last_msg_data_end - data_start;
    MemoryBuffer buffer(reinterpret_cast<uint8_t *>(data_start), size);
    data_buffers_.AddBuffer(buffer);
  }
  if (raw_data_size) {
    STREAMING_CHECK(raw_data_size == data_size)
        << "raw_data_size " << raw_data_size << " data_size " << data_size;
  }
  raw_bundle_size_ = data_size;
}

StreamingMessageBundle::StreamingMessageBundle(
    std::list<StreamingMessagePtr> &message_list, uint64_t message_ts,
    uint64_t first_message_id, uint64_t last_message_id,
    StreamingMessageBundleType bundle_type, uint32_t raw_data_size)
    : StreamingMessageBundle(std::list<StreamingMessagePtr>(message_list), message_ts,
                             first_message_id, last_message_id, bundle_type,
                             raw_data_size) {}

StreamingMessageBundle::StreamingMessageBundle(StreamingMessageBundle &bundle) {
  message_bundle_ts_ = bundle.message_bundle_ts_;
  message_list_size_ = bundle.message_list_size_;
  raw_bundle_size_ = bundle.raw_bundle_size_;
  bundle_type_ = bundle.bundle_type_;
  first_message_id_ = bundle.first_message_id_;
  last_message_id_ = bundle.last_message_id_;
  message_list_ = bundle.message_list_;
  data_buffers_ = bundle.data_buffers_;
}

StreamingMessageBundlePtr StreamingMessageBundle::FromBytes(const uint8_t *meta,
                                                            const uint8_t *data) {
  uint32_t byte_offset = 0;
  StreamingMessageBundleMetaPtr meta_ptr = StreamingMessageBundleMeta::FromBytes(meta);
  byte_offset += meta_ptr->ClassBytesSize();

  uint32_t raw_data_size = *reinterpret_cast<const uint32_t *>(meta + byte_offset);
  std::list<StreamingMessagePtr> message_list;
  // only message bundle own raw data
  if (meta_ptr->GetBundleType() != StreamingMessageBundleType::Empty && data != nullptr) {
    GetMessageListFromRawData(data, raw_data_size, meta_ptr->GetMessageListSize(),
                              message_list);
  }
  auto result = std::make_shared<StreamingMessageBundle>(
      message_list, meta_ptr->GetMessageBundleTs(), meta_ptr->GetFirstMessageId(),
      meta_ptr->GetLastMessageId(), meta_ptr->GetBundleType());
  return result;
}

void StreamingMessageBundle::ToBytes(uint8_t *data) {
  uint32_t byte_offset = 0;
  StreamingMessageBundleMeta::ToBytes(data + byte_offset);
  byte_offset += StreamingMessageBundleMeta::ClassBytesSize();

  std::memcpy(data + byte_offset, reinterpret_cast<char *>(&raw_bundle_size_),
              sizeof(uint32_t));
  byte_offset += sizeof(uint32_t);

  if (raw_bundle_size_ > 0) {
    ConvertMessageListToRawData(message_list_, raw_bundle_size_, data + byte_offset);
  }
}

void StreamingMessageBundle::GetMessageListFromRawData(
    const uint8_t *bytes, uint32_t size, uint32_t message_list_size,
    std::list<StreamingMessagePtr> &message_list) {
  uint32_t byte_offset = 0;
  // only message bundle own raw data
  for (size_t i = 0; i < message_list_size; ++i) {
    StreamingMessagePtr item = StreamingMessage::FromBytes(bytes + byte_offset, true);
    message_list.push_back(item);
    byte_offset += item->Size();
  }
  STREAMING_CHECK(byte_offset == size)
      << "byte_offset :" << byte_offset << " size: " << size;
}

void StreamingMessageBundle::GetMessageList(
    std::list<StreamingMessagePtr> &message_list) {
  message_list = message_list_;
}

void StreamingMessageBundle::ConvertMessageListToRawData(
    const std::list<StreamingMessagePtr> &message_list, uint32_t raw_data_size,
    uint8_t *raw_data) {
  uint32_t byte_offset = 0;
  for (auto &message : message_list) {
    std::memcpy(raw_data + byte_offset, message->Data(), message->Size());
    byte_offset += message->Size();
  }
  STREAMING_CHECK(byte_offset == raw_data_size);
}

bool StreamingMessageBundle::operator==(StreamingMessageBundle &bundle) const {
  if (!(StreamingMessageBundleMeta::operator==(&bundle) &&
        this->GetRawBundleSize() == bundle.GetRawBundleSize() &&
        this->GetMessageListSize() == bundle.GetMessageListSize())) {
    return false;
  }
  auto it1 = message_list_.begin();
  auto it2 = bundle.message_list_.begin();
  while (it1 != message_list_.end() && it2 != bundle.message_list_.end()) {
    if (!((*it1).get()->operator==(*(*it2).get()))) {
      return false;
    }
    it1++;
    it2++;
  }
  return true;
}

bool StreamingMessageBundle::operator==(StreamingMessageBundle *bundle) const {
  return this->operator==(*bundle);
}

}  // namespace streaming
}  // namespace ray
