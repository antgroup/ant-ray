#include "message.h"

#include <cstring>
#include <string>
#include <utility>

#include "logging.h"

namespace ray {
namespace streaming {

StreamingMessage::StreamingMessage(MemoryBuffer &buffer, bool has_data_copy)
    : buffer_(buffer), has_data_copy_(has_data_copy) {}

StreamingMessage::StreamingMessage(uint8_t *buffer, uint32_t size, bool has_data_copy)
    : buffer_(buffer, size), has_data_copy_(has_data_copy) {}

StreamingMessage::StreamingMessage(const uint8_t *payload, uint32_t payload_size,
                                   uint64_t message_id,
                                   StreamingMessageType message_type) {
  uint32_t data_size = kMessageHeaderSize + payload_size;
  auto *data = new uint8_t[data_size];

  uint32_t byte_offset = 0;
  std::memcpy(data + byte_offset, reinterpret_cast<char *>(&data_size),
              sizeof(data_size));
  byte_offset += sizeof(data_size);

  std::memcpy(data + byte_offset, reinterpret_cast<char *>(&message_id),
              sizeof(message_id));
  byte_offset += sizeof(message_id);

  std::memcpy(data + byte_offset, reinterpret_cast<char *>(&message_type),
              sizeof(message_type));
  byte_offset += sizeof(message_type);

  std::memcpy(data + byte_offset, payload, payload_size);
  byte_offset += payload_size;
  STREAMING_CHECK(byte_offset == data_size);
  buffer_ = MemoryBuffer(data, data_size);
  has_data_copy_ = true;
}

StreamingMessage::StreamingMessage(const StreamingMessage &msg) {
  buffer_ = msg.buffer_;
  has_data_copy_ = msg.has_data_copy_;
}

StreamingMessage::~StreamingMessage() {
  if (has_data_copy_) {
    delete[] buffer_.Data();
  }
}

StreamingMessagePtr StreamingMessage::FromBytes(const uint8_t *bytes, bool copy_data) {
  uint32_t data_size = *reinterpret_cast<const uint32_t *>(bytes);
  auto *buffer = const_cast<uint8_t *>(bytes);
  if (copy_data) {
    buffer = new uint8_t[data_size];
    std::memcpy(buffer, bytes, data_size);
  }
  return std::make_shared<StreamingMessage>(buffer, data_size, copy_data);
}

StreamingMessagePtr StreamingMessage::FromBytes(uint8_t *data, uint32_t data_size,
                                                uint64_t message_id,
                                                StreamingMessageType message_type) {
  uint32_t byte_offset = 0;
  std::memcpy(data, reinterpret_cast<char *>(&data_size), sizeof(data_size));
  byte_offset += sizeof(data_size);

  std::memcpy(data + byte_offset, reinterpret_cast<char *>(&message_id),
              sizeof(message_id));
  byte_offset += sizeof(message_id);

  std::memcpy(data + byte_offset, reinterpret_cast<char *>(&message_type),
              sizeof(message_type));
  return std::make_shared<StreamingMessage>(data, data_size, false);
}

bool StreamingMessage::operator==(const StreamingMessage &message) const {
  return buffer_.Size() == message.buffer_.Size() &&
         !std::memcmp(buffer_.Data(), message.buffer_.Data(), buffer_.Size());
}

std::ostream &operator<<(std::ostream &os, const StreamingMessage &message) {
  os << "{"
     << " message_type_: " << static_cast<int>(message.GetMessageType())
     << " message_id_: " << message.GetMessageId()
     << " payload_size_: " << message.PayloadSize() << "}";
  return os;
}

}  // namespace streaming
}  // namespace ray
