#ifndef RAY_STREAMING_MESSAGE_H
#define RAY_STREAMING_MESSAGE_H

#include <cstring>
#include <memory>
#include <ostream>

#include "common/buffer.h"
#include "message.h"

namespace ray {
namespace streaming {

class StreamingMessage;

typedef std::shared_ptr<StreamingMessage> StreamingMessagePtr;

enum class StreamingMessageType : uint32_t {
  Barrier = 1,
  Message = 2,
  MIN = Barrier,
  MAX = Message
};

enum class StreamingBarrierType : uint32_t {
  GlobalBarrier = 0,
  PartialBarrier = 1,
  EndOfDataBarrier = 2,
  UDCMsgBarrier = 3
};

struct StreamingBarrierHeader {
  StreamingBarrierType barrier_type;
  uint64_t barrier_id;
  // It's -1 if it's global barrier;
  uint64_t partial_barrier_id;
  inline bool IsGlobalBarrier() {
    return StreamingBarrierType::GlobalBarrier == barrier_type;
  }
  inline bool IsPartialBarrier() {
    return StreamingBarrierType::PartialBarrier == barrier_type;
  }
  inline bool IsEndOfDataBarrier() {
    return StreamingBarrierType::EndOfDataBarrier == barrier_type;
  }
  inline bool IsUDCMsgBarrier() {
    return StreamingBarrierType::UDCMsgBarrier == barrier_type;
  }
};

// Note: message/bundle parse have an implementation in java, changes in message/bundle
// should be in sync with java consumer.

constexpr uint32_t kMessageHeaderSize =
    sizeof(uint32_t) + sizeof(uint64_t) + sizeof(StreamingMessageType);

// constexpr uint32_t kBarrierHeaderSize = sizeof(StreamingBarrierHeader);
constexpr uint32_t kBarrierHeaderSize =
    sizeof(StreamingBarrierType) + sizeof(uint64_t) * 2;

/*
        +----------------+
        | Size=U32       |
        +----------------+
        | MessageId=U64  |
        +----------------+
        | MessageType=U32|
        +----------------+
        | Payload=bytes  |
        +----------------+
  Data contains barrier header and carried buffer if message type is
  global/partial barrier.
*/

class StreamingMessage {
 private:
  MemoryBuffer buffer_;
  bool has_data_copy_;

 public:
  StreamingMessage(MemoryBuffer &buffer, bool has_data_copy_ = false);
  StreamingMessage(uint8_t *buffer, uint32_t size, bool has_data_copy_ = false);

  /*!
   * @param payload message payload
   * @param data_size message payload size
   * @param message_id message id
   * @param message_type message_type
   */
  StreamingMessage(const uint8_t *payload, uint32_t payload_size, uint64_t message_id,
                   StreamingMessageType message_type);

  StreamingMessage(const StreamingMessage &);

  StreamingMessage operator=(const StreamingMessage &) = delete;

  static StreamingMessagePtr FromBytes(const uint8_t *data, bool copy_data = false);

  static StreamingMessagePtr FromBytes(uint8_t *data, uint32_t data_size, uint64_t seq_id,
                                       StreamingMessageType message_type);

  ~StreamingMessage();

  inline MemoryBuffer &GetBuffer() { return buffer_; }

  inline uint8_t *Data() const { return buffer_.Data(); }

  inline uint32_t Size() const { return buffer_.Size(); }

  inline uint8_t *Payload() const { return Data() + kMessageHeaderSize; }

  inline uint32_t PayloadSize() const { return Size() - kMessageHeaderSize; }

  inline StreamingMessageType GetMessageType() const {
    return *reinterpret_cast<const StreamingMessageType *>(Data() + sizeof(uint32_t) +
                                                           sizeof(uint64_t));
  }
  inline uint64_t GetMessageId() const {
    return *reinterpret_cast<const uint64_t *>(Data() + sizeof(uint32_t));
  }

  inline bool IsMessage() { return StreamingMessageType::Message == GetMessageType(); }

  inline bool IsBarrier() { return StreamingMessageType::Barrier == GetMessageType(); }

  bool operator==(const StreamingMessage &) const;

  static inline std::shared_ptr<uint8_t> MakeBarrierMessage(
      StreamingBarrierHeader &barrier_header, const uint8_t *data, uint32_t data_size) {
    std::shared_ptr<uint8_t> ptr(new uint8_t[data_size + kBarrierHeaderSize],
                                 std::default_delete<uint8_t[]>());
    std::memcpy(ptr.get(), &barrier_header.barrier_type, sizeof(StreamingBarrierType));
    std::memcpy(ptr.get() + sizeof(StreamingBarrierType), &barrier_header.barrier_id,
                sizeof(uint64_t));
    if (barrier_header.IsGlobalBarrier()) {
      barrier_header.partial_barrier_id = -1;
    }
    std::memcpy(ptr.get() + sizeof(StreamingBarrierType) + sizeof(uint64_t),
                &barrier_header.partial_barrier_id, sizeof(uint64_t));
    if (data && data_size > 0) {
      std::memcpy(ptr.get() + kBarrierHeaderSize, data, data_size);
    }
    return ptr;
  }

  static inline void GetBarrierIdFromRawData(const uint8_t *data,
                                             StreamingBarrierHeader *barrier_header) {
    barrier_header->barrier_type = *reinterpret_cast<const StreamingBarrierType *>(data);
    barrier_header->barrier_id =
        *reinterpret_cast<const uint64_t *>(data + sizeof(StreamingBarrierType));
    barrier_header->partial_barrier_id = *reinterpret_cast<const uint64_t *>(
        data + sizeof(StreamingBarrierType) + sizeof(uint64_t));
  }

  friend std::ostream &operator<<(std::ostream &os, const StreamingMessage &message);
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MESSAGE_H
