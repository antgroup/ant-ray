#ifndef _STREAMING_QUEUE_ITEM_H_
#define _STREAMING_QUEUE_ITEM_H_
#include <iterator>
#include <list>
#include <thread>
#include <vector>

#include "common/buffer.h"
#include "logging.h"
#include "message.h"
#include "ray/common/id.h"

namespace ray {
namespace streaming {

using ray::ObjectID;
const uint64_t QUEUE_INVALID_SEQ_ID = std::numeric_limits<uint64_t>::max();
constexpr uint32_t kQueueItemHeaderSize = sizeof(uint64_t) * 6 + sizeof(bool);

class QueueItem {
 public:
  QueueItem() = default;

  QueueItem(uint64_t seq_id, uint8_t *meta_data, uint32_t meta_data_size, uint8_t *data,
            uint32_t data_size, uint64_t timestamp_message, uint64_t timestamp_item,
            uint64_t msg_id_start, uint64_t msg_id_end, bool copy = false)
      : seq_id_(seq_id),
        msg_id_start_(msg_id_start),
        msg_id_end_(msg_id_end),
        timestamp_message_(timestamp_message),
        timestamp_item_(timestamp_item),
        timestamp_received_(0),
        copy_(copy) {
    if (data_size > 0) {
      buffer_ = std::make_shared<LocalMemoryBuffer>(data, data_size, copy_, copy_);
    }
    if (meta_data_size > 0) {
      meta_buffer_ =
          std::make_shared<LocalMemoryBuffer>(meta_data, meta_data_size, true, true);
    }
  }

  QueueItem(uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> meta_buffer,
            std::shared_ptr<LocalMemoryBuffer> buffer, uint64_t timestamp_message,
            uint64_t timestamp_item, uint64_t timestamp_received, uint64_t msg_id_start,
            uint64_t msg_id_end, bool copy = false)
      : seq_id_(seq_id),
        msg_id_start_(msg_id_start),
        msg_id_end_(msg_id_end),
        timestamp_message_(timestamp_message),
        timestamp_item_(timestamp_item),
        timestamp_received_(timestamp_received),
        copy_(copy),
        buffer_(buffer),
        meta_buffer_(meta_buffer) {}

  QueueItem(std::shared_ptr<DataMessage> data_msg, uint64_t timestamp_received)
      : seq_id_(data_msg->SeqId()),
        msg_id_start_(data_msg->MsgIdStart()),
        msg_id_end_(data_msg->MsgIdEnd()),
        timestamp_message_(data_msg->TimestampMessage()),
        timestamp_item_(data_msg->TimestampItem()),
        timestamp_received_(timestamp_received),
        copy_(false),
        buffer_(data_msg->Buffer()),
        meta_buffer_(data_msg->MetaBuffer()) {}

  QueueItem(std::shared_ptr<LocalDataMessage> local_msg,
            std::shared_ptr<LocalMemoryBuffer> data_buffer, uint64_t timestamp_received)
      : seq_id_(local_msg->SeqId()),
        msg_id_start_(local_msg->MsgIdStart()),
        msg_id_end_(local_msg->MsgIdEnd()),
        timestamp_message_(local_msg->TimestampMessage()),
        timestamp_item_(local_msg->TimestampItem()),
        timestamp_received_(timestamp_received),
        copy_(false),
        buffer_(data_buffer),
        meta_buffer_(local_msg->MetaBuffer()),
        object_id_(local_msg->GetCollocateObjectId()) {}

  QueueItem(const QueueItem &&item) {
    buffer_ = item.buffer_;
    meta_buffer_ = item.meta_buffer_;
    seq_id_ = item.seq_id_;
    msg_id_start_ = item.msg_id_start_;
    msg_id_end_ = item.msg_id_end_;
    timestamp_message_ = item.timestamp_message_;
    timestamp_item_ = item.timestamp_item_;
    timestamp_received_ = item.timestamp_received_;
    copy_ = item.copy_;
    object_id_ = item.object_id_;
  }

  QueueItem(const QueueItem &item) {
    buffer_ = item.buffer_;
    meta_buffer_ = item.meta_buffer_;
    seq_id_ = item.seq_id_;
    msg_id_start_ = item.msg_id_start_;
    msg_id_end_ = item.msg_id_end_;
    timestamp_message_ = item.timestamp_message_;
    timestamp_item_ = item.timestamp_item_;
    timestamp_received_ = item.timestamp_received_;
    copy_ = item.copy_;
    object_id_ = item.object_id_;
  }

  /// Only meta filed is provided, used by elastic buffer.
  QueueItem(uint64_t seq_id, uint64_t msg_id_start, uint64_t msg_id_end)
      : seq_id_(seq_id), msg_id_start_(msg_id_start), msg_id_end_(msg_id_end) {}

  QueueItem &operator=(const QueueItem &item) {
    buffer_ = item.buffer_;
    meta_buffer_ = item.meta_buffer_;
    seq_id_ = item.seq_id_;
    msg_id_start_ = item.msg_id_start_;
    msg_id_end_ = item.msg_id_end_;
    timestamp_message_ = item.timestamp_message_;
    timestamp_item_ = item.timestamp_item_;
    timestamp_received_ = item.timestamp_received_;
    copy_ = item.copy_;
    object_id_ = item.object_id_;
    return *this;
  }

  QueueItem(uint8_t *data, uint32_t data_size) {
    std::memcpy(GetFirstMemberAddress(), data, kQueueItemHeaderSize);
    uint32_t offset = kQueueItemHeaderSize;
    uint32_t tmpsize = *reinterpret_cast<uint32_t *>(data + offset);
    offset += sizeof(uint32_t);
    meta_buffer_ = tmpsize == 0 ? nullptr
                                : std::make_shared<LocalMemoryBuffer>(
                                      data + offset, tmpsize, true, true);
    offset += tmpsize;
    tmpsize = *reinterpret_cast<uint32_t *>(data + offset);
    offset += sizeof(uint32_t);
    buffer_ = std::make_shared<LocalMemoryBuffer>(data + offset, tmpsize, true, true);
    STREAMING_CHECK(offset + tmpsize == data_size)
        << "QueueItem use error data to construct item!";
  }

  virtual ~QueueItem() = default;

  virtual void ToBytes(uint8_t *bytes) {
    size_t offset = 0;
    std::memcpy(bytes + offset, GetFirstMemberAddress(), kQueueItemHeaderSize);
    offset += kQueueItemHeaderSize;
    if (meta_buffer_) {
      std::memcpy(bytes + offset, meta_buffer_->Data(), meta_buffer_->Size());
      offset += meta_buffer_->Size();
    }
    if (buffer_) {
      std::memcpy(bytes + offset, buffer_->Data(), buffer_->Size());
    }
  }

  virtual void ToSerialize(uint8_t *bytes) {
    uint32_t offset = 0;
    std::memcpy(bytes + offset, GetFirstMemberAddress(), kQueueItemHeaderSize);
    offset += kQueueItemHeaderSize;
    uint32_t tmpsize = meta_buffer_ ? meta_buffer_->Size() : 0;
    std::memcpy(bytes + offset, &tmpsize, sizeof(tmpsize));
    offset += sizeof(tmpsize);
    if (tmpsize) {
      std::memcpy(bytes + offset, meta_buffer_->Data(), tmpsize);
    }
    offset += tmpsize;
    tmpsize = buffer_ ? buffer_->Size() : 0;
    std::memcpy(bytes + offset, &tmpsize, sizeof(tmpsize));
    offset += sizeof(tmpsize);
    if (tmpsize) {
      std::memcpy(bytes + offset, buffer_->Data(), tmpsize);
    }
  }
  inline uint32_t ClassBytesSize() { return DataSize() + kQueueItemHeaderSize; }
  inline uint32_t SerializeSize() {
    return DataSize() + kQueueItemHeaderSize + sizeof(uint32_t) * 2;
  }
  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgIdStart() { return msg_id_start_; }
  inline uint64_t MsgIdEnd() { return msg_id_end_; }
  inline bool InItem(uint64_t msg_id) {
    return msg_id >= msg_id_start_ && msg_id <= msg_id_end_;
  }

  inline bool LessThanItem(uint64_t msg_id) { return msg_id_end_ < msg_id; }
  inline bool LargeThanItem(uint64_t msg_id) { return msg_id_start_ > msg_id; }
  inline bool IsCopy() { return copy_; }
  inline uint64_t TimeStamp() { return timestamp_item_; }
  inline size_t DataSize() const {
    size_t data_size = 0;
    if (meta_buffer_) {
      data_size += meta_buffer_->Size();
    }
    if (buffer_) {
      data_size += buffer_->Size();
    }
    return data_size;
  }
  inline size_t BufferSize() const { return buffer_ ? buffer_->Size() : 0; }
  inline size_t MetaBufferSize() const { return meta_buffer_ ? meta_buffer_->Size() : 0; }
  inline std::shared_ptr<LocalMemoryBuffer> Buffer() const { return buffer_; }
  inline std::shared_ptr<LocalMemoryBuffer> MetaBuffer() const { return meta_buffer_; }
  inline uint64_t TimestampMessage() { return timestamp_message_; }
  inline uint64_t TimestampItem() { return timestamp_item_; }
  inline uint64_t TimestampReceived() { return timestamp_received_; }

  inline ObjectID GetCollocateObjectId() { return object_id_; }
  /// If the item is not in bufferpool, then OwnsData return true.
  inline bool OwnsData() const {
    return !(MetaBuffer() && Buffer() && !Buffer()->OwnsData());
  }

 private:
  inline uint8_t *GetFirstMemberAddress() {
    return reinterpret_cast<uint8_t *>(&seq_id_);
  }

 protected:
  uint64_t seq_id_;
  uint64_t msg_id_start_;
  uint64_t msg_id_end_;
  uint64_t timestamp_message_;
  uint64_t timestamp_item_;
  uint64_t timestamp_received_;
  bool copy_;

  std::shared_ptr<LocalMemoryBuffer> buffer_;
  std::shared_ptr<LocalMemoryBuffer> meta_buffer_;

  ObjectID object_id_;

 public:
  static QueueItem &InvalidQueueItem() {
    static QueueItem item(QUEUE_INVALID_SEQ_ID, nullptr, 0, nullptr, 0, 0, 0,
                          QUEUE_INVALID_SEQ_ID, QUEUE_INVALID_SEQ_ID);
    return item;
  }

  static bool IsInvalidQueueItem(QueueItem &item) {
    return item.SeqId() == QUEUE_INVALID_SEQ_ID &&
           item.MsgIdStart() == QUEUE_INVALID_SEQ_ID &&
           item.MsgIdEnd() == QUEUE_INVALID_SEQ_ID;
  }
};

typedef std::shared_ptr<QueueItem> QueueItemPtr;

}  // namespace streaming
}  // namespace ray
#endif
