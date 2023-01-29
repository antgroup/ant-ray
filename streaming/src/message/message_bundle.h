#ifndef RAY_STREAMING_MESSAGE_BUNDLE_H
#define RAY_STREAMING_MESSAGE_BUNDLE_H

#include <ctime>
#include <list>
#include <numeric>
#include <ostream>

#include "buffer_pool/buffer_pool.h"
#include "common/buffer.h"
#include "message.h"

namespace ray {
namespace streaming {

enum class StreamingMessageBundleType : uint32_t {
  Empty = 1,
  Barrier = 2,
  Bundle = 3,
  MIN = Empty,
  MAX = Bundle
};

class StreamingMessageBundleMeta;
class StreamingMessageBundle;

typedef std::shared_ptr<StreamingMessageBundle> StreamingMessageBundlePtr;
typedef std::shared_ptr<StreamingMessageBundleMeta> StreamingMessageBundleMetaPtr;

/*
        +--------------------+
        | MagicNum=U32       |
        +--------------------+
        | BundleTs=U64       |
        +--------------------+
        | FirstMessageId=U64 |
        +--------------------+
        | LastMessageId=U64  |
        +--------------------+
        | MessageListSize=U32|
        +--------------------+
        | BundleType=U32     |
        +--------------------+
        | RawBundleSize=U32  |
        +--------------------+
        | RawData=var(N*Msg) |
        +--------------------+
*/

constexpr uint32_t kMessageBundleMetaHeaderSize =
    sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) +
    sizeof(uint64_t) + sizeof(StreamingMessageBundleType);

constexpr uint32_t kMessageBundleHeaderSize =
    kMessageBundleMetaHeaderSize + sizeof(uint32_t);

class StreamingMessageBundleMeta {
 public:
  static const uint32_t StreamingMessageBundleMagicNum = 0xCAFEBABA;

 protected:
  uint64_t message_bundle_ts_;

  uint64_t first_message_id_;
  uint64_t last_message_id_;

  uint32_t message_list_size_;

  StreamingMessageBundleType bundle_type_;

 public:
  explicit StreamingMessageBundleMeta(uint64_t message_bundle_ts,
                                      uint64_t first_message_id, uint64_t last_message_id,
                                      uint32_t message_list_size,
                                      StreamingMessageBundleType bundle_type);

  explicit StreamingMessageBundleMeta(const uint8_t *bytes);

  explicit StreamingMessageBundleMeta(StreamingMessageBundleMeta *);

  explicit StreamingMessageBundleMeta();

  virtual ~StreamingMessageBundleMeta(){};

  virtual uint32_t GetRawBundleSize() const { return 0; }

  bool operator==(StreamingMessageBundleMeta &) const;

  bool operator==(StreamingMessageBundleMeta *) const;

  inline uint64_t GetMessageBundleTs() const { return message_bundle_ts_; }

  inline uint64_t GetLastMessageId() const { return last_message_id_; }

  inline uint64_t GetFirstMessageId() const { return first_message_id_; }

  inline uint32_t GetMessageListSize() const { return message_list_size_; }

  inline StreamingMessageBundleType GetBundleType() const { return bundle_type_; }

  inline static bool CheckBundleMagicNum(const uint8_t *bytes) {
    const auto *magic_num = reinterpret_cast<const uint32_t *>(bytes);
    return *magic_num == StreamingMessageBundleMagicNum;
  }

  inline bool IsBarrier() { return StreamingMessageBundleType::Barrier == bundle_type_; }
  inline bool IsBundle() { return StreamingMessageBundleType::Bundle == bundle_type_; }
  inline bool IsEmptyMsg() { return StreamingMessageBundleType::Empty == bundle_type_; }

  virtual void ToBytes(uint8_t *data);
  static StreamingMessageBundleMetaPtr FromBytes(const uint8_t *data,
                                                 bool verifer_check = true);
  inline virtual uint32_t ClassBytesSize() { return kMessageBundleMetaHeaderSize; }

  inline std::shared_ptr<LocalMemoryBuffer> MetaBuffer() {
    if (nullptr == meta_buffer_) {
      uint32_t byte_offset = 0;
      uint8_t bytes[kMessageBundleHeaderSize];
      uint32_t magicNum = StreamingMessageBundleMeta::StreamingMessageBundleMagicNum;
      std::memcpy(bytes + byte_offset, reinterpret_cast<const uint8_t *>(&magicNum),
                  sizeof(uint32_t));
      byte_offset += sizeof(uint32_t);
      std::memcpy(bytes + byte_offset, GetFirstMemberAddress(),
                  kMessageBundleMetaHeaderSize - sizeof(uint32_t));
      byte_offset += kMessageBundleMetaHeaderSize - sizeof(uint32_t);
      uint32_t raw_bundle_size = GetRawBundleSize();
      std::memcpy(bytes + byte_offset, reinterpret_cast<char *>(&raw_bundle_size),
                  sizeof(uint32_t));
      meta_buffer_ = std::make_shared<LocalMemoryBuffer>(bytes, kMessageBundleHeaderSize,
                                                         true, true);
    }
    return meta_buffer_;
  }

  std::string ToString() {
    return std::to_string(first_message_id_) + "," + std::to_string(last_message_id_) +
           "," + std::to_string(message_list_size_) + "," +
           std::to_string(message_bundle_ts_) + "," +
           std::to_string(static_cast<uint32_t>(bundle_type_));
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const StreamingMessageBundleMeta &meta);

 private:
  /// To speed up memory copy and serialization, we use memory layout of compiler related
  /// member variables. It's must be modified if any field is going to be inserted before
  /// first member property.
  /// Reference
  /// :/http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p1113r0.html#2254).
  inline uint8_t *GetFirstMemberAddress() {
    return reinterpret_cast<uint8_t *>(&message_bundle_ts_);
  }
  std::shared_ptr<LocalMemoryBuffer> meta_buffer_ = nullptr;
};
class StreamingMessageBundle final : public StreamingMessageBundleMeta {
 private:
  CompositeBuffer data_buffers_;
  uint32_t raw_bundle_size_;

  // Lazy serialization/deserialization.
  std::list<StreamingMessagePtr> message_list_;

 public:
  explicit StreamingMessageBundle(std::list<StreamingMessagePtr> &&, uint64_t, uint64_t,
                                  uint64_t, StreamingMessageBundleType,
                                  uint32_t raw_data_size = 0);

  // Duplicated copy if no right reference in constructor.
  explicit StreamingMessageBundle(std::list<StreamingMessagePtr> &, uint64_t, uint64_t,
                                  uint64_t, StreamingMessageBundleType,
                                  uint32_t raw_data_size = 0);

  explicit StreamingMessageBundle(uint64_t first_message_id, uint64_t last_message_id,
                                  uint64_t message_bundle_ts);

  StreamingMessageBundle(StreamingMessageBundle &);

  ~StreamingMessageBundle() override = default;

  inline uint32_t GetRawBundleSize() const override { return raw_bundle_size_; }

  bool operator==(StreamingMessageBundle &) const;

  bool operator==(StreamingMessageBundle *) const;

  void GetMessageList(std::list<StreamingMessagePtr> &);

  const std::list<StreamingMessagePtr> &GetMessageList() const { return message_list_; }

  CompositeBuffer &DataBuffers() { return data_buffers_; }
  virtual void ToBytes(uint8_t *data) override;
  inline uint32_t PayLoadSize() { return raw_bundle_size_; }

  static StreamingMessageBundlePtr FromBytes(const uint8_t *meta, const uint8_t *data);

  inline virtual uint32_t ClassBytesSize() override {
    return kMessageBundleHeaderSize + raw_bundle_size_;
  };

  static void GetMessageListFromRawData(const uint8_t *, uint32_t, uint32_t,
                                        std::list<StreamingMessagePtr> &);
  static void ConvertMessageListToRawData(const std::list<StreamingMessagePtr> &,
                                          uint32_t, uint8_t *);
};

class StreamingReaderBundle {
 private:
  // it's immutable data point
  std::shared_ptr<LocalMemoryBuffer> data_buf;
  std::shared_ptr<LocalMemoryBuffer> meta_buf;

 public:
  // Source channel id.
  ObjectID from;
  // Last message id for this bundle.
  uint64_t message_id;
  // Seq id for recording message bundle.
  uint64_t bundle_id;
  // Current barrier id in this channel.
  uint64_t last_barrier_id;
  // Current UDC msg barrier id in this channel.
  uint64_t last_udc_barrier_id;
  StreamingMessageBundleMetaPtr meta;
  uint64_t timestamp_push;
  // Whether the channel which the bundle read from is cyclic or not.
  bool cyclic;

  StreamingReaderBundle()
      : data_buf(nullptr),
        meta_buf(nullptr),
        message_id(0),
        bundle_id(0),
        last_barrier_id(0),
        last_udc_barrier_id(0),
        timestamp_push(0) {}

  ~StreamingReaderBundle() {}

  void Realloc(uint32_t size) {
    // data_buf = new uint8_t[size];
  }

  inline bool IsEmpty() { return data_buf == nullptr && meta_buf == nullptr; }

  inline uint64_t DataSize() const {
    if (data_buf != nullptr) {
      return data_buf->Size();
    }
    return 0;
  }
  inline uint8_t *DataBuffer() const {
    if (data_buf != nullptr) {
      return data_buf->Data();
    }
    return nullptr;
  }

  inline uint8_t *MetaBuffer() const {
    if (meta_buf != nullptr) {
      return meta_buf->Data();
    }
    return nullptr;
  }

  void FillBundle(std::shared_ptr<LocalMemoryBuffer> meta_b,
                  std::shared_ptr<LocalMemoryBuffer> data_b) {
    meta_buf = meta_b;
    data_buf = data_b;
    meta = StreamingMessageBundleMeta::FromBytes(MetaBuffer());
    message_id = meta->GetLastMessageId();
  }

  friend std::ostream &operator<<(std::ostream &os, const StreamingReaderBundle &bundle) {
    os << "{"
       << "message_id : " << bundle.message_id << ", meta: " << *(bundle.meta)
       << ", data_size: " << bundle.DataSize() << ", from: " << bundle.from
       << ", last_barrier_id: " << bundle.last_barrier_id
       << ", last_udc_barrier_id: " << bundle.last_udc_barrier_id << ", timestamp_push"
       << bundle.timestamp_push << "}";
    return os;
  }

  inline void ResetBuffer() {
    meta.reset();
    meta_buf.reset();
    data_buf.reset();
  }
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MESSAGE_BUNDLE_H
