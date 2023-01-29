#ifndef _STREAMING_QUEUE_MESSAGE_H_
#define _STREAMING_QUEUE_MESSAGE_H_

#include <condition_variable>
#include <mutex>
#include <queue>

#include "common/buffer.h"
#include "logging.h"
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "streaming_queue_generated.h"

namespace ray {
namespace streaming {

/// base class of all command messages
class Message {
 public:
  Message(const ActorID &actor_id, const ActorID &peer_actor_id, const ObjectID &queue_id,
          std::shared_ptr<LocalMemoryBuffer> buffer = nullptr)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        buffer_(buffer) {}
  Message(const queue::flatbuf::CommonMsg *message)
      : actor_id_(ActorID::FromBinary(message->src_actor_id()->str())),
        peer_actor_id_(ActorID::FromBinary(message->dst_actor_id()->str())),
        queue_id_(ObjectID::FromBinary(message->queue_id()->str())) {}
  Message() {}
  virtual ~Message() = default;
  virtual inline ActorID ActorId() { return actor_id_; }
  virtual inline ActorID PeerActorId() { return peer_actor_id_; }
  virtual inline ObjectID QueueId() { return queue_id_; }
  virtual queue::flatbuf::MessageType Type() = 0;
  inline std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

  virtual std::unique_ptr<LocalMemoryBuffer> ToBytes();
  virtual std::unique_ptr<LocalMemoryBuffer> GetMetaBytes();
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) = 0;

 protected:
  flatbuffers::Offset<queue::flatbuf::CommonMsg> ConstructCommonFlatBuf(
      flatbuffers::FlatBufferBuilder &builder);

 protected:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  std::shared_ptr<LocalMemoryBuffer> buffer_;

 public:
  static const uint32_t MagicNum;
};

constexpr uint32_t kItemMetaHeaderSize =
    sizeof(Message::MagicNum) + sizeof(queue::flatbuf::MessageType) + sizeof(uint64_t);
constexpr uint32_t kItemHeaderSize = kItemMetaHeaderSize + sizeof(uint64_t);

class QueueItem;
class DataMessage : public Message {
 public:
  DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
              const ObjectID &queue_id, QueueItem &item, bool resend, uint64_t resend_id,
              queue::flatbuf::ResendReason reason);
  DataMessage(const queue::flatbuf::StreamingQueueDataMsg *message,
              std::shared_ptr<LocalMemoryBuffer> meta_buffer,
              std::shared_ptr<LocalMemoryBuffer> buffer, uint64_t timestamp_message)
      : Message(ActorID::FromBinary(message->common()->src_actor_id()->str()),
                ActorID::FromBinary(message->common()->dst_actor_id()->str()),
                ObjectID::FromBinary(message->common()->queue_id()->str()), buffer),
        seq_id_(message->seq_id()),
        msg_id_start_(message->msg_id_start()),
        msg_id_end_(message->msg_id_end()),
        timestamp_message_(timestamp_message),
        timestamp_item_(message->timestamp()),
        meta_buffer_(meta_buffer),
        resend_(message->resend()),
        resend_id_(message->resend_id()),
        resend_reason_(message->reason()) {}
  DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
              const ObjectID &queue_id, std::shared_ptr<LocalMemoryBuffer> buffer,
              bool resend, uint64_t resend_id, queue::flatbuf::ResendReason reason)
      : Message(actor_id, peer_actor_id, queue_id, buffer),
        resend_(resend),
        resend_id_(resend_id),
        resend_reason_(reason) {}
  virtual ~DataMessage() {}

  static std::shared_ptr<DataMessage> FromBytes(uint8_t *bytes);
  static std::shared_ptr<DataMessage> FromBytes(
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;

  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgIdStart() { return msg_id_start_; }
  inline uint64_t MsgIdEnd() { return msg_id_end_; }
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueDataMsg;
  }
  inline uint64_t TimestampMessage() { return timestamp_message_; }
  inline uint64_t TimestampItem() { return timestamp_item_; }
  inline std::shared_ptr<LocalMemoryBuffer> MetaBuffer() { return meta_buffer_; }
  inline bool IsResend() { return resend_; }
  inline uint64_t ResendId() { return resend_id_; }
  queue::flatbuf::ResendReason ResendReason() { return resend_reason_; }

 protected:
  uint64_t seq_id_;
  uint64_t msg_id_start_;
  uint64_t msg_id_end_;
  uint64_t timestamp_message_;
  uint64_t timestamp_item_;
  std::shared_ptr<LocalMemoryBuffer> meta_buffer_;
  bool resend_;
  uint64_t resend_id_;
  queue::flatbuf::ResendReason resend_reason_;
};

class LocalDataMessage : public DataMessage {
 public:
  LocalDataMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                   const ObjectID &queue_id, const ObjectID &object_id, QueueItem &item,
                   uint64_t data_offset, uint64_t data_size, bool resend,
                   uint64_t resend_id, queue::flatbuf::ResendReason reason)
      : DataMessage(actor_id, peer_actor_id, queue_id, item, resend, resend_id, reason),
        data_offset_(data_offset),
        data_size_(data_size),
        object_id_(object_id) {}
  LocalDataMessage(const queue::flatbuf::StreamingQueueLocalDataMsg *message,
                   std::shared_ptr<LocalMemoryBuffer> meta_buffer,
                   uint64_t timestamp_message)
      : DataMessage(ActorID::FromBinary(message->common()->src_actor_id()->str()),
                    ActorID::FromBinary(message->common()->dst_actor_id()->str()),
                    ObjectID::FromBinary(message->common()->queue_id()->str()), nullptr,
                    message->resend(), message->resend_id(), message->reason()) {
    seq_id_ = message->seq_id();
    msg_id_start_ = message->msg_id_start();
    msg_id_end_ = message->msg_id_end();
    timestamp_message_ = timestamp_message;
    timestamp_item_ = message->timestamp();
    meta_buffer_ = meta_buffer;
    data_offset_ = message->data_offset();
    data_size_ = message->data_size();
    object_id_ = ObjectID::FromBinary(message->object_id()->str());
  }

  virtual ~LocalDataMessage() = default;
  static std::shared_ptr<LocalDataMessage> FromBytes(
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueLocalDataMsg;
  }
  inline uint64_t GetDataOffset() { return data_offset_; }
  inline uint64_t GetDataSize() { return data_size_; }
  inline ObjectID GetCollocateObjectId() { return object_id_; }

 private:
  uint64_t data_offset_;
  uint64_t data_size_;
  ObjectID object_id_;
};

class NotificationMessage : public Message {
 public:
  NotificationMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t msg_id, uint64_t bundle_id)
      : Message(actor_id, peer_actor_id, queue_id),
        msg_id_(msg_id),
        bundle_id_(bundle_id) {}

  virtual ~NotificationMessage() = default;

  static std::shared_ptr<NotificationMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;

  inline uint64_t MsgId() { return msg_id_; }
  inline uint64_t BundleId() { return bundle_id_; }
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueNotificationMsg;
  }

 private:
  uint64_t msg_id_;
  uint64_t bundle_id_;
};

class PullRequestMessage : public Message {
 public:
  PullRequestMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                     const ObjectID &queue_id, uint64_t checkpoint_id, uint64_t msg_id)
      : Message(actor_id, peer_actor_id, queue_id),
        checkpoint_id_(checkpoint_id),
        msg_id_(msg_id) {}
  virtual ~PullRequestMessage() = default;

  static std::shared_ptr<PullRequestMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;

  inline uint64_t CheckpointId() { return checkpoint_id_; }
  inline uint64_t MsgId() { return msg_id_; }
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueuePullRequestMsg;
  }

 private:
  uint64_t checkpoint_id_;
  uint64_t msg_id_;
};

class PullResponseMessage : public Message {
 public:
  PullResponseMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t seq_id, uint64_t msg_id,
                      uint64_t count, queue::flatbuf::StreamingQueueError err_code,
                      bool is_upstream_first_pull)
      : Message(actor_id, peer_actor_id, queue_id),
        seq_id_(seq_id),
        msg_id_(msg_id),
        count_(count),
        is_upstream_first_pull_(is_upstream_first_pull),
        err_code_(err_code) {}
  virtual ~PullResponseMessage() = default;

  static std::shared_ptr<PullResponseMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;

  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgId() { return msg_id_; }
  inline uint64_t Count() { return count_; }
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueuePullResponseMsg;
  }
  inline queue::flatbuf::StreamingQueueError ErrorCode() { return err_code_; }
  inline bool IsUpstreamFirstPull() { return is_upstream_first_pull_; }

 private:
  uint64_t seq_id_;
  uint64_t msg_id_;
  uint64_t count_;
  bool is_upstream_first_pull_;
  queue::flatbuf::StreamingQueueError err_code_;
};

class GetLastMsgIdMessage : public Message {
 public:
  GetLastMsgIdMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~GetLastMsgIdMessage() = default;
  static std::shared_ptr<GetLastMsgIdMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueGetLastMsgId;
  }
};

class GetLastMsgIdRspMessage : public Message {
 public:
  GetLastMsgIdRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                         const ObjectID &queue_id, uint64_t seq_id, uint64_t msg_id,
                         queue::flatbuf::StreamingQueueError error)
      : Message(actor_id, peer_actor_id, queue_id),
        seq_id_(seq_id),
        msg_id_(msg_id),
        err_code_(error) {}
  virtual ~GetLastMsgIdRspMessage() = default;
  static std::shared_ptr<GetLastMsgIdRspMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp;
  }
  inline queue::flatbuf::StreamingQueueError ErrorCode() { return err_code_; }
  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgId() { return msg_id_; }

 private:
  uint64_t seq_id_;
  uint64_t msg_id_;
  queue::flatbuf::StreamingQueueError err_code_;
};

class GetHostnameMessage : public Message {
 public:
  GetHostnameMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                     const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~GetHostnameMessage() = default;
  static std::shared_ptr<GetHostnameMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  inline queue::flatbuf::MessageType Type() {
    return queue::flatbuf::MessageType::StreamingQueueGetHostnameMsg;
  }
};

class GetHostnameRspMessage : public Message {
 public:
  GetHostnameRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                        const ObjectID &queue_id, std::string hostname,
                        queue::flatbuf::StreamingQueueError error)
      : Message(actor_id, peer_actor_id, queue_id),
        hostname_(hostname),
        err_code_(error) {}
  virtual ~GetHostnameRspMessage() = default;
  static std::shared_ptr<GetHostnameRspMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  inline queue::flatbuf::MessageType Type() {
    return queue::flatbuf::MessageType::StreamingQueueGetHostnameRspMsg;
  }
  inline queue::flatbuf::StreamingQueueError ErrorCode() { return err_code_; }
  inline std::string Hostname() { return hostname_; }

 private:
  std::string hostname_;
  queue::flatbuf::StreamingQueueError err_code_;
};

class StartLoggingMessage : public Message {
 public:
  StartLoggingMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t msg_id, uint64_t seq_id,
                      uint64_t barrier_id)
      : Message(actor_id, peer_actor_id, queue_id),
        msg_id_(msg_id),
        seq_id_(seq_id),
        barrier_id_(barrier_id) {}
  StartLoggingMessage(const queue::flatbuf::StreamingQueueStartLoggingMsg *message)
      : Message(message->common()),
        msg_id_(message->msg_id()),
        seq_id_(message->seq_id()),
        barrier_id_(message->barrier_id()) {}
  virtual ~StartLoggingMessage() = default;
  static std::shared_ptr<StartLoggingMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  inline queue::flatbuf::MessageType Type() {
    return queue::flatbuf::MessageType::StreamingQueueStartLoggingMsg;
  }
  inline uint64_t MsgId() { return msg_id_; }
  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t BarrierId() { return barrier_id_; }

 private:
  uint64_t msg_id_;
  uint64_t seq_id_;
  uint64_t barrier_id_;
};

class StartLoggingRspMessage : public Message {
 public:
  StartLoggingRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                         const ObjectID &queue_id,
                         queue::flatbuf::StreamingQueueError error)
      : Message(actor_id, peer_actor_id, queue_id), err_code_(error) {}
  StartLoggingRspMessage(const queue::flatbuf::StreamingQueueStartLoggingRspMsg *message)
      : Message(message->common()) {}
  virtual ~StartLoggingRspMessage() = default;
  static std::shared_ptr<StartLoggingRspMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  inline queue::flatbuf::MessageType Type() {
    return queue::flatbuf::MessageType::StreamingQueueStartLoggingRspMsg;
  }
  inline queue::flatbuf::StreamingQueueError ErrorCode() { return err_code_; }

 private:
  queue::flatbuf::StreamingQueueError err_code_;
};

class TestInitMsg : public Message {
 public:
  TestInitMsg(const queue::flatbuf::StreamingQueueTestRole role, const ActorID &actor_id,
              const ActorID &peer_actor_id, const std::string &actor_handle_serialized,
              const std::vector<ObjectID> &queue_ids,
              const std::vector<ObjectID> &rescale_queue_ids,
              std::string &test_suite_name, std::string &test_name, uint64_t param,
              std::string &plasma_store_socket)
      : Message(actor_id, peer_actor_id, queue_ids[0]),
        actor_handle_serialized_(actor_handle_serialized),
        queue_ids_(queue_ids),
        rescale_queue_ids_(rescale_queue_ids),
        role_(role),
        test_suite_name_(test_suite_name),
        test_name_(test_name),
        param_(param),
        plasma_store_socket_(plasma_store_socket) {}
  virtual ~TestInitMsg() = default;

  static std::shared_ptr<TestInitMsg> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueTestInitMsg;
  }
  inline std::string ActorHandleSerialized() { return actor_handle_serialized_; }
  inline queue::flatbuf::StreamingQueueTestRole Role() { return role_; }
  inline std::vector<ObjectID> QueueIds() { return queue_ids_; }
  inline std::vector<ObjectID> RescaleQueueIds() { return rescale_queue_ids_; }
  inline std::string TestSuiteName() { return test_suite_name_; }
  inline std::string TestName() { return test_name_; }
  inline uint64_t Param() { return param_; }
  inline std::string PlasmaStoreSocket() { return plasma_store_socket_; }

  std::string ToString() {
    std::ostringstream os;
    os << "actor_handle_serialized: " << actor_handle_serialized_;
    os << " queue_ids:[";
    for (auto &qid : queue_ids_) {
      os << qid << ",";
    }
    os << "], rescale_queue_ids:[";
    for (auto &qid : rescale_queue_ids_) {
      os << qid << ",";
    }
    os << "],";
    os << " role:" << queue::flatbuf::EnumNameStreamingQueueTestRole(role_);
    os << " suite_name: " << test_suite_name_;
    os << " test_name: " << test_name_;
    os << " param: " << param_;
    return os.str();
  }

 private:
  std::string actor_handle_serialized_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
  queue::flatbuf::StreamingQueueTestRole role_;
  std::string test_suite_name_;
  std::string test_name_;
  uint64_t param_;
  std::string plasma_store_socket_;
};

class TestCheckStatusRspMsg : public Message {
 public:
  TestCheckStatusRspMsg(const std::string &test_name, bool status)
      : test_name_(test_name), status_(status) {}
  virtual ~TestCheckStatusRspMsg() = default;

  static std::shared_ptr<TestCheckStatusRspMsg> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) override;
  inline queue::flatbuf::MessageType Type() override {
    return queue::flatbuf::MessageType::StreamingQueueTestCheckStatusRspMsg;
  }
  inline std::string TestName() { return test_name_; }
  inline bool Status() { return status_; }

 private:
  std::string test_name_;
  bool status_;
};

}  // namespace streaming
}  // namespace ray
#endif
