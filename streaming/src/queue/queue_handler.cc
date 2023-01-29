#include "queue_handler.h"

#include "conf/streaming_config.h"
#include "config.h"
#include "queue.h"
#include "util/utility.h"
#include "utils.h"

namespace ray {
namespace streaming {

std::shared_ptr<Message> QueueMessageHandler::ParseMessage(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  uint8_t *bytes = buffer->Data();
  uint8_t *p_cur = bytes;
  uint32_t *magic_num = (uint32_t *)p_cur;
  STREAMING_CHECK(*magic_num == Message::MagicNum)
      << *magic_num << " " << Message::MagicNum;

  p_cur += sizeof(Message::MagicNum);
  queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;

  std::shared_ptr<Message> message = nullptr;
  switch (*type) {
  case queue::flatbuf::MessageType::StreamingQueueNotificationMsg:
    message = NotificationMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueDataMsg:
    message = DataMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueuePullRequestMsg:
    message = PullRequestMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueuePullResponseMsg:
    message = PullResponseMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueGetLastMsgId:
    message = GetLastMsgIdMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp:
    message = GetLastMsgIdRspMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueGetHostnameMsg:
    message = GetHostnameMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueGetHostnameRspMsg:
    message = GetHostnameRspMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueStartLoggingMsg:
    message = StartLoggingMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueStartLoggingRspMsg:
    message = StartLoggingRspMessage::FromBytes(bytes);
    break;
  default:
    STREAMING_CHECK(false) << "nonsupport message type: "
                           << queue::flatbuf::EnumNameMessageType(*type);
    break;
  }

  return message;
}

std::shared_ptr<Message> QueueMessageHandler::ParseMessage(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
  size_t buffer_size = buffers.size();
  STREAMING_CHECK(buffer_size <= 3) << "Invalid buffer size: " << buffer_size;
  if (buffer_size == 1) {
    return ParseMessage(buffers[0]);
  }

  /// buffers[0]: item meta buffer
  /// buffers[1]: bundle meta buffer
  /// buffers[2]: bundle data buffer
  uint8_t *bytes = buffers[0]->Data();
  uint8_t *p_cur = bytes;
  uint32_t *magic_num = (uint32_t *)p_cur;
  STREAMING_CHECK(*magic_num == Message::MagicNum)
      << *magic_num << " " << Message::MagicNum;

  p_cur += sizeof(Message::MagicNum);
  queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;

  std::shared_ptr<Message> message = nullptr;
  switch (*type) {
  case queue::flatbuf::MessageType::StreamingQueueDataMsg:
    message = DataMessage::FromBytes(buffers);
    break;
  case queue::flatbuf::MessageType::StreamingQueueLocalDataMsg:
    message = LocalDataMessage::FromBytes(buffers);
    break;
  default:
    STREAMING_CHECK(false) << "nonsupport message type: "
                           << queue::flatbuf::EnumNameMessageType(*type);
    break;
  }

  return message;
}

queue::flatbuf::MessageType QueueMessageHandler::GetMessageType(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  uint8_t *bytes = buffer->Data();
  uint8_t *p_cur = bytes;
  uint32_t *magic_num = (uint32_t *)p_cur;
  STREAMING_CHECK(*magic_num == Message::MagicNum)
      << *magic_num << " " << Message::MagicNum;

  p_cur += sizeof(Message::MagicNum);
  queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;
  return *type;
}

void QueueMessageHandler::DispatchMessageAsync(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
  STREAMING_LOG(DEBUG) << "QueueMessageHandler::DispatchMessageAsync " << buffers.size();
  queue_service_.post(
      boost::bind(&QueueMessageHandler::DispatchMessageInternal, this, buffers, nullptr));
}

std::shared_ptr<LocalMemoryBuffer> QueueMessageHandler::DispatchMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers{buffer};
  std::shared_ptr<LocalMemoryBuffer> result = nullptr;
  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  queue_service_.post(
      boost::bind(&QueueMessageHandler::DispatchMessageInternal, this, buffers,
                  [&promise, &result](std::shared_ptr<LocalMemoryBuffer> rst) {
                    result = rst;
                    promise->Notify(ray::Status::OK());
                  }));
  Status st = promise->Wait();
  STREAMING_CHECK(st.ok());

  return result;
}

void QueueMessageHandler::SetPeerActorID(const ObjectID &queue_id,
                                         const ActorID &actor_id) {
  std::lock_guard<std::mutex> lock(actors_mutex_);
  actors_.emplace(queue_id, actor_id);
}

void QueueMessageHandler::SetOutTransport(const ObjectID &queue_id,
                                          std::shared_ptr<Transport> transport) {
  transport_manager_->SetOutTransport(queue_id, transport);
}

ActorID QueueMessageHandler::GetPeerActorID(const ObjectID &queue_id) {
  std::lock_guard<std::mutex> lock(actors_mutex_);
  auto it = actors_.find(queue_id);
  STREAMING_CHECK(it != actors_.end());
  return it->second;
}

void QueueMessageHandler::ErasePeerActorID(const ObjectID &queue_id) {
  std::lock_guard<std::mutex> lock(actors_mutex_);
  actors_.erase(queue_id);
}

void QueueMessageHandler::Release() {
  std::lock_guard<std::mutex> lock(actors_mutex_);
  actors_.clear();
}

void QueueMessageHandler::Start() {
  STREAMING_LOG(INFO) << "QueueMessageHandler::Start";
  if (transport_type_ == TransportType::DIRECTCALL &&
      ray::CoreWorkerProcess::IsInitialized()) {
    ActorConnectionListener::GetInstance()->SubscribeReconnected(shared_from_this());
  }
}

void QueueMessageHandler::Stop() {
  STREAMING_LOG(INFO) << "QueueMessageHandler Stop.";
  if (transport_type_ == TransportType::DIRECTCALL &&
      ray::CoreWorkerProcess::IsInitialized()) {
    ActorConnectionListener::GetInstance()->UnSubscribeReconnected(shared_from_this());
  }
}

QueueMessageHandler::~QueueMessageHandler() {
  queue_service_.stop();
  if (queue_thread_.joinable()) {
    queue_thread_.join();
  }
  if (plasma_client_) {
    RAY_IGNORE_EXPR(plasma_client_->Disconnect());
    plasma_client_ = nullptr;
  }
}

ElasticBuffer<QueueItem>::BufferSerilizeFunction UpstreamQueueMessageHandler::ser_func =
    [](QueueItem &ptr) {
      DataUnit du;
      du.size = ptr.SerializeSize();
      du.data = new uint8_t[du.size];
      du.use_copy = true;
      ptr.ToSerialize(du.data);
      return du;
    };

ElasticBuffer<QueueItem>::BufferDeserilizedFunction
    UpstreamQueueMessageHandler::deser_func = [](DataUnit &ptr) {
      std::shared_ptr<QueueItem> item(new QueueItem(ptr.data, ptr.size));
      return item;
    };

void UpstreamQueueMessageHandler::Start() {
  STREAMING_LOG(INFO) << "UpstreamQueueMessageHandler::Start";
  QueueMessageHandler::Start();
  handle_service_thread_ = std::thread([this] {
    STREAMING_LOG(INFO) << "This is handle_service_thread.";
    resend_service_.restart();
    resend_service_.run();
  });
}

void UpstreamQueueMessageHandler::Stop() {
  STREAMING_LOG(INFO) << "UpstreamQueueMessageHandler::Stop";
  resend_service_.stop();
  if (handle_service_thread_.joinable()) {
    handle_service_thread_.join();
  }
  QueueMessageHandler::Stop();
}

std::shared_ptr<WriterQueue> UpstreamQueueMessageHandler::CreateUpstreamQueue(
    const ObjectID &queue_id, const StreamingQueueInitialParameter &parameter,
    uint64_t size, int32_t buffer_pool_min_size, bool enable_collocate) {
  STREAMING_LOG(INFO) << "CreateUpstreamQueue: " << queue_id << " " << actor_id_ << "->"
                      << parameter.actor_id << " queue size: " << size
                      << " buffer_pool_min_size: " << buffer_pool_min_size
                      << " cyclic: " << parameter.cyclic;
  std::shared_ptr<WriterQueue> queue = GetUpQueue(queue_id);
  if (queue != nullptr) {
    STREAMING_LOG(WARNING) << "Duplicate to create up queue." << queue_id;
    return queue;
  }

  SetPeerActorID(queue_id, parameter.actor_id);
  if (transport_type_ == TransportType::DIRECTCALL) {
    SetOutTransport(queue_id, std::make_shared<ray::streaming::DirectCallTransport>(
                                  parameter.actor_id, *parameter.async_function,
                                  *parameter.sync_function));
  }

  if (parameter.cyclic) {
    queue = std::unique_ptr<CyclicWriterQueue>(new CyclicWriterQueue(
        queue_id, actor_id_, parameter.actor_id, parameter.cyclic, size,
        transport_manager_->GetOutTransport(queue_id), elasticbuffer_));
  } else {
    queue = std::unique_ptr<WriterQueue>(
        new WriterQueue(queue_id, actor_id_, parameter.actor_id, parameter.cyclic, size,
                        transport_manager_->GetOutTransport(queue_id), elasticbuffer_));
  }

  if (enable_collocate) {
    SendGetPeerHostnameMsg(queue->GetQueueID(), buffer_pool_min_size);
  } else {
    STREAMING_LOG(INFO) << "Collocate disabled.";
    queue->CreateInternalBuffer(plasma_client_, buffer_pool_min_size);
    queue->SetInitialized(true);
  }

  SetUpQueue(queue_id, queue);
  return queue;
}

void UpstreamQueueMessageHandler::DeleteUpstreamQueue(const ObjectID &queue_id) {
  STREAMING_LOG(INFO) << "DeleteUpstreamQueue queue: " << queue_id;
  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  ExecuteInQueueService([this, &queue_id, &promise] {
    ErasePeerActorID(queue_id);
    transport_manager_->EraseOutTransport(queue_id);
    EraseUpQueue(queue_id);
    promise->Notify(ray::Status::OK());
  });

  Status st = promise->Wait();
  STREAMING_LOG(INFO) << "DeleteUpstreamQueue done";
}

bool UpstreamQueueMessageHandler::UpstreamQueueExists(const ObjectID &queue_id) {
  return nullptr != GetUpQueue(queue_id);
}

std::shared_ptr<streaming::WriterQueue> UpstreamQueueMessageHandler::GetUpQueue(
    const ObjectID &queue_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = upstream_queues_.find(queue_id);
  if (it == upstream_queues_.end()) return nullptr;

  return it->second;
}

std::shared_ptr<streaming::WriterQueue> UpstreamQueueMessageHandler::GetUpQueueByActorId(
    const ActorID &actor_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = std::find_if(
      upstream_queues_.begin(), upstream_queues_.end(),
      [&actor_id](const std::unordered_map<
                  ObjectID, std::shared_ptr<streaming::WriterQueue>>::value_type &pair) {
        return actor_id == pair.second->GetPeerActorID();
      });
  if (it == upstream_queues_.end()) return nullptr;

  return it->second;
}

void UpstreamQueueMessageHandler::SetUpQueue(const ObjectID &queue_id,
                                             std::shared_ptr<WriterQueue> queue) {
  std::lock_guard<std::mutex> lock(mutex_);
  upstream_queues_[queue_id] = queue;
}

void UpstreamQueueMessageHandler::EraseUpQueue(const ObjectID &queue_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  upstream_queues_.erase(queue_id);
}

void UpstreamQueueMessageHandler::DispatchMessageInternal(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  std::shared_ptr<Message> msg = ParseMessage(buffers);
  STREAMING_LOG(DEBUG) << "UpstreamQueueMessageHandler::DispatchMessageInternal: "
                       << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
                       << " peer actorid: " << msg->PeerActorId()
                       << " type: " << queue::flatbuf::EnumNameMessageType(msg->Type());

  if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueNotificationMsg) {
    STREAMING_CHECK(callback == nullptr)
        << "StreamingQueueNotificationMsg "
        << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
        << " peer actorid: " << msg->PeerActorId();
    OnNotify(std::dynamic_pointer_cast<NotificationMessage>(msg));
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueuePullRequestMsg) {
    STREAMING_CHECK(callback) << "StreamingQueuePullRequestMsg "
                              << " qid: " << msg->QueueId() << " actorid "
                              << msg->ActorId()
                              << " peer actorid: " << msg->PeerActorId();
    OnPullRequest(std::dynamic_pointer_cast<PullRequestMessage>(msg),
                  [callback](std::shared_ptr<PullResponseMessage> response) {
                    auto buffer = response->ToBytes();
                    callback(std::move(buffer));
                  });
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp) {
    STREAMING_CHECK(false) << "Should not receive StreamingQueueGetLastMsgIdRsp";
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueStartLoggingMsg) {
    STREAMING_CHECK(callback) << "StreamingQueueStartLoggingMsg "
                              << " qid: " << msg->QueueId() << " actorid "
                              << msg->ActorId()
                              << " peer actorid: " << msg->PeerActorId();
    OnStartLogging(std::dynamic_pointer_cast<StartLoggingMessage>(msg), callback);
  } else {
    STREAMING_CHECK(false) << "message type should be added: "
                           << queue::flatbuf::EnumNameMessageType(msg->Type());
  }
}

void UpstreamQueueMessageHandler::OnNotify(
    std::shared_ptr<NotificationMessage> notify_msg) {
  auto queue = GetUpQueue(notify_msg->QueueId());
  if (queue == nullptr) {
    STREAMING_LOG(DEBUG) << "Can not find queue for "
                         << queue::flatbuf::EnumNameMessageType(notify_msg->Type())
                         << ", maybe queue has been destroyed, ignore it."
                         << " msg id: " << notify_msg->MsgId();
    return;
  }
  queue->OnNotify(notify_msg);
}

void UpstreamQueueMessageHandler::OnPullRequest(
    std::shared_ptr<PullRequestMessage> pull_msg,
    std::function<void(std::unique_ptr<PullResponseMessage>)> callback) {
  STREAMING_LOG(INFO) << "OnPullRequest from " << pull_msg->QueueId()
                      << " checkpoint id: " << pull_msg->CheckpointId()
                      << " message id: " << pull_msg->MsgId();
  auto queue = GetUpQueue(pull_msg->QueueId());
  if (nullptr == queue) {
    STREAMING_LOG(INFO) << "Can not find queue " << pull_msg->QueueId();
    auto response = std::unique_ptr<PullResponseMessage>(new PullResponseMessage(
        pull_msg->PeerActorId(), pull_msg->ActorId(), pull_msg->QueueId(),
        QUEUE_INVALID_SEQ_ID, QUEUE_INVALID_SEQ_ID, 0,
        queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST, false));
    callback(std::move(response));
    return;
  }

  queue->OnPull(pull_msg, resend_service_, callback);
}

void UpstreamQueueMessageHandler::OnStartLogging(
    std::shared_ptr<StartLoggingMessage> start_logging_msg,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  STREAMING_LOG(INFO) << "OnStartLogging";
  auto queue = GetUpQueue(start_logging_msg->QueueId());
  if (nullptr == queue) {
    STREAMING_LOG(INFO) << "Can not find queue " << start_logging_msg->QueueId();
    StartLoggingRspMessage msg(start_logging_msg->PeerActorId(),
                               start_logging_msg->ActorId(), start_logging_msg->QueueId(),
                               queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST);
    callback(msg.ToBytes());
  } else {
    /// Trigger DataWriter to start logging.
    start_logging_cb_(start_logging_msg->QueueId(), start_logging_msg->MsgId(),
                      start_logging_msg->SeqId(), start_logging_msg->BarrierId());
    StartLoggingRspMessage msg(start_logging_msg->PeerActorId(),
                               start_logging_msg->ActorId(), start_logging_msg->QueueId(),
                               queue::flatbuf::StreamingQueueError::OK);
    callback(msg.ToBytes());
  }
}

void UpstreamQueueMessageHandler::ReleaseAllUpQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllUpQueues";
  upstream_queues_.clear();
  Release();
}

void UpstreamQueueMessageHandler::GetLastReceivedMsgId(const ObjectID &queue_id,
                                                       uint64_t &last_queue_msg_id,
                                                       uint64_t &last_queue_seq_id) {
  STREAMING_LOG(INFO) << "GetPeerLastMsgId qid: " << queue_id;
  ActorID peer_actor_id = GetPeerActorID(queue_id);

  GetLastMsgIdMessage msg(actor_id_, peer_actor_id, queue_id);

  auto transport_it = transport_manager_->GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer =
      transport_it->SendForResultWithRetry(msg.ToBytes());
  if (result_buffer == nullptr) {
    last_queue_msg_id = 0;
    last_queue_seq_id = QUEUE_INVALID_SEQ_ID;
    return;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(result_msg->Type() ==
                  queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp);
  std::shared_ptr<GetLastMsgIdRspMessage> get_rsp_msg =
      std::dynamic_pointer_cast<GetLastMsgIdRspMessage>(result_msg);
  STREAMING_LOG(INFO) << "GetPeerLastMsgId return queue_id: " << get_rsp_msg->QueueId()
                      << " error: "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             get_rsp_msg->ErrorCode());
  STREAMING_CHECK(get_rsp_msg->PeerActorId() == actor_id_);

  if (queue::flatbuf::StreamingQueueError::OK != get_rsp_msg->ErrorCode()) {
    // return 0 because PlasmaProducer::FetchLastMessageIdFromQueue return 0 by default.
    last_queue_msg_id = 0;
    // set to QUEUE_INVALID_SEQ_ID because PlasmaProducer::FetchLastMessageIdFromQueue
    // 'queue_last_message_id == static_cast<uint64_t>(-1)'
    last_queue_seq_id = QUEUE_INVALID_SEQ_ID;
    return;
  }

  last_queue_seq_id = get_rsp_msg->SeqId();
  last_queue_msg_id = get_rsp_msg->MsgId();
}

void UpstreamQueueMessageHandler::ActorReconnectedCallback(const ActorID &actor_id) {
  /// `ActorReconnectedCallback` will be called in CoreWorker IO thread.
  auto queue = GetUpQueueByActorId(actor_id);
  if (queue != nullptr) {
    STREAMING_LOG(WARNING) << "Up stream queue receive actor reconnected, peer actorid: "
                           << actor_id << " queue_id: " << queue->GetQueueID();
    queue->Resend(queue->GetMinConsumedMsgID() + 1, resend_service_);
  } else {
    STREAMING_LOG(WARNING) << "Unexpected actor reconnected callback, peer actorid: "
                           << actor_id;
  }
}

void UpstreamQueueMessageHandler::SendGetPeerHostnameMsg(const ObjectID queue_id,
                                                         int32_t buffer_pool_min_size) {
  STREAMING_LOG(INFO) << "SendGetPeerHostnameMsg qid: " << queue_id;
  ActorID peer_actor_id = GetPeerActorID(queue_id);

  GetHostnameMessage msg(actor_id_, peer_actor_id, queue_id);

  transport_manager_->SendForResultWithRetryAsync(
      queue_id, msg.ToBytes(),
      [this, buffer_pool_min_size](ObjectID queue_id,
                                   std::shared_ptr<LocalMemoryBuffer> buffer) {
        std::shared_ptr<Message> result_msg = ParseMessage(buffer);
        STREAMING_CHECK(result_msg->Type() ==
                        queue::flatbuf::MessageType::StreamingQueueGetHostnameRspMsg);
        std::shared_ptr<GetHostnameRspMessage> get_rsp_msg =
            std::dynamic_pointer_cast<GetHostnameRspMessage>(result_msg);
        STREAMING_LOG(INFO) << "GetPeerHostname return queue_id: "
                            << get_rsp_msg->QueueId() << " error: "
                            << queue::flatbuf::EnumNameStreamingQueueError(
                                   get_rsp_msg->ErrorCode());
        STREAMING_CHECK(get_rsp_msg->PeerActorId() == actor_id_);

        if (queue::flatbuf::StreamingQueueError::OK != get_rsp_msg->ErrorCode()) {
          STREAMING_LOG(INFO) << "Resend GetPeerHostnameMsg";
          SendGetPeerHostnameMsg(queue_id, buffer_pool_min_size);
          return;
        }

        auto queue = GetUpQueue(queue_id);
        if (nullptr == queue) {
          STREAMING_LOG(WARNING)
              << "Queue " << queue_id << " not exist, maybe has been deleted.";
          return;
        }
        queue->SetPeerHostname(get_rsp_msg->Hostname());

        queue->CreateInternalBuffer(plasma_client_, buffer_pool_min_size);
        queue->SetInitialized(true);
      });
}

bool DownstreamQueueMessageHandler::DownstreamQueueExists(const ObjectID &queue_id) {
  return nullptr != GetDownQueue(queue_id);
}

std::shared_ptr<ReaderQueue> DownstreamQueueMessageHandler::CreateDownstreamQueue(
    const ObjectID &queue_id, const StreamingQueueInitialParameter &parameter) {
  STREAMING_LOG(INFO) << "CreateDownstreamQueue: " << queue_id << " "
                      << parameter.actor_id << "->" << actor_id_
                      << " cyclic: " << parameter.cyclic
                      << " mock_transport: " << parameter.mock_transport;
  auto queue = GetDownQueue(queue_id);
  if (nullptr != queue) {
    STREAMING_LOG(WARNING) << "Duplicate to create down queue!!!! " << queue_id;
    return queue;
  }
  SetPeerActorID(queue_id, parameter.actor_id);
  if (transport_type_ == TransportType::DIRECTCALL) {
    SetOutTransport(queue_id, std::make_shared<ray::streaming::DirectCallTransport>(
                                  parameter.actor_id, *parameter.async_function,
                                  *parameter.sync_function));
  }

  queue = std::unique_ptr<streaming::ReaderQueue>(new streaming::ReaderQueue(
      queue_id, actor_id_, parameter.actor_id, parameter.cyclic,
      transport_manager_->GetOutTransport(queue_id)));
  SetDownQueue(queue_id, queue);
  return queue;
}

StreamingQueueStatus DownstreamQueueMessageHandler::PullQueue(
    const ObjectID &queue_id, uint64_t checkpoint_id, uint64_t start_msg_id,
    bool &is_upstream_first_pull, uint64_t &count, uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "PullQueue queue_id: " << queue_id
                      << " checkpoint_id: " << checkpoint_id
                      << " start_msg_id: " << start_msg_id
                      << " is_upstream_first_pull: " << is_upstream_first_pull;
  uint64_t start_time_ms = current_sys_time_ms();
  uint64_t current_time_ms = start_time_ms;
  StreamingQueueStatus st = StreamingQueueStatus::OK;
  uint64_t wait_time_ms = GetDownQueue(queue_id)->IsCyclic() ? 5000 : 1000;
  while (current_time_ms < start_time_ms + timeout_ms &&
         (st = PullPeerAsync(queue_id, checkpoint_id, start_msg_id,
                             is_upstream_first_pull, count, wait_time_ms)) ==
             StreamingQueueStatus::Timeout) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    current_time_ms = current_sys_time_ms();
  }
  GetDownQueue(queue_id)->SetPullStatus(st);
  return st;
}

void DownstreamQueueMessageHandler::DeleteDownstreamQueue(const ObjectID &queue_id) {
  STREAMING_LOG(INFO) << "DeleteDownstreamQueue queue: " << queue_id;
  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  ExecuteInQueueService([this, &queue_id, &promise] {
    ErasePeerActorID(queue_id);
    transport_manager_->EraseOutTransport(queue_id);
    EraseDownQueue(queue_id);
    promise->Notify(ray::Status::OK());
  });

  Status st = promise->Wait();
  STREAMING_LOG(INFO) << "DeleteDownstreamQueue done";
}

std::shared_ptr<streaming::ReaderQueue> DownstreamQueueMessageHandler::GetDownQueue(
    const ObjectID &queue_id) {
  auto it = downstream_queues_.find(queue_id);
  if (it == downstream_queues_.end()) return nullptr;

  return it->second;
}

std::shared_ptr<streaming::ReaderQueue>
DownstreamQueueMessageHandler::GetDownQueueByActorId(const ActorID &actor_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = std::find_if(
      downstream_queues_.begin(), downstream_queues_.end(),
      [&actor_id](const std::unordered_map<
                  ObjectID, std::shared_ptr<streaming::ReaderQueue>>::value_type &pair) {
        return actor_id == pair.second->GetPeerActorID();
      });
  if (it == downstream_queues_.end()) return nullptr;

  return it->second;
}

void DownstreamQueueMessageHandler::SetDownQueue(const ObjectID &queue_id,
                                                 std::shared_ptr<ReaderQueue> queue) {
  std::lock_guard<std::mutex> lock(mutex_);
  downstream_queues_[queue_id] = queue;
}

void DownstreamQueueMessageHandler::EraseDownQueue(const ObjectID &queue_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  downstream_queues_.erase(queue_id);
}

void DownstreamQueueMessageHandler::ReleaseAllDownQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllDownQueues size: " << downstream_queues_.size();
  downstream_queues_.clear();
  Release();
}

void DownstreamQueueMessageHandler::DispatchMessageInternal(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  std::shared_ptr<Message> msg = ParseMessage(buffers);
  STREAMING_LOG(DEBUG) << "DownstreamQueueMessageHandler::DispatchMessageInternal: "
                       << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
                       << " peer actorid: " << msg->PeerActorId()
                       << " type: " << queue::flatbuf::EnumNameMessageType(msg->Type());

  if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueDataMsg) {
    STREAMING_CHECK(callback == nullptr)
        << "StreamingQueueDataMsg "
        << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
        << " peer actorid: " << msg->PeerActorId();
    OnData(std::dynamic_pointer_cast<DataMessage>(msg));
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueLocalDataMsg) {
    STREAMING_CHECK(callback == nullptr)
        << "StreamingQueueLocalDataMsg "
        << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
        << " peer actorid: " << msg->PeerActorId();
    OnData(std::dynamic_pointer_cast<LocalDataMessage>(msg));
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueGetLastMsgId) {
    STREAMING_CHECK(callback) << "StreamingQueueGetLastMsgId "
                              << " qid: " << msg->QueueId() << " actorid "
                              << msg->ActorId()
                              << " peer actorid: " << msg->PeerActorId();
    if (callback != nullptr) {
      callback(this->OnGetLastMsgId(std::dynamic_pointer_cast<GetLastMsgIdMessage>(msg)));
    }
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueGetHostnameMsg) {
    STREAMING_CHECK(callback) << "StreamingQueueGetLastMsgId "
                              << " qid: " << msg->QueueId() << " actorid "
                              << msg->ActorId()
                              << " peer actorid: " << msg->PeerActorId();
    callback(this->OnGetHostname(std::dynamic_pointer_cast<GetHostnameMessage>(msg)));
  } else {
    STREAMING_CHECK(false) << "message type should be added: "
                           << queue::flatbuf::EnumNameMessageType(msg->Type());
  }
}

void DownstreamQueueMessageHandler::OnData(std::shared_ptr<DataMessage> msg) {
  auto queue = GetDownQueue(msg->QueueId());
  if (queue == nullptr) {
    STREAMING_LOG(WARNING) << "Can not find queue for "
                           << queue::flatbuf::EnumNameMessageType(msg->Type())
                           << ", maybe queue has been destroyed, ignore it."
                           << " seq id: " << msg->SeqId();
    return;
  }

  if (msg->IsResend()) {
    STREAMING_LOG(INFO) << "OnData receive resend message queue_id: " << msg->QueueId()
                        << " seq_id: " << msg->SeqId() << " msg_id:(" << msg->MsgIdStart()
                        << "," << msg->MsgIdEnd() << ")";
  }

  STREAMING_CHECK(msg->Buffer());
  QueueItem item(msg, current_sys_time_ms());
  if (msg->IsResend()) {
    queue->OnResendData(item, msg->ResendId(), msg->ResendReason());
  } else {
    queue->OnData(item);
  }
}

void DownstreamQueueMessageHandler::OnData(std::shared_ptr<LocalDataMessage> msg) {
  auto queue = GetDownQueue(msg->QueueId());
  if (queue == nullptr) {
    STREAMING_LOG(WARNING) << "Can not find queue for "
                           << queue::flatbuf::EnumNameMessageType(msg->Type())
                           << ", maybe queue has been destroyed, ignore it."
                           << " seq id: " << msg->SeqId();
    return;
  }
  if (msg->IsResend()) {
    STREAMING_LOG(INFO) << "OnData receive resend message queue_id: " << msg->QueueId()
                        << " seq_id: " << msg->SeqId() << " msg_id:(" << msg->MsgIdStart()
                        << "," << msg->MsgIdEnd() << ")";
  }
  if (!queue->InternalBufferInitialed(msg->GetCollocateObjectId())) {
    if (!queue->CreateInternalBuffer(plasma_client_, msg->GetCollocateObjectId())) {
      STREAMING_LOG(WARNING)
          << "Create internal buffer fail, skip the message from queue: "
          << msg->QueueId();
      return;
    }
  }
  std::shared_ptr<LocalMemoryBuffer> data_buffer = nullptr;
  if (msg->GetDataSize() != 0) {
    data_buffer = std::make_shared<LocalMemoryBuffer>(
        queue->GetBufferPtr(msg->GetDataOffset()), msg->GetDataSize());
  }

  QueueItem item(msg, data_buffer, current_sys_time_ms());
  if (msg->IsResend()) {
    queue->OnResendData(item, msg->ResendId(), msg->ResendReason());
  } else {
    queue->OnData(item);
  }
}

StreamingQueueStatus DownstreamQueueMessageHandler::PullPeerAsync(
    const ObjectID &queue_id, uint64_t checkpoint_id, uint64_t start_msg_id,
    bool &is_upstream_first_pull, uint64_t &count, uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "PullPeerAsync queue_id: " << queue_id
                      << " start_msg_id: " << start_msg_id;
  auto queue = GetDownQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);
  queue->SetStartMsgId(start_msg_id);
  PullRequestMessage msg(queue->GetActorID(), queue->GetPeerActorID(), queue_id,
                         checkpoint_id, start_msg_id);

  auto transport_it = transport_manager_->GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer =
      transport_it->SendForResultWithRetry(msg.ToBytes(), 1, timeout_ms);
  if (result_buffer == nullptr) {
    STREAMING_LOG(WARNING) << "PullPeerAsync timeout queue_id: " << queue_id;
    return StreamingQueueStatus::Timeout;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(result_msg->Type() ==
                  queue::flatbuf::MessageType::StreamingQueuePullResponseMsg);
  std::shared_ptr<PullResponseMessage> response_msg =
      std::dynamic_pointer_cast<PullResponseMessage>(result_msg);

  STREAMING_LOG(INFO) << "PullPeerAsync error: "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             response_msg->ErrorCode())
                      << " start_msg_id: " << start_msg_id;

  is_upstream_first_pull = response_msg->IsUpstreamFirstPull();
  count = response_msg->Count();
  if (response_msg->ErrorCode() == queue::flatbuf::StreamingQueueError::OK) {
    return StreamingQueueStatus::OK;
  } else if (response_msg->ErrorCode() ==
             queue::flatbuf::StreamingQueueError::DATA_LOST) {
    return StreamingQueueStatus::DataLost;
  } else if (response_msg->ErrorCode() ==
             queue::flatbuf::StreamingQueueError::NO_VALID_DATA) {
    return StreamingQueueStatus::NoValidData;
  } else {  // QUEUE_NOT_EXIST
    return StreamingQueueStatus::Timeout;
  }
}

std::shared_ptr<LocalMemoryBuffer> DownstreamQueueMessageHandler::OnGetLastMsgId(
    std::shared_ptr<GetLastMsgIdMessage> get_msg) {
  STREAMING_LOG(WARNING) << "OnGetLastMsgId " << get_msg->QueueId();
  auto down_queue = GetDownQueue(get_msg->QueueId());
  if (down_queue == nullptr) {
    STREAMING_LOG(WARNING) << "OnGetLastMsgId " << get_msg->QueueId() << " not found.";
    queue::flatbuf::StreamingQueueError err_code =
        queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST;
    GetLastMsgIdRspMessage msg(get_msg->PeerActorId(), get_msg->ActorId(),
                               get_msg->QueueId(), QUEUE_INVALID_SEQ_ID,
                               QUEUE_INVALID_SEQ_ID, err_code);
    std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    return buffer;
  } else {
    queue::flatbuf::StreamingQueueError err_code =
        queue::flatbuf::StreamingQueueError::OK;
    STREAMING_LOG(INFO) << "OnGetLastMsgId msg_id: " << down_queue->GetLastRecvMsgId()
                        << " seq_id: " << down_queue->GetLastRecvSeqId();
    GetLastMsgIdRspMessage msg(get_msg->PeerActorId(), get_msg->ActorId(),
                               get_msg->QueueId(), down_queue->GetLastRecvSeqId(),
                               down_queue->GetLastRecvMsgId(), err_code);
    std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    return buffer;
  }
}

void DownstreamQueueMessageHandler::ActorReconnectedCallback(const ActorID &actor_id) {
  /// `ActorReconnectedCallback` will be called in CoreWorker IO thread.
  auto queue = GetDownQueueByActorId(actor_id);
  if (queue != nullptr) {
    STREAMING_LOG(WARNING)
        << "Down stream queue receive actor reconnected, peer actorid: " << actor_id
        << " queue_id: " << queue->GetQueueID();
    queue->IncReconnectedCount();
  } else {
    STREAMING_LOG(WARNING) << "Unexpected actor reconnected callback, peer actorid: "
                           << actor_id;
  }
}

std::shared_ptr<LocalMemoryBuffer> DownstreamQueueMessageHandler::OnGetHostname(
    std::shared_ptr<GetHostnameMessage> get_msg) {
  STREAMING_LOG(WARNING) << "OnGetHostname " << get_msg->QueueId();
  auto down_queue = GetDownQueue(get_msg->QueueId());
  if (down_queue == nullptr) {
    STREAMING_LOG(WARNING) << "OnGetHostname " << get_msg->QueueId() << " not found.";
    GetHostnameRspMessage msg(get_msg->PeerActorId(), get_msg->ActorId(),
                              get_msg->QueueId(), "",
                              queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST);
    std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    return buffer;
  } else {
    std::string hostname = StreamingUtility::GetHostname();
    STREAMING_LOG(INFO) << "OnGetHostname hostname: " << hostname;
    GetHostnameRspMessage msg(get_msg->PeerActorId(), get_msg->ActorId(),
                              get_msg->QueueId(), hostname,
                              queue::flatbuf::StreamingQueueError::OK);
    std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    return buffer;
  }
}

}  // namespace streaming
}  // namespace ray
