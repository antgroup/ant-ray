#ifndef _QUEUE_MANAGER_H_
#define _QUEUE_MANAGER_H_

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <mutex>
#include <thread>

#include "elasticbuffer/elastic_buffer.h"
#include "logging.h"
#include "queue/queue.h"
#include "queue_item.h"
#include "receiver.h"

namespace ray {
namespace streaming {

static inline std::ostream &operator<<(std::ostream &os,
                                       const StreamingQueueStatus &status) {
  os << static_cast<std::underlying_type<StreamingQueueStatus>::type>(status);
  return os;
}

/// Base class of UpstreamQueueMessageHandler and DownstreamQueueMessageHandler.
/// A queue service manages a group of queues, upstream queues or downstream queues of
/// the current actor. Each queue service holds a boost.asio io_service, to handle
/// messages asynchronously. When a message received by Writer/Reader in ray call thread,
/// the message was delivered to
/// UpstreamQueueMessageHandler/DownstreamQueueMessageHandler, then the ray call thread
/// returns immediately. The queue service parses meta infomation from the message,
/// including queue_id actor_id, etc, and dispatchs message to queue according to
/// queue_id.
class QueueMessageHandler : public std::enable_shared_from_this<QueueMessageHandler> {
 public:
  /// Construct a QueueMessageHandler instance.
  /// \param[in] actor_id actor id of current actor.
  QueueMessageHandler(const std::string plasma_socket_path, bool enable_collocate,
                      TransportType transport_type)
      : actor_id_(GetCoreWorkerActorID()),
        transport_type_(transport_type),
        queue_dummy_work_(queue_service_) {
    if (enable_collocate && !plasma_socket_path.empty()) {
      plasma_client_ = std::make_shared<plasma::PlasmaClient>();
      STREAMING_LOG(INFO) << "Connect to plasma store server: " << plasma_socket_path;
      ray::Status st = plasma_client_->Connect(plasma_socket_path);
      STREAMING_CHECK(st.ok()) << st;
    }

    switch (transport_type_) {
    case TransportType::MEMORY:
      transport_manager_ = std::make_shared<SimpleMemoryTransportManager>();
      break;
    case TransportType::DIRECTCALL:
      transport_manager_ = std::make_shared<DirectCallTransportManager>();
      break;
    default:
      STREAMING_CHECK(false) << "Unsupported transport type";
    }
    queue_thread_ = std::thread(&QueueMessageHandler::QueueThreadCallback, this);
  }

  virtual ~QueueMessageHandler();

  /// Dispatch message buffer to asio service.
  /// \param[in] buffer serialized message received from peer actor.
  void DispatchMessageAsync(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers);

  /// Dispatch message buffer to asio service synchronously, and wait for handle result.
  /// \param[in] buffer serialized message received from peer actor.
  /// \return handle result.
  std::shared_ptr<LocalMemoryBuffer> DispatchMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer);

  /// Get transport to a peer actor specified by actor_id.
  /// \param[in] actor_id actor id of peer actor
  /// \return transport
  std::shared_ptr<Transport> GetOutTransport(const ObjectID &actor_id);

  /// The actual function where message being dispatched, called by DispatchMessageAsync
  /// and DispatchMessageSync.
  /// \param[in] buffer serialized message received from peer actor.
  /// \param[in] callback the callback function used by DispatchMessageSync, called
  ///            after message processed complete. The std::shared_ptr<LocalMemoryBuffer>
  ///            parameter is the return value.
  virtual void DispatchMessageInternal(
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) = 0;

  /// Save actor_id of the peer actor specified by queue_id. For a upstream queue, the
  /// peer actor refer specifically to the actor in current ray cluster who has a
  /// downstream queue with same queue_id, and vice versa.
  /// \param[in] queue_id queue id of current queue.
  /// \param[in] actor_id actor_id actor id of corresponded peer actor.
  void SetPeerActorID(const ObjectID &queue_id, const ActorID &actor_id);

  /// Obtain the actor id of the peer actor specified by queue_id.
  /// \return actor id
  ActorID GetPeerActorID(const ObjectID &queue_id);

  void ErasePeerActorID(const ObjectID &queue_id);

  /// Save the Transport object used when communicate with the peer actor.
  /// \param[in] queue_id queue id of current queue.
  /// \param[in] transport
  void SetOutTransport(const ObjectID &queue_id, std::shared_ptr<Transport> transport);

  void EraseOutTransport(const ObjectID &queue_id);

  /// Release all queues in current queue service.
  void Release();

  template <typename CompletionHandler>
  void ExecuteInQueueService(BOOST_ASIO_MOVE_ARG(CompletionHandler) handler) {
    queue_service_.post(handler);
  }

  /// The callback function called by CoreWorker when directcall connection abort.
  /// \param[in] The actor id of peer actor.
  virtual void ActorReconnectedCallback(const ActorID &actor_id) = 0;
  /// Start asio service
  virtual void Start();
  /// Stop asio service
  virtual void Stop();

  ActorID GetActorID() { return actor_id_; }

  std::function<void(std::vector<std::shared_ptr<LocalMemoryBuffer>>)> GetAsyncMethod() {
    return
        [this](std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers) {
          this->DispatchMessageAsync(buffers);
        };
  }

  std::function<
      std::shared_ptr<LocalMemoryBuffer>(std::vector<std::shared_ptr<LocalMemoryBuffer>>)>
  GetSyncMethod() {
    return [this](std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers)
               -> std::shared_ptr<ray::streaming::LocalMemoryBuffer> {
      return this->DispatchMessageSync(buffers[0]);
    };
  }

 protected:
  /// The callback function of internal thread.
  void QueueThreadCallback() {
    queue_service_.restart();
    queue_service_.run();
  }
  static ActorID GetCoreWorkerActorID() {
    if (IsInitializedInternal()) {
      return GetCurrentActorIDInternal();
    } else {
      return ActorID::Nil();
    }
  }

 protected:
  /// actor_id actor id of current actor
  ActorID actor_id_;
  /// Helper function, parse message buffer to Message object.
  std::shared_ptr<Message> ParseMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<Message> ParseMessage(
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers);

 public:
  static queue::flatbuf::MessageType GetMessageType(
      std::shared_ptr<LocalMemoryBuffer> buffer);

 protected:
  /// Map from queue id to a actor id of the queue's peer actor.
  std::unordered_map<ObjectID, ActorID> actors_;
  std::shared_ptr<TransportManager> transport_manager_;
  /// Plasma client
  std::shared_ptr<plasma::PlasmaClient> plasma_client_;
  TransportType transport_type_;

 private:
  /// The internal thread which asio service run with.
  std::thread queue_thread_;
  /// The internal asio service.
  boost::asio::io_service queue_service_;
  /// The asio work which keeps queue_service_ alive.
  boost::asio::io_service::work queue_dummy_work_;
  std::mutex actors_mutex_;
};

typedef std::function<void(const ray::ObjectID &queue_id, uint64_t msg_id,
                           uint64_t seq_id, uint64_t barrier_id)>
    OnStartLoggingCallBack;
/// UpstreamQueueMessageHandler holds and manages all upstream queues of current actor.
class UpstreamQueueMessageHandler : public QueueMessageHandler {
 public:
  /// Construct a UpstreamQueueMessageHandler instance.
  UpstreamQueueMessageHandler(const std::string plasma_socket_path,
                              TransportType transport_type = TransportType::DIRECTCALL,
                              const ElasticBufferConfig &es_config = {},
                              OnStartLoggingCallBack start_logging_cb = {},
                              bool enable_collocate = true)
      : QueueMessageHandler(plasma_socket_path, enable_collocate, transport_type),
        resend_service_dummy_worker_(resend_service_),
        start_logging_cb_(start_logging_cb) {
    if (es_config.enable) {
      elasticbuffer_ =
          std::make_shared<ElasticBuffer<QueueItem>>(es_config, ser_func, deser_func);
    } else {
      elasticbuffer_ = nullptr;
    }
  }

  virtual ~UpstreamQueueMessageHandler() = default;
  /// Create a upstream queue.
  /// \param[in] queue_id queue id of the queue to be created.
  /// \param[in] peer_actor_id actor id of peer actor.
  /// \param[in] size the max memory size of the queue.
  std::shared_ptr<WriterQueue> CreateUpstreamQueue(
      const ObjectID &queue_id, const StreamingQueueInitialParameter &parameter,
      uint64_t size, int32_t buffer_pool_min_size, bool enable_collocate = true);
  /// Delete a upstream queue. All data exists in this queue will be lost.
  /// \param[in] queue_id queue id of the queue to be deleted.
  void DeleteUpstreamQueue(const ObjectID &queue_id);
  /// Check whether the upstream queue specified by queue_id exists or not.
  bool UpstreamQueueExists(const ObjectID &queue_id);
  /// Handle notify message from corresponded downstream queue.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);
  /// Handle pull request message from corresponded downstream queue.
  void OnPullRequest(std::shared_ptr<PullRequestMessage> pull_msg,
                     std::function<void(std::unique_ptr<PullResponseMessage>)> callback);
  void OnStartLogging(std::shared_ptr<StartLoggingMessage> start_logging_msg,
                      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback);
  /// Obtain upstream queue specified by queue_id.
  std::shared_ptr<streaming::WriterQueue> GetUpQueue(const ObjectID &queue_id);
  std::shared_ptr<streaming::WriterQueue> GetUpQueueByActorId(const ActorID &actor_id);
  void SetUpQueue(const ObjectID &queue_id, std::shared_ptr<WriterQueue> queue);
  void EraseUpQueue(const ObjectID &queue_id);
  /// Release all upstream queues
  void ReleaseAllUpQueues();

  static ElasticBuffer<QueueItem>::BufferSerilizeFunction ser_func;
  static ElasticBuffer<QueueItem>::BufferDeserilizedFunction deser_func;

  virtual void DispatchMessageInternal(
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) override;

  /// Get the max message id of the last item received by the downstream
  /// queue.
  /// \param[in] queue_id the downstream queue id to get from.
  void GetLastReceivedMsgId(const ObjectID &queue_id, uint64_t &last_queue_msg_id,
                            uint64_t &last_queue_seq_id);
  void ActorReconnectedCallback(const ActorID &actor_id) override;
  virtual void Start() override;
  void SendGetPeerHostnameMsg(const ObjectID queue_id, int32_t buffer_pool_min_size);
  virtual void Stop() override final;

 private:
  std::mutex mutex_;
  std::unordered_map<ObjectID, std::shared_ptr<streaming::WriterQueue>> upstream_queues_;
  boost::asio::io_service resend_service_;
  boost::asio::io_service::work resend_service_dummy_worker_;
  std::thread handle_service_thread_;

  std::shared_ptr<streaming::ElasticBuffer<QueueItem>> elasticbuffer_;
  /// The initializing queue collection, the queue is waiting for peer's hostname return
  /// asynchronous.
  /// Note: The initializing can not be pushed.
  std::list<ObjectID> initializing_queues_list_;
  OnStartLoggingCallBack start_logging_cb_;
};

/// DownstreamQueueMessageHandler holds and manages all downstream queues of current
/// actor.
class DownstreamQueueMessageHandler : public QueueMessageHandler {
 public:
  DownstreamQueueMessageHandler(const std::string plasma_socket_path,
                                TransportType transport_type = TransportType::DIRECTCALL,
                                bool enable_collocate = true)
      : QueueMessageHandler(plasma_socket_path, enable_collocate, transport_type) {}
  virtual ~DownstreamQueueMessageHandler() = default;

  std::shared_ptr<ReaderQueue> CreateDownstreamQueue(
      const ObjectID &queue_id, const StreamingQueueInitialParameter &parameter);
  StreamingQueueStatus PullQueue(const ObjectID &queue_id, uint64_t checkpoint_id,
                                 uint64_t start_msg_id, bool &is_upstream_first_pull,
                                 uint64_t &count, uint64_t timeout_ms = 2000);
  /// Delete a downstream queue. All data exists in this queue will be lost.
  /// \param[in] queue_id queue id of the queue to be deleted.
  void DeleteDownstreamQueue(const ObjectID &queue_id);
  bool DownstreamQueueExists(const ObjectID &queue_id);

  std::shared_ptr<streaming::ReaderQueue> GetDownQueue(const ObjectID &queue_id);
  std::shared_ptr<streaming::ReaderQueue> GetDownQueueByActorId(const ActorID &actor_id);
  void SetDownQueue(const ObjectID &queue_id, std::shared_ptr<ReaderQueue> queue);
  void EraseDownQueue(const ObjectID &queue_id);
  void ReleaseAllDownQueues();

  void OnData(std::shared_ptr<DataMessage> msg);
  void OnData(std::shared_ptr<LocalDataMessage> msg);
  virtual void DispatchMessageInternal(
      std::vector<std::shared_ptr<LocalMemoryBuffer>> buffer,
      std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) override;

  StreamingQueueStatus PullPeerAsync(const ObjectID &queue_id, uint64_t checkpoint_id,
                                     uint64_t start_msg_id, bool &is_upstream_first_pull,
                                     uint64_t &count, uint64_t timeout_ms);

  std::shared_ptr<LocalMemoryBuffer> OnGetLastMsgId(
      std::shared_ptr<GetLastMsgIdMessage> msg);
  void ActorReconnectedCallback(const ActorID &actor_id) override;
  std::shared_ptr<LocalMemoryBuffer> OnGetHostname(
      std::shared_ptr<GetHostnameMessage> msg);

 private:
  std::mutex mutex_;
  std::unordered_map<ObjectID, std::shared_ptr<streaming::ReaderQueue>>
      downstream_queues_;
};

class ActorConnectionListener {
 private:
  ActorConnectionListener() {
    reconnected_callback_ = std::bind(&ActorConnectionListener::ActorReconnectedCallback,
                                      this, std::placeholders::_1);
    CoreWorkerProcess::GetCoreWorker().SubscribeActorReconnected(reconnected_callback_);
  }

 public:
  static ActorConnectionListener *GetInstance() {
    static ActorConnectionListener instance;
    return &instance;
  }

  void SubscribeReconnected(std::shared_ptr<QueueMessageHandler> handler) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = std::find(handlers_.begin(), handlers_.end(), handler);
    if (it == handlers_.end()) {
      handlers_.push_back(handler);
    } else {
      STREAMING_LOG(WARNING) << "Repeat SubscribeReconnected";
    }
  }

  void UnSubscribeReconnected(std::shared_ptr<QueueMessageHandler> handler) {
    std::unique_lock<std::mutex> lock(mutex_);
    handlers_.remove(handler);
  }

  /// For mock test.
  std::function<void(const ActorID &)> GetActorReconnectedCallback() {
    return reconnected_callback_;
  }

 private:
  void ActorReconnectedCallback(const ActorID &actor_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto &handler : handlers_) {
      handler->ActorReconnectedCallback(actor_id);
    }
  }

 private:
  std::mutex mutex_;
  std::list<std::shared_ptr<QueueMessageHandler>> handlers_;
  std::function<void(const ActorID &)> reconnected_callback_;
};
}  // namespace streaming
}  // namespace ray
#endif
