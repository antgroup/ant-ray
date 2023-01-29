#ifndef _STREAMING_QUEUE_H_
#define _STREAMING_QUEUE_H_
#include <iterator>
#include <list>
#include <vector>

#include "buffer_pool/buffer_pool.h"
#include "common/status.h"
#include "elasticbuffer/elastic_buffer.h"
#include "queue_item.h"
#include "ray/common/id.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/util/util.h"
#include "streaming/src/util/logging.h"
#include "transport.h"
#include "utils.h"

namespace ray {
namespace streaming {

using ray::ObjectID;

enum QueueType { UPSTREAM = 0, DOWNSTREAM };

struct StreamingQueueInitialParameter {
 public:
  StreamingQueueInitialParameter(const ActorID &id,
                                 std::shared_ptr<ray::RayFunction> async,
                                 std::shared_ptr<ray::RayFunction> sync, bool c, bool m) {
    actor_id = id;
    async_function = async;
    sync_function = sync;
    cyclic = c;
    mock_transport = m;
  }
  StreamingQueueInitialParameter(const ActorID &id,
                                 std::shared_ptr<ray::RayFunction> async,
                                 std::shared_ptr<ray::RayFunction> sync) {
    actor_id = id;
    async_function = async;
    sync_function = sync;
    cyclic = false;
    mock_transport = false;
  }
  StreamingQueueInitialParameter() {}
  // actor id of peer actor
  ActorID actor_id;
  // direct call asynchronous entry function
  std::shared_ptr<ray::RayFunction> async_function;
  // direct call synchronous entry function
  std::shared_ptr<ray::RayFunction> sync_function;
  // whether the queue is cyclic queue or not
  bool cyclic = false;
  // Use mock transport instead of direct call transport, only work in tests.
  bool mock_transport = false;
};

enum class StreamingQueueStatus : uint32_t {
  OK = 0,
  Timeout = 1,
  DataLost = 2,  // The data in upstream has been evicted when downstream try to pull data
                 // from upstream.
  NoValidData = 3,  // There is no data written into queue, or start_msg_id is bigger than
                    // all items in queue now.
  Invalid = 4,
};

/// A queue-like data structure, which do not delete it's item
/// after poped. The lifecycle of each item is:
/// - Pending, a item is pushed into queue, but has not been
///   processed (sent out or consumed),
/// - Processed, has been handled by user, but should not be deleted
/// - Evicted, useless to user, should be poped and destroyed.
/// At present, this data structure is implemented with one std::list,
/// using a watershed iterator to divided.
class Queue {
 public:
  /// \param size max size of the queue in bytes.
  Queue(const ActorID &actor_id, const ActorID &peer_actor_id, const ObjectID &queue_id,
        uint64_t size, std::shared_ptr<Transport> transport, bool cyclic)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        max_data_size_(size),
        data_size_(0),
        last_pop_msg_id_(QUEUE_INVALID_SEQ_ID),
        last_pop_seq_id_(QUEUE_INVALID_SEQ_ID),
        reconnected_count_(0),
        object_id_(ObjectID::Nil()),
        collocate_(false),
        plasma_client_(nullptr),
        cyclic_(cyclic),
        pull_status_(StreamingQueueStatus::Invalid) {
    buffer_queue_.push_back(QueueItem::InvalidQueueItem());
    watershed_iter_ = buffer_queue_.begin();
  }

  virtual ~Queue() {}

  /// Push item into queue, return false is queue is full.
  bool Push(QueueItem item);
  /// Get the front of item which in processed state.
  QueueItem FrontProcessed();
  /// Pop the front of item which in processed state.
  QueueItem PopProcessed();
  /// Pop the front of item which in pending state, the item
  /// will not be evicted at this moment, its state turn to
  /// processed.
  QueueItem PopPending();
  /// PopPending with timeout in microseconds.
  QueueItem PopPendingBlockTimeout(uint64_t timeout_us);
  /// Return the last item in pending state.
  QueueItem BackPending();
  /// Return the last item in processed state.
  QueueItem LastProcessed();

  bool IsPendingEmpty();
  inline bool IsPendingFull(uint64_t data_size = 0);
  /// Return the size in bytes of all items in queue.
  inline uint64_t QueueSize() { return data_size_; }
  /// Return the max data size in bytes of the queue.
  inline uint64_t QueueMaxSize() { return max_data_size_; }
  inline uint64_t GetLastPopMsgId() { return last_pop_msg_id_; }
  inline uint64_t GetLastPopSeqId() { return last_pop_seq_id_; }
  inline ActorID GetActorID() { return actor_id_; }
  inline ActorID GetPeerActorID() { return peer_actor_id_; }
  inline ObjectID GetQueueID() { return queue_id_; }
  inline uint64_t GetReconnectedCount() { return reconnected_count_; }
  /// Return item count in pending state.
  size_t PendingCount();
  /// Return item count in processed state.
  size_t ProcessedCount();

  inline void SetCollocateObjectId(const ObjectID &object_id) { object_id_ = object_id; }
  inline bool IsCollocate() { return collocate_; }

  void ReleasePlasmaObject(const ObjectID &object_id);

  inline bool IsCyclic() { return cyclic_; }

  inline void SetPullStatus(StreamingQueueStatus status) { pull_status_ = status; }
  StreamingQueueStatus GetPullStatus() { return pull_status_; }

  inline virtual void IncreaseDataSize(const QueueItem &item);
  inline virtual void DecreaseDataSize(const QueueItem &item);

 protected:
  // TODO: lock-free list
  // Using list to implement: Push/Pop/Traverse
  std::list<QueueItem> buffer_queue_;
  std::list<QueueItem>::iterator watershed_iter_;

  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  // max data size in bytes
  uint64_t max_data_size_;
  // The current data size occupied by queue, in bytes.
  uint64_t data_size_;

  std::recursive_mutex mutex_;
  std::condition_variable_any readable_cv_;
  std::condition_variable_any writeable_cv_;
  uint64_t last_pop_msg_id_;
  uint64_t last_pop_seq_id_;

  uint64_t reconnected_count_;
  // The plasma store object id used by collocate buffer. We create object_id_ by
  // `ObjectID::FromRandom()`, instead of using the queue id, because the object id may be
  // changed when FO, but queue id is always the same.
  ObjectID object_id_;
  bool collocate_;
  std::shared_ptr<plasma::PlasmaClient> plasma_client_;

  bool cyclic_;
  StreamingQueueStatus pull_status_;
};

const uint64_t QUEUE_INITIAL_SEQ_ID = 1;

/// Used for trouble shooting debug infos.
struct WriterQueueProfilingInfo {
  uint64_t seq_id;
  uint64_t last_sent_message_id;
  uint64_t last_sent_seq_id;
  uint64_t min_consumed_msg_id;
  uint64_t eviction_limit;
  uint64_t ignored_eviction_limit;
  uint64_t reconnected_count;
  bool collocate;
  bool initialized;
  uint64_t push_elasticbuffer_count;
  uint64_t clear_elasticbuffer_count;
  uint64_t push_elasticbuffer_size;
  StreamingQueueStatus pull_status;
};

/// Queue in upstream.
class WriterQueue : public Queue {
 public:
  /// \param queue_id, the unique ObjectID to identify a queue
  /// \param actor_id, the actor id of upstream worker
  /// \param peer_actor_id, the actor id of downstream worker
  /// \param size, max data size in bytes
  /// \param transport, transport
  /// \param esbuffer, backup data which is cousumed
  WriterQueue(const ObjectID &queue_id, const ActorID &actor_id,
              const ActorID &peer_actor_id, bool cyclic, uint64_t size,
              std::shared_ptr<Transport> transport,
              std::shared_ptr<ElasticBuffer<QueueItem>> esbuffer = nullptr)
      : Queue(actor_id, peer_actor_id, queue_id, size, transport, cyclic),
        seq_id_(QUEUE_INITIAL_SEQ_ID),
        eviction_limit_(QUEUE_INVALID_SEQ_ID),
        queue_limit_(QUEUE_INVALID_SEQ_ID),
        min_consumed_msg_id_(QUEUE_INVALID_SEQ_ID),
        min_consumed_bundle_id_(QUEUE_INVALID_SEQ_ID),
        peer_last_msg_id_(0),
        peer_last_seq_id_(QUEUE_INVALID_SEQ_ID),
        transport_(transport),
        is_resending_(false),
        is_upstream_first_pull_(true),
        evict_count_(0),
        evict_fail_count_(0),
        push_elasticbuffer_count_(0),
        clear_elasticbuffer_count_(0),
        push_elasticbuffer_size_(0),
        last_evict_fail_seq_id_(QUEUE_INVALID_SEQ_ID),
        last_sent_message_id_(QUEUE_INVALID_SEQ_ID),
        last_sent_seq_id_(QUEUE_INVALID_SEQ_ID),
        ignored_eviction_limit_(0),
        peer_hostname_(""),
        buffer_pool_(nullptr),
        buffer_pool_external_buffer_(nullptr),
        esbuffer_(esbuffer),
        initialized_(false),
        resend_id_(0),
        data_size_in_buffer_pool_(0) {
    if (esbuffer_) {
      esbuffer_->AddChannel(queue_id, 0);
    }
  }

  virtual ~WriterQueue();
  bool Push(QueueItem item);
  /// Push a continuous buffer into queue.
  /// NOTE: the buffer should be copied.
  virtual StreamingStatus Push(uint8_t *meta_data, uint32_t meta_data_size, uint8_t *data,
                               uint32_t data_size, uint64_t timestamp,
                               uint64_t msg_id_start, uint64_t msg_id_end);

  virtual int Evict(uint32_t data_size);

  /// Callback function, will be called when downstream queue notifies
  /// it has consumed some items.
  /// NOTE: this callback function is called in queue thread.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);

  /// Callback function, will be called when downstream queue wants to
  /// pull some items from our upstream queue.
  /// NOTE: this callback function is called in queue thread.
  virtual void OnPull(std::shared_ptr<PullRequestMessage> pull_msg,
                      boost::asio::io_service &service,
                      std::function<void(std::unique_ptr<PullResponseMessage>)> callback);

  /// Send items through direct call.
  void Send();
  /// Resend items whose msg_id is larger than start_msg_id
  void Resend(uint64_t start_msg_id, boost::asio::io_service &service);

  /// Just for tests.
  uint64_t MockSend();

  /// if there is no elasticbuffer remove item from buffer queue directly
  int EvictFromQueue(uint64_t target, uint32_t required_data_size);
  void SetQueueLimit(uint64_t queue_limit);
  /// Without ElasticBuffer queue_limit_ = eviction_limit_ = eviction_limit,
  /// with ElasictBuffer, eviction_limit_ = eviction_limit, queue_limit_ =
  /// max(eviction_limit_, queue_limit_).
  void SetQueueEvictionLimit(uint64_t eviction_limit);
  /// Remove all data in queue and elasticbuffer
  int EvictLessThan(uint64_t target);
  /// Try to clear old data in elasticbuffer because data is not cleared when set
  /// eviction_limit_.
  void TryClearElasticBuffer();

  inline uint64_t EvictionLimit() { return eviction_limit_; }
  inline uint64_t GetMinConsumedMsgID() { return min_consumed_msg_id_; }
  inline uint64_t GetMinConsumedBundleID() { return min_consumed_bundle_id_; }
  inline void SetPeerLastIds(uint64_t msg_id, uint64_t seq_id) {
    peer_last_msg_id_ = msg_id;
    peer_last_seq_id_ = seq_id;
  }
  inline uint64_t GetPeerLastSeqId() { return peer_last_seq_id_; }
  inline uint64_t GetCurrentSeqId() { return seq_id_; }
  inline uint64_t GetLastSentMsgId() { return last_sent_message_id_; }
  inline uint64_t GetLastSentSeqId() { return last_sent_seq_id_; }
  inline uint64_t GetIgnoredEvictLimit() { return ignored_eviction_limit_; }
  inline uint64_t GetEvictCount() { return evict_count_; }
  inline uint64_t GetEvictFailCount() { return evict_fail_count_; }
  inline uint64_t GetPushElasticBufferCount() { return push_elasticbuffer_count_; }
  inline uint64_t GetClearElasticBufferCount() { return clear_elasticbuffer_count_; }
  inline uint64_t GetPushElasticBufferSize() { return push_elasticbuffer_size_; }
  inline std::shared_ptr<BufferPool> GetBufferPool() { return buffer_pool_; }
  void SetPeerHostname(std::string hostname);
  inline uint64_t GetBufferOffset(std::shared_ptr<LocalMemoryBuffer> buffer) {
    return buffer == nullptr ? 0 : buffer->Data() - buffer_pool_external_buffer_->Data();
  }
  inline size_t GetBufferDataSize(std::shared_ptr<LocalMemoryBuffer> buffer) {
    return buffer == nullptr ? 0 : buffer->Size();
  }
  inline std::string GetPeerHostName() { return peer_hostname_; }

  /// Set plasma_client and new a PlasmaBuffer(peer host is remote) or a BufferPool(peer
  /// host is local).
  void CreateInternalBuffer(const std::shared_ptr<plasma::PlasmaClient> &plasma_client,
                            int32_t buffer_pool_min_size);

  inline void SetInitialized(bool initialized) { initialized_ = initialized; }
  inline bool IsInitialized() { return initialized_; }
  void SendItem(
      QueueItem &item, bool in_elastic_buffer, bool resend = false,
      uint64_t resend_id = 0,
      queue::flatbuf::ResendReason reason = queue::flatbuf::ResendReason::PULLED);
  void GetProfilingInfo(WriterQueueProfilingInfo &debug_info);
  double GetUsageRate();
  inline void IncreaseDataSize(const QueueItem &item) override;
  inline void DecreaseDataSize(const QueueItem &item) override;

 protected:
  inline bool IsElasticBufferEnabled() { return esbuffer_ != nullptr; }
  StreamingStatus PushInternal(uint8_t *meta_data, uint32_t meta_data_size, uint8_t *data,
                               uint32_t data_size, uint64_t timestamp,
                               uint64_t msg_id_start, uint64_t msg_id_end);
  inline uint32_t MaxEvictSize(uint32_t required) { return 10 * required; }
  void FindItem(uint64_t target, std::function<void()> large, std::function<void()> small,
                std::function<void(std::list<QueueItem>::iterator, uint64_t, uint64_t,
                                   std::list<QueueItem>::iterator)>
                    succeedCallBack);
  int Iterator(std::list<QueueItem>::iterator queue_start_iter,
               std::list<QueueItem>::iterator esbuffer_start_iter, uint64_t first_seq_id,
               u_int64_t last_seq_id, std::function<void(QueueItem &, bool)> handler);

  void OnPullInternal(uint64_t msg_id, ActorID peer_actor_id, ActorID actor_id,
                      ObjectID queue_id, boost::asio::io_service &service,
                      std::function<void(std::unique_ptr<PullResponseMessage>)> callback);
  void WaitForResending(std::unique_lock<std::recursive_mutex> &lock);
  /// Called when user pushs item into queue. The count of items
  /// can be put into elasticbuffer, determined by min_consumed_msg_id_.
  virtual int TryEvictItems(uint32_t required_data_size);

 private:
  int ResendItems(std::list<QueueItem>::iterator queue_start_iter, uint64_t first_seq_id,
                  uint64_t last_seq_id,
                  std::list<QueueItem>::iterator esbuffer_start_iter);
  inline bool IsLargeItem(uint8_t *data) {
    return buffer_pool_ != nullptr ? buffer_pool_->IsDisposableBuffer(data) : false;
  }

 private:
  uint64_t seq_id_;
  /*
  For the writer queue has three parameters, eviction_limit_, queue_limit_,
  min_consumed_id. There are two conditions, with ElasticBuffer and without ElasticBuffer.

  1. Without ElasticBuffer Condition: queue_limit_ = eviction_limit_, data less than these
  parameters need to be evicted from queue.
  /// ----------------------------------------------------------------------------
  /// |        evictlimit/queuelimit    minconsumedid     water shed              |
  /// |---delete data---|---consumed data---|---sending data---|---pending data---|
  /// ----------------------------------------------------------------------------

  2. With ElasticBuffer Condition: queue_limit_ means data's msgid less than it need to be
  removed from queue to ElasticBuffer. eviction_limit_ means data's msgid less than it
  need to be deleted completely.
  --------------------------------------------------------------------------------------
  |          evict limit       queue limit     minconsumed id     water shed            |
  |--delete data--|--ElasticBuffer--|--consumed data--|--sending data--|--pending data--|
  --------------------------------------------------------------------------------------
  PS. consumed data and sending data in writerqueue are called "processed data"

  */
  /// Real remove all the data less than eviction_limit_.
  uint64_t eviction_limit_;
  /// Remove the data from queue to put the new data in.
  uint64_t queue_limit_;
  /// The msgid of data which have been received.
  uint64_t min_consumed_msg_id_;
  uint64_t min_consumed_bundle_id_;
  uint64_t peer_last_msg_id_;
  uint64_t peer_last_seq_id_;
  std::shared_ptr<Transport> transport_;

  // metrics
  uint64_t evict_count_;
  uint64_t evict_fail_count_;
  uint64_t push_elasticbuffer_count_;
  uint64_t clear_elasticbuffer_count_;
  uint64_t push_elasticbuffer_size_;

  // For logs
  uint64_t last_evict_fail_seq_id_;

  uint64_t last_sent_message_id_;
  uint64_t last_sent_seq_id_;

  uint64_t ignored_eviction_limit_;

  std::shared_ptr<BufferPool> buffer_pool_;
  std::string peer_hostname_;
  std::shared_ptr<ray::Buffer> buffer_pool_external_buffer_;

  /// Placeholder buffer used to padding directcall arguments.
  static const std::shared_ptr<LocalMemoryBuffer> PLACE_HOLDER_BUFFER;
  static const uint32_t PLACE_HOLDER;
  bool initialized_;

  // The current data size in buffer pool occupied by queue, in bytes.
  uint64_t data_size_in_buffer_pool_;

 protected:
  std::shared_ptr<ElasticBuffer<QueueItem>> esbuffer_;
  std::list<QueueItem> es_meta_queue_;
  bool is_upstream_first_pull_;
  bool is_resending_;
  uint64_t resend_id_;
};

/// WriterQueue for cyclic channel.
class CyclicWriterQueue : public WriterQueue {
 public:
  CyclicWriterQueue(const ObjectID &queue_id, const ActorID &actor_id,
                    const ActorID &peer_actor_id, bool cyclic, uint64_t size,
                    std::shared_ptr<Transport> transport,
                    std::shared_ptr<ElasticBuffer<QueueItem>> esbuffer = nullptr)
      : WriterQueue(queue_id, actor_id, peer_actor_id, cyclic, size, transport, nullptr) {
    esbuffer_ = esbuffer;
    if (esbuffer) {
      /* Cyclic channel esbuffer can use infinite disk space. */
      esbuffer->AddChannel(queue_id, 0, ElasticBufferChannelConfig{-1});
    }
  }

  virtual StreamingStatus Push(uint8_t *meta_data, uint32_t meta_data_size, uint8_t *data,
                               uint32_t data_size, uint64_t timestamp,
                               uint64_t msg_id_start, uint64_t msg_id_end) override;
  virtual void OnPull(
      std::shared_ptr<PullRequestMessage> pull_msg, boost::asio::io_service &service,
      std::function<void(std::unique_ptr<PullResponseMessage>)> callback) override;

  int GetItemsLargeThan(uint64_t msg_id, std::vector<QueueItem> &vec);

 protected:
  virtual int TryEvictItems(uint32_t required_data_size) override;
};

/// Used for trouble shooting debug infos.
struct ReaderQueueProfilingInfo {
  uint64_t seq_id;
  uint64_t last_pop_msg_id;
  uint64_t last_pop_seq_id;
  uint64_t reconnected_count;
  uint64_t notify_count;
  uint64_t pending_count;
  StreamingQueueStatus pull_status;
};

/// Queue in downstream.
class ReaderQueue : public Queue {
 public:
  /// \param queue_id, the unique ObjectID to identify a queue
  /// \param actor_id, the actor id of downstream worker
  /// \param peer_actor_id, the actor id of upstream worker
  /// \param transport, transport
  /// NOTE: we do not restrict queue size of ReaderQueue
  ReaderQueue(const ObjectID &queue_id, const ActorID &actor_id,
              const ActorID &peer_actor_id, bool cyclic,
              std::shared_ptr<Transport> transport)
      : Queue(actor_id, peer_actor_id, queue_id, std::numeric_limits<uint64_t>::max(),
              transport, cyclic),
        last_recv_seq_id_(QUEUE_INVALID_SEQ_ID),
        last_recv_msg_id_(QUEUE_INVALID_SEQ_ID),
        transport_(transport),
        last_item_latency_(0),
        last_item_transfer_latency_(0),
        last_item_latency_large_than_10ms_(0),
        last_item_latency_large_than_5ms_(0),
        notify_count_(0),
        buffer_pool_external_buffer_(nullptr),
        last_use_collocate_object_id_(ObjectID::Nil()),
        last_invalid_collocate_object_id_(ObjectID::Nil()),
        start_msg_id_(QUEUE_INVALID_SEQ_ID),
        first_received_msg_id_(QUEUE_INVALID_SEQ_ID),
        dropped_resend_id_(-1) {}

  ~ReaderQueue();
  /// Delete processed items whose msg id <= msg_id,
  /// then notify upstream queue.
  /// \param msg_id, the last messag id of bundle
  /// \param bundle_id, the raw bundle id of channel
  void OnConsumed(uint64_t msg_id, uint64_t bundle_id);
  void NotifyStartLogging(uint64_t barrier_id, uint64_t msg_id, uint64_t seq_id);
  void OnData(QueueItem &item);
  void OnResendData(QueueItem &item, uint64_t resend_id,
                    queue::flatbuf::ResendReason reason);

  inline uint64_t GetLastRecvSeqId() { return last_recv_seq_id_; }
  inline uint64_t GetLastRecvMsgId() { return last_recv_msg_id_; }
  /// From the timestamp when writer push data into queue to
  /// the timestamp when reader receive the item and add into internal buffer.
  inline uint64_t GetLastItemLatency() { return last_item_latency_; }
  /// From the timestamp when the item begin to send out using direct call to
  /// the timestamp when reader receive the item.
  inline uint64_t GetLastItemTransferLatency() { return last_item_transfer_latency_; }
  inline uint64_t GetLastItemLatencyLargeThan10ms() {
    return last_item_latency_large_than_10ms_;
  }
  inline uint64_t GetLastItemLatencyLargeThan5ms() {
    return last_item_latency_large_than_5ms_;
  }
  inline uint64_t GetNotifyCount() { return notify_count_; }
  inline void IncReconnectedCount() { reconnected_count_++; }
  inline bool InternalBufferInitialed(const ObjectID &object_id) {
    return object_id_ == object_id;
  }
  bool CreateInternalBuffer(std::shared_ptr<plasma::PlasmaClient> &plasma_client,
                            const ObjectID &object_id);
  inline uint8_t *GetBufferPtr(uint64_t offset) {
    return buffer_pool_external_buffer_->Data() + offset;
  }
  inline void SetStartMsgId(uint64_t start) { start_msg_id_ = start; }
  void GetProfilingInfo(ReaderQueueProfilingInfo &debug_info);

 private:
  void Notify(uint64_t seq_id, uint64_t bundle_id);

 private:
  uint64_t last_recv_seq_id_;
  uint64_t last_recv_msg_id_;
  uint64_t expect_seq_id_;
  std::shared_ptr<Transport> transport_;
  uint64_t last_item_latency_;
  uint64_t last_item_transfer_latency_;
  uint64_t last_item_latency_large_than_10ms_;
  uint64_t last_item_latency_large_than_5ms_;
  uint64_t notify_count_;
  std::shared_ptr<ray::Buffer> buffer_pool_external_buffer_;

  ObjectID last_use_collocate_object_id_;
  ObjectID last_invalid_collocate_object_id_;

  /// The message id upstream queue should send from, specified by DataReader when
  /// initialized.
  uint64_t start_msg_id_;
  /// The first message id received by the current queue.
  uint64_t first_received_msg_id_;
  /// The current resend sequence id which should be dropped.
  uint64_t dropped_resend_id_;
};

}  // namespace streaming
}  // namespace ray
#endif
