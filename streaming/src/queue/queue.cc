#include "queue/queue.h"

#include <chrono>
#include <thread>

#include "config.h"
#include "reliability/persistence.h"
#include "util/utility.h"

namespace ray {
namespace streaming {

const uint32_t WriterQueue::PLACE_HOLDER = 0xBABABABA;
const std::shared_ptr<LocalMemoryBuffer> WriterQueue::PLACE_HOLDER_BUFFER =
    std::make_shared<LocalMemoryBuffer>((uint8_t *)(&WriterQueue::PLACE_HOLDER),
                                        sizeof(uint32_t), true, true);

bool Queue::Push(QueueItem item) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_LOG(DEBUG) << "Queue push item " << item.SeqId() << " item data size is "
                       << (uint64_t)item.DataSize() << " data_size_ is " << data_size_;
  buffer_queue_.push_back(item);
  IncreaseDataSize(item);
  readable_cv_.notify_one();
  return true;
}

QueueItem Queue::FrontProcessed() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_CHECK(!buffer_queue_.empty()) << "WriterQueue Pop fail";

  if (watershed_iter_ == buffer_queue_.begin()) {
    return QueueItem::InvalidQueueItem();
  }

  QueueItem item = buffer_queue_.front();
  return item;
}

QueueItem Queue::PopProcessed() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_CHECK(!buffer_queue_.empty()) << "WriterQueue Pop fail";

  if (watershed_iter_ == buffer_queue_.begin()) {
    return QueueItem::InvalidQueueItem();
  }

  QueueItem item = buffer_queue_.front();
  buffer_queue_.pop_front();
  DecreaseDataSize(item);
  return item;
}

QueueItem Queue::PopPending() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  if (std::next(watershed_iter_) == buffer_queue_.end()) {
    return QueueItem(QUEUE_INVALID_SEQ_ID, nullptr, nullptr, 0, 0, 0, 0, 0);
  } else {
    auto it = std::next(watershed_iter_);
    QueueItem item = *it;
    buffer_queue_.splice(watershed_iter_, buffer_queue_, it, std::next(it));
    last_pop_msg_id_ = item.MsgIdEnd();
    last_pop_seq_id_ = item.SeqId();
    return item;
  }
}

QueueItem Queue::PopPendingBlockTimeout(uint64_t timeout_us) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  std::chrono::system_clock::time_point point =
      std::chrono::system_clock::now() + std::chrono::microseconds(timeout_us);
  if (readable_cv_.wait_until(lock, point, [this] {
        return std::next(watershed_iter_) != buffer_queue_.end();
      })) {
    auto it = std::next(watershed_iter_);
    QueueItem item = *it;
    buffer_queue_.splice(watershed_iter_, buffer_queue_, it, std::next(it));
    last_pop_msg_id_ = item.MsgIdEnd();
    last_pop_seq_id_ = item.SeqId();
    return item;
  } else {
    /// TODO:
    return QueueItem(QUEUE_INVALID_SEQ_ID, nullptr, nullptr, 0, 0, 0, 0, 0);
  }
}

QueueItem Queue::BackPending() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  if (std::next(watershed_iter_) == buffer_queue_.end()) {
    /// TODO
    return QueueItem(QUEUE_INVALID_SEQ_ID, nullptr, nullptr, 0, 0, 0, 0, 0);
  }
  return buffer_queue_.back();
}

bool Queue::IsPendingEmpty() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  return std::next(watershed_iter_) == buffer_queue_.end();
}

bool Queue::IsPendingFull(uint64_t data_size) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  return max_data_size_ < data_size + data_size_;
}

size_t Queue::ProcessedCount() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  return std::distance(buffer_queue_.begin(), watershed_iter_);
}

size_t Queue::PendingCount() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  return std::distance(std::next(watershed_iter_), buffer_queue_.end());
}

void Queue::ReleasePlasmaObject(const ObjectID &object_id) {
  auto status = plasma_client_->Release(object_id);
  STREAMING_LOG(INFO) << "Release plasma object: " << status;
  status = plasma_client_->Delete(object_id);
  STREAMING_LOG(INFO) << "Delete plasma object: " << status;
}

QueueItem Queue::LastProcessed() {
  if (watershed_iter_ == buffer_queue_.begin()) {
    return QueueItem::InvalidQueueItem();
  }

  auto target = std::prev(watershed_iter_);
  return *target;
}

void Queue::IncreaseDataSize(const QueueItem &item) { data_size_ += item.DataSize(); }

void Queue::DecreaseDataSize(const QueueItem &item) { data_size_ -= item.DataSize(); }

bool WriterQueue::Push(QueueItem item) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_LOG(DEBUG) << "WriterQueue push item " << item.SeqId()
                       << " item data size is " << (uint64_t)item.DataSize()
                       << " data_size_ is " << data_size_;
  buffer_queue_.push_back(item);
  IncreaseDataSize(item);
  readable_cv_.notify_one();
  return true;
}

StreamingStatus WriterQueue::Push(uint8_t *meta_data, uint32_t meta_data_size,
                                  uint8_t *data, uint32_t data_size, uint64_t timestamp,
                                  uint64_t msg_id_start, uint64_t msg_id_end) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  WaitForResending(lock);
  RAY_IGNORE_EXPR(TryEvictItems(MaxEvictSize(data_size + meta_data_size)));
  return PushInternal(meta_data, meta_data_size, data, data_size, timestamp, msg_id_start,
                      msg_id_end);
}

StreamingStatus WriterQueue::PushInternal(uint8_t *meta_data, uint32_t meta_data_size,
                                          uint8_t *data, uint32_t data_size,
                                          uint64_t timestamp, uint64_t msg_id_start,
                                          uint64_t msg_id_end) {
  uint32_t required_data_size = data_size + meta_data_size;
  if (IsPendingFull(required_data_size) && !IsLargeItem(data)) {
    if (!IsElasticBufferEnabled() || esbuffer_->IsFull(queue_id_)) {
      return StreamingStatus::FullChannel;
    } else {
      /// This status means that when trying to put data into queue is failed but
      /// ElasticBuffer still has space reamining. There is no new consumed msg and queue
      /// is filled with pending and processed data cause this condition.
      return StreamingStatus::ElasticBufferRemain;
    }
  }

  STREAMING_LOG(DEBUG) << "WriterQueue::Push metasize: " << meta_data_size
                       << " datasize: " << data_size;

  QueueItem item(seq_id_, meta_data, meta_data_size, data, data_size, 0, timestamp,
                 msg_id_start, msg_id_end);
  Push(item);
  seq_id_++;
  Send();

  // /// Whenever large item has been pushed into queue, evict all items in queue.
  // if (IsLargeItem(data)) {
  //   EvictFromQueue(QUEUE_INVALID_SEQ_ID, std::numeric_limits<uint32_t>::max());
  // }
  return StreamingStatus::OK;
}

void WriterQueue::Send() {
  while (!IsPendingEmpty()) {
    QueueItem item = PopPending();
    SendItem(item, false, false, 0);
  }
}

void WriterQueue::Resend(uint64_t start_msg_id, boost::asio::io_service &service) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  WaitForResending(lock);
  reconnected_count_++;
  FindItem(start_msg_id,
           [start_msg_id]() {
             STREAMING_LOG(WARNING) << "Resend start_msg_id too large: " << start_msg_id;
           },
           [start_msg_id]() {
             STREAMING_LOG(WARNING) << "Resend start_msg_id too small: " << start_msg_id;
           },
           [this, &service](std::list<QueueItem>::iterator queue_target,
                            uint64_t first_seq_id, uint64_t last_seq_id,
                            std::list<QueueItem>::iterator esbuffer_target) {
             is_resending_ = true;
             service.post(std::bind(&WriterQueue::ResendItems, this, queue_target,
                                    first_seq_id, last_seq_id, esbuffer_target));
           });
}

int WriterQueue::TryEvictItems(uint32_t max_evict_size) {
  evict_count_++;
  QueueItem item = FrontProcessed();
  STREAMING_LOG(DEBUG) << "WriterQueue TryEvictItems queue_id: " << queue_id_
                       << " first_item: (" << item.MsgIdStart() << "," << item.MsgIdEnd()
                       << ")"
                       << " min_consumed_msg_id_: " << min_consumed_msg_id_
                       << " queue_limit_: " << queue_limit_
                       << " eviction_limit_: " << eviction_limit_
                       << " max_data_size_: " << max_data_size_
                       << " data_size_: " << data_size_;

  if (min_consumed_msg_id_ == QUEUE_INVALID_SEQ_ID ||
      min_consumed_msg_id_ < item.MsgIdEnd()) {
    STREAMING_LOG(DEBUG) << "Fail in try to evict min_consumed_id is "
                         << min_consumed_msg_id_ << " first item msgend is "
                         << item.MsgIdEnd();
    evict_fail_count_++;
    return 0;
  }

  if (IsElasticBufferEnabled()) {
    TryClearElasticBuffer();
  }

  int count = 0;
  if (queue_limit_ == QUEUE_INVALID_SEQ_ID || queue_limit_ < item.MsgIdEnd()) {
    evict_fail_count_++;
    return 0;
  }
  uint64_t evict_target_msg_id = std::min(min_consumed_msg_id_, queue_limit_);
  count = EvictFromQueue(evict_target_msg_id, max_evict_size);

  STREAMING_LOG(DEBUG) << count << " items move to esbuffer " << GetQueueID()
                       << " current item: (" << item.MsgIdStart() << ","
                       << item.MsgIdEnd() << ")";
  return count;
}

void WriterQueue::TryClearElasticBuffer() {
  if (eviction_limit_ != QUEUE_INVALID_SEQ_ID && es_meta_queue_.size() > 0 &&
      es_meta_queue_.begin()->MsgIdEnd() <= eviction_limit_) {
    EvictLessThan(eviction_limit_);
  }
}

int WriterQueue::EvictFromQueue(uint64_t target, uint32_t max_evict_size) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_CHECK(!buffer_queue_.empty()) << "WriterQueue Pop fail";
  int count = 0;
  uint32_t released_data_size = 0;
  for (auto it = buffer_queue_.begin(); it != watershed_iter_;) {
    QueueItem &item = *it;
    if (item.MsgIdEnd() > target || released_data_size >= max_evict_size) {
      break;
    }
    DecreaseDataSize(item);
    released_data_size += item.DataSize();
    if (IsElasticBufferEnabled() &&
        (item.MsgIdEnd() > eviction_limit_ || eviction_limit_ == QUEUE_INVALID_SEQ_ID)) {
      push_elasticbuffer_size_ += item.DataSize();
      if (!item.OwnsData()) {
        QueueItem temp_item(
            item.SeqId(), item.MetaBuffer()->Data(), item.MetaBuffer()->Size(),
            item.Buffer()->Data(), item.Buffer()->Size(), item.TimestampMessage(),
            item.TimestampItem(), item.MsgIdStart(), item.MsgIdEnd(), true);
        buffer_pool_->Release(item.Buffer()->Data(), item.Buffer()->Size());
        esbuffer_->Put(queue_id_, item.SeqId(), temp_item);
        es_meta_queue_.emplace_back(item.SeqId(), item.MsgIdStart(), item.MsgIdEnd());
      } else {
        esbuffer_->Put(queue_id_, item.SeqId(), item);
        es_meta_queue_.emplace_back(item.SeqId(), item.MsgIdStart(), item.MsgIdEnd());
      }
      push_elasticbuffer_count_++;
    } else {
      if (!item.OwnsData()) {
        /// MetaBuffer is not nullptr, means DataBuffer exists in bufferpool
        buffer_pool_->Release(item.Buffer()->Data(), item.Buffer()->Size());
      }
    }
    buffer_queue_.erase(it++);
    count++;
  }
  return count;
}

void WriterQueue::SetQueueLimit(uint64_t queue_limit) {
  if (queue_limit_ == QUEUE_INVALID_SEQ_ID) {
    queue_limit_ = queue_limit;
  } else {
    queue_limit_ = std::max(queue_limit_, queue_limit);
  }
}

void WriterQueue::SetQueueEvictionLimit(uint64_t eviction_limit) {
  if (eviction_limit_ == QUEUE_INVALID_SEQ_ID) {
    eviction_limit_ = eviction_limit;
  } else {
    if (eviction_limit_ > eviction_limit) {
      ignored_eviction_limit_ = eviction_limit;
      STREAMING_LOG(WARNING) << "Ignore a smaller limit: "
                             << "eviction_limit_: " << eviction_limit_
                             << " eviction_limit: " << eviction_limit;
    }
    eviction_limit_ = std::max(eviction_limit, eviction_limit_);
  }
  if (queue_limit_ == QUEUE_INVALID_SEQ_ID) {
    queue_limit_ = eviction_limit_;
  } else {
    queue_limit_ = std::max(queue_limit_, eviction_limit_);
  }
}

int WriterQueue::EvictLessThan(uint64_t target) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_CHECK(!buffer_queue_.empty()) << "WriterQueue Pop fail";

  uint32_t count = 0;
  for (auto it = es_meta_queue_.begin(); it != es_meta_queue_.end();) {
    QueueItem &item = *it;
    if (item.MsgIdEnd() <= target) {
      es_meta_queue_.erase(it++);
      count++;
    }
    if (IsElasticBufferEnabled() &&
        (it == es_meta_queue_.end() || it->MsgIdEnd() > target)) {
      esbuffer_->Clear(queue_id_, item.SeqId());
      clear_elasticbuffer_count_++;
      break;
    }
  }
  for (auto it = buffer_queue_.begin(); it != watershed_iter_;) {
    QueueItem &item = *it;
    if (item.MsgIdEnd() > target) {
      break;
    }
    DecreaseDataSize(item);
    if (!item.OwnsData()) {
      /// MetaBuffer is not nullptr, means DataBuffer exists in bufferpool
      buffer_pool_->Release(item.Buffer()->Data(), item.Buffer()->Size());
    }
    buffer_queue_.erase(it++);
    count++;
  }
  return count;
}

uint64_t WriterQueue::MockSend() {
  uint64_t count = 0;
  while (!IsPendingEmpty()) {
    QueueItem item = PopPending();
    count++;
  }

  return count;
}

void WriterQueue::SendItem(QueueItem &item, bool in_elastic_buffer, bool resend,
                           uint64_t resend_id,
                           queue::flatbuf::ResendReason resend_reason) {
  STREAMING_LOG(DEBUG) << "SendItem collocate_: " << collocate_
                       << " item.MetaBuffer: " << (item.MetaBuffer() != nullptr)
                       << " item.Buffer: " << (item.Buffer() != nullptr);
  STREAMING_LOG(DEBUG) << "[ES DEBUG] resend item msgstart: " << item.MsgIdStart()
                       << " msgend: " << item.MsgIdEnd();
  STREAMING_CHECK(transport_);
  if (collocate_ && !in_elastic_buffer) {
    STREAMING_CHECK(!object_id_.IsNil()) << queue_id_;
    LocalDataMessage msg(actor_id_, peer_actor_id_, queue_id_, object_id_, item,
                         GetBufferOffset(item.Buffer()), GetBufferDataSize(item.Buffer()),
                         resend, resend_id, resend_reason);
    auto item_meta = msg.GetMetaBytes();
    transport_->Send(
        {std::move(item_meta), item.MetaBuffer(), WriterQueue::PLACE_HOLDER_BUFFER});
  } else {
    DataMessage msg(actor_id_, peer_actor_id_, queue_id_, item, resend, resend_id,
                    resend_reason);
    STREAMING_CHECK(item.MetaBuffer());
    auto item_meta = msg.GetMetaBytes();
    /// For empty message, there is no data buffer in QueueItem. Our direct call
    /// entry point function has three parameters, so the placeholder buffer is
    /// used here to fill all the parameters.
    transport_->Send({std::move(item_meta),  /// item meta buffer
                      item.MetaBuffer(),     /// bundle meta buffer
                      item.Buffer()
                          ? item.Buffer()
                          : WriterQueue::PLACE_HOLDER_BUFFER});  /// bundle data buffer
  }
  last_sent_message_id_ = item.MsgIdEnd();
  last_sent_seq_id_ = item.SeqId();
}

void WriterQueue::OnNotify(std::shared_ptr<NotificationMessage> notify_msg) {
  min_consumed_msg_id_ = notify_msg->MsgId();
  min_consumed_bundle_id_ = notify_msg->BundleId();
  STREAMING_LOG(DEBUG) << "OnNotify target msg_id: " << min_consumed_msg_id_
                       << ", bundle id : " << min_consumed_bundle_id_;

  // TODO: Async notify user thread
}

int WriterQueue::ResendItems(std::list<QueueItem>::iterator queue_start_iter,
                             uint64_t first_seq_id, uint64_t last_seq_id,
                             std::list<QueueItem>::iterator esbuffer_start_iter) {
  STREAMING_LOG(INFO) << "ResendItems queue_id: " << queue_id_
                      << " resend_id_: " << resend_id_;
  int count = 0;
  Iterator(queue_start_iter, esbuffer_start_iter, first_seq_id, last_seq_id,
           [this, &count](QueueItem &item, bool in_elastic_buffer) {
             STREAMING_LOG(DEBUG)
                 << "ResendItem seq_id " << item.SeqId() << " message id: ("
                 << item.MsgIdStart() << "," << item.MsgIdEnd() << ")"
                 << " size " << item.DataSize() << ".";
             SendItem(item, in_elastic_buffer, true, resend_id_);
             count++;
           });
  resend_id_++;
  STREAMING_LOG(INFO) << "ResendItems done. queue_id: " << queue_id_
                      << " count: " << count;
  return 0;
}

int WriterQueue::Iterator(std::list<QueueItem>::iterator queue_start_iter,
                          std::list<QueueItem>::iterator esbuffer_start_iter,
                          uint64_t first_seq_id, uint64_t last_seq_id,
                          std::function<void(QueueItem &, bool)> handler) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  STREAMING_LOG(INFO) << "Begin Iterator first seqid: " << first_seq_id
                      << " lastseqid: " << last_seq_id;
  int count = 0;
  if (IsElasticBufferEnabled()) {
    esbuffer_->BeginRecovery();
    for (auto it = esbuffer_start_iter; it != es_meta_queue_.end(); it++) {
      if (it->SeqId() > last_seq_id) {
        break;
      }
      STREAMING_LOG(INFO) << "Iterator seq_id " << it->SeqId() << " message id: ("
                          << it->MsgIdStart() << "," << it->MsgIdEnd() << ")"
                          << " size " << it->DataSize() << " (from esbuffer).";
      std::shared_ptr<QueueItem> temp_item;
      if (esbuffer_ && esbuffer_->Find(queue_id_, it->SeqId(),
                                       temp_item)) {  /// esbuffer recovery succeed
        STREAMING_LOG(DEBUG) << "Recover from esbuffer size: " << temp_item->BufferSize()
                             << " meta size: " << temp_item->MetaBufferSize()
                             << " msgid: " << temp_item->MsgIdStart()
                             << " seqid: " << temp_item->SeqId();
        handler(*temp_item, true);
      } else {  /// esbuffer recovery failed
        STREAMING_LOG(WARNING) << "Recover from esbuffer failed, Seq_id: " << it->SeqId()
                               << " queue_id " << queue_id_;
      }
      count++;
    }
    esbuffer_->EndRecovery();
  }

  for (auto it = queue_start_iter; it != watershed_iter_; ++it) {
    if (it->SeqId() > last_seq_id) {
      break;
    }
    STREAMING_LOG(DEBUG) << "Iterator seq_id " << it->SeqId() << " message id: ("
                         << it->MsgIdStart() << "," << it->MsgIdEnd() << ")"
                         << " size " << it->DataSize() << ".";
    handler(*it, false);
    count++;
  }

  STREAMING_LOG(INFO) << "Iterator total count: " << count;
  is_resending_ = false;
  writeable_cv_.notify_one();
  return count;
}

void WriterQueue::FindItem(uint64_t target_msg_id, std::function<void()> large,
                           std::function<void()> small,
                           std::function<void(std::list<QueueItem>::iterator, uint64_t,
                                              uint64_t, std::list<QueueItem>::iterator)>
                               succeedCallBack) {
  auto last_one = std::prev(watershed_iter_);
  bool too_large =
      last_one != buffer_queue_.end() && last_one->MsgIdEnd() < target_msg_id;

  if (QUEUE_INITIAL_SEQ_ID == seq_id_ || too_large) {
    large();
    return;
  }

  // Find target in queue.
  auto begin = buffer_queue_.begin();
  uint64_t first_seq_id = begin->SeqId();
  uint64_t last_seq_id = begin->SeqId() + std::distance(begin, watershed_iter_) - 1;
  STREAMING_LOG(INFO) << "FindItem last_seq_id: " << last_seq_id
                      << " first_seq_id: " << first_seq_id;
  auto queue_target = std::find_if(
      begin, watershed_iter_,
      [&target_msg_id](QueueItem &item) { return item.InItem(target_msg_id); });
  if (queue_target != watershed_iter_) {
    succeedCallBack(queue_target, first_seq_id, last_seq_id, es_meta_queue_.end());
    return;
  }

  if (IsElasticBufferEnabled()) {
    // If target can not be found in queue, search the target in esbuffer.
    auto esbegin = es_meta_queue_.begin();
    uint64_t first_es_seq_id = es_meta_queue_.size() ? esbegin->SeqId() : 0;
    uint64_t last_es_seq_id =
        es_meta_queue_.size() ? (first_es_seq_id + es_meta_queue_.size() - 1) : 0;

    STREAMING_LOG(INFO) << "FindItem last_es_seq_id: " << last_es_seq_id
                        << " first_es_seq_id: " << first_es_seq_id;

    auto esbuffer_target = std::find_if(
        esbegin, es_meta_queue_.end(),
        [&target_msg_id](QueueItem &item) { return item.InItem(target_msg_id); });

    if (esbuffer_target != es_meta_queue_.end()) {
      succeedCallBack(buffer_queue_.begin(), first_es_seq_id, last_seq_id,
                      esbuffer_target);
      return;
    }
  }

  // Neither of queue and esbuffer contains the target, means the target is to small.
  small();
}

// TODO: What will happen if user push items while we are OnPull ?
// lock to sync OnPull and Push
void WriterQueue::OnPull(
    std::shared_ptr<PullRequestMessage> pull_msg, boost::asio::io_service &service,
    std::function<void(std::unique_ptr<PullResponseMessage>)> callback) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  WaitForResending(lock);
  STREAMING_CHECK(peer_actor_id_ == pull_msg->ActorId())
      << peer_actor_id_ << " " << pull_msg->ActorId();
  OnPullInternal(pull_msg->MsgId(), pull_msg->PeerActorId(), pull_msg->ActorId(),
                 pull_msg->QueueId(), service, callback);
}

void WriterQueue::OnPullInternal(
    uint64_t msg_id, ActorID peer_actor_id, ActorID actor_id, ObjectID queue_id,
    boost::asio::io_service &service,
    std::function<void(std::unique_ptr<PullResponseMessage>)> callback) {
  FindItem(msg_id,
           /// target_msg_id is too large.
           [&]() {
             STREAMING_LOG(WARNING)
                 << "No valid data to pull, the writer has not push data yet. ";
             auto response = std::unique_ptr<PullResponseMessage>(new PullResponseMessage(
                 peer_actor_id, actor_id, queue_id, QUEUE_INVALID_SEQ_ID,
                 QUEUE_INVALID_SEQ_ID, 0,
                 queue::flatbuf::StreamingQueueError::NO_VALID_DATA,
                 is_upstream_first_pull_));
             is_upstream_first_pull_ = false;
             SetPullStatus(StreamingQueueStatus::NoValidData);
             callback(std::move(response));
           },
           /// target_msg_id is too small.
           [&]() {
             STREAMING_LOG(WARNING) << "Data lost.";
             auto response = std::unique_ptr<PullResponseMessage>(new PullResponseMessage(
                 peer_actor_id, actor_id, queue_id, QUEUE_INVALID_SEQ_ID,
                 QUEUE_INVALID_SEQ_ID, 0, queue::flatbuf::StreamingQueueError::DATA_LOST,
                 is_upstream_first_pull_));
             SetPullStatus(StreamingQueueStatus::DataLost);
             callback(std::move(response));
           },
           /// target_msg_id found.
           [&](std::list<QueueItem>::iterator queue_target, uint64_t first_seq_id,
               uint64_t last_seq_id, std::list<QueueItem>::iterator esbuffer_target) {
             STREAMING_LOG(INFO) << "Found target msg id " << msg_id;
             is_resending_ = true;
             service.post(std::bind(&WriterQueue::ResendItems, this, queue_target,
                                    first_seq_id, last_seq_id, esbuffer_target));
             auto response = std::unique_ptr<PullResponseMessage>(new PullResponseMessage(
                 peer_actor_id, actor_id, queue_id, first_seq_id, msg_id,
                 last_seq_id - first_seq_id + 1, queue::flatbuf::StreamingQueueError::OK,
                 is_upstream_first_pull_));
             is_upstream_first_pull_ = false;
             SetPullStatus(StreamingQueueStatus::OK);
             callback(std::move(response));
           });
}

void WriterQueue::SetPeerHostname(std::string hostname) {
  peer_hostname_ = hostname;
  collocate_ = StreamingUtility::GetHostname() == peer_hostname_;
}

void WriterQueue::CreateInternalBuffer(
    const std::shared_ptr<plasma::PlasmaClient> &plasma_client,
    int32_t buffer_pool_min_size) {
  size_t real_buffer_pool_size = max_data_size_ * 2;
  if (collocate_ && plasma_client) {
    /// The upstream queue support for collocate is quite simple. Create a new object in
    /// plasma store with a random object id, create a mutable buffer in the object, and
    /// then put bufferpool in the buffer. We do the same thing no matter at first start
    /// or failover. So, there may be two or more objects coexists when failover, the
    /// downstream queue will release the old object when all items were consumed.
    ObjectID object_id = ObjectID::FromRandom();
    SetCollocateObjectId(object_id);
    STREAMING_LOG(WARNING)
        << "[WriterQueue] The queue is collocated, allocate bufferpool in plasma store: "
        << object_id << " real_buffer_pool_size: " << real_buffer_pool_size;
    plasma_client_ = plasma_client;
    bool has_object = false;
    auto status = plasma_client_->Contains(object_id, &has_object);
    STREAMING_CHECK(status.ok() && !has_object) << status << " " << has_object;
    std::shared_ptr<ray::Buffer> buffer;
    uint64_t retry_with_request_id = 0;
    status = plasma_client_->Create(object_id, ray::rpc::Address(), real_buffer_pool_size,
                                    nullptr, 0, &retry_with_request_id, &buffer);
    STREAMING_CHECK(status.ok()) << "Create plasma object status: " << status;
    status = plasma_client_->Seal(object_id);
    STREAMING_CHECK(status.ok()) << "Seal plasma object status: " << status;
    buffer_pool_external_buffer_ = std::make_shared<PlasmaBuffer>(buffer, nullptr);
    buffer_pool_ = std::make_shared<BufferPool>(buffer_pool_external_buffer_->Data(),
                                                real_buffer_pool_size);
  } else {
    buffer_pool_ =
        std::make_shared<BufferPool>(real_buffer_pool_size, buffer_pool_min_size);
  }
  /// NOTE:(wanxing.wwx) Set buffer pool oom limit size to queue size to ensure arbitrary
  /// buffers allocated in buffer pool can be push into the queue.
  /// TODO: fix kMessageBundleHeaderSize
  buffer_pool_->SetOOMLimit(max_data_size_ - kMessageBundleHeaderSize);
}

WriterQueue::~WriterQueue() {
  if (IsElasticBufferEnabled()) {
    esbuffer_->RemoveChannel(queue_id_);
  }
  if (collocate_) {
    STREAMING_LOG(WARNING)
        << "[WriterQueue] The queue is collocated, release object in plasma store: "
        << object_id_;
    ReleasePlasmaObject(object_id_);
    plasma_client_ = nullptr;
  }
}

void WriterQueue::WaitForResending(std::unique_lock<std::recursive_mutex> &lock) {
  while (is_resending_) {
    writeable_cv_.wait_for(lock, std::chrono::milliseconds(100),
                           [this]() { return !is_resending_; });
  }
}

int WriterQueue::Evict(uint32_t data_size) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  WaitForResending(lock);
  return TryEvictItems(data_size);
}

void WriterQueue::GetProfilingInfo(WriterQueueProfilingInfo &debug_info) {
  debug_info.seq_id = GetCurrentSeqId();
  debug_info.last_sent_message_id = GetLastSentMsgId();
  debug_info.last_sent_seq_id = GetLastSentSeqId();
  debug_info.min_consumed_msg_id = GetMinConsumedMsgID();
  debug_info.eviction_limit = EvictionLimit();
  debug_info.ignored_eviction_limit = GetIgnoredEvictLimit();
  debug_info.reconnected_count = GetReconnectedCount();
  debug_info.collocate = IsCollocate();
  debug_info.initialized = IsInitialized();
  debug_info.push_elasticbuffer_count = GetPushElasticBufferCount();
  debug_info.clear_elasticbuffer_count = GetClearElasticBufferCount();
  debug_info.push_elasticbuffer_size = GetPushElasticBufferSize();
  debug_info.pull_status = GetPullStatus();
}

void WriterQueue::IncreaseDataSize(const QueueItem &item) {
  Queue::IncreaseDataSize(item);
  /// NOTE: buffer owned by item and large buffer in bufferpool can not be counted.
  if (!item.OwnsData() && buffer_pool_ &&
      !buffer_pool_->IsLargeBuffer(reinterpret_cast<uint64_t>(item.Buffer()->Data()))) {
    data_size_in_buffer_pool_ += item.BufferSize();
  }
  STREAMING_LOG(DEBUG) << "data_size_: " << data_size_
                       << " data_size_in_buffer_pool_: " << data_size_in_buffer_pool_;
}

void WriterQueue::DecreaseDataSize(const QueueItem &item) {
  Queue::DecreaseDataSize(item);
  if (!item.OwnsData()) {
    data_size_in_buffer_pool_ -= item.BufferSize();
  }
  STREAMING_CHECK(data_size_ >= 0 && data_size_in_buffer_pool_ >= 0)
      << "Invalid data size: " << data_size_ << " " << data_size_in_buffer_pool_;
}

double WriterQueue::GetUsageRate() {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  uint64_t buffer_pool_size = buffer_pool_->pool_size();
  /// NOTE: `buffer_pool_->used()` may be larger than expected, because `MarkUsed` may be
  /// called when user thread write message to ringbuffer, but it doesn't matter.
  uint64_t buffer_pool_used = buffer_pool_->used();
  /// NOTE: Exclude the data size which has been written to channel.
  int64_t pool_size_exclude_channel = buffer_pool_size - data_size_in_buffer_pool_;
  int64_t used_size_exclude_channel = buffer_pool_used - data_size_in_buffer_pool_;
  STREAMING_CHECK(pool_size_exclude_channel >= used_size_exclude_channel &&
                  used_size_exclude_channel >= 0)
      << data_size_in_buffer_pool_ << " " << buffer_pool_size << " " << buffer_pool_used
      << " " << pool_size_exclude_channel << " " << used_size_exclude_channel;

  double buffer_pool_usage_rate = 0;
  if (0 == pool_size_exclude_channel) {
    buffer_pool_usage_rate = 1;
  } else {
    buffer_pool_usage_rate = used_size_exclude_channel * 1.0 / pool_size_exclude_channel;
  }

  return buffer_pool_usage_rate;
}

StreamingStatus CyclicWriterQueue::Push(uint8_t *meta_data, uint32_t meta_data_size,
                                        uint8_t *data, uint32_t data_size,
                                        uint64_t timestamp, uint64_t msg_id_start,
                                        uint64_t msg_id_end) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  WaitForResending(lock);
  uint32_t max_evict_size = MaxEvictSize(data_size + meta_data_size);
  // First evict items as normal queue, evication limited by limit-ids
  // (min_consumed_msg_id_/eviction_limit_/queue_limit_)
  RAY_IGNORE_EXPR(WriterQueue::TryEvictItems(max_evict_size));
  StreamingStatus status = PushInternal(meta_data, meta_data_size, data, data_size,
                                        timestamp, msg_id_start, msg_id_end);
  if (StreamingStatus::ElasticBufferRemain == status) {
    // If the queue is filled with unconsumed items, call CyclicWriterQueue::TryEvictItems
    // to evict unconsumed items to esbuffer.
    RAY_IGNORE_EXPR(TryEvictItems(max_evict_size));
    status = PushInternal(meta_data, meta_data_size, data, data_size, timestamp,
                          msg_id_start, msg_id_end);
  }
  if (StreamingStatus::OK != status) {
    STREAMING_LOG(DEBUG) << "PushInternal unexpected return value: " << status
                         << ", elastic buffer may not be turned on.";
  }
  return status;
}

void CyclicWriterQueue::OnPull(
    std::shared_ptr<PullRequestMessage> pull_msg, boost::asio::io_service &service,
    std::function<void(std::unique_ptr<PullResponseMessage>)> callback) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  WaitForResending(lock);
  STREAMING_CHECK(peer_actor_id_ == pull_msg->ActorId())
      << peer_actor_id_ << " " << pull_msg->ActorId();

  BundlePersistence persistence;
  std::vector<StreamingMessageBundleMetaPtr> bundle_vec;
  persistence.Load(pull_msg->CheckpointId(), bundle_vec, GetQueueID());
  if (bundle_vec.size() == 0) {
    STREAMING_LOG(INFO) << "OnPull can not load bundles from storage for checkpoint: "
                        << pull_msg->CheckpointId() << ". Looking in queue.";
    WriterQueue::OnPull(pull_msg, service, callback);
    return;
  }

  STREAMING_LOG(INFO) << "Reload " << bundle_vec.size() << " bundles from storage.";
  STREAMING_LOG(INFO) << "Found target msg id in storage. " << pull_msg->MsgId();
  auto response = std::unique_ptr<PullResponseMessage>(new PullResponseMessage(
      pull_msg->PeerActorId(), pull_msg->ActorId(), pull_msg->QueueId(), 0,
      pull_msg->MsgId(), 0, queue::flatbuf::StreamingQueueError::OK,
      is_upstream_first_pull_));
  is_upstream_first_pull_ = false;
  callback(std::move(response));

  for (auto &b : bundle_vec) {
    auto bundle = std::dynamic_pointer_cast<StreamingMessageBundle>(b);
    std::shared_ptr<ray::streaming::LocalMemoryBuffer> meta_buffer = bundle->MetaBuffer();
    uint64_t msg_id_end = bundle->GetLastMessageId();
    uint64_t msg_id_start = bundle->GetFirstMessageId();
    STREAMING_LOG(INFO) << "Resend bundles recovered from storage. msg_id_start "
                        << msg_id_start << " msg_id_end " << msg_id_end;
    auto &data_buffers = bundle->DataBuffers();
    QueueItem item(0, meta_buffer->Data(), meta_buffer->Size(), data_buffers.Data(),
                   data_buffers.DataSize(), 0, current_sys_time_ms(), msg_id_start,
                   msg_id_end, /*copy*/ true);
    SendItem(item, true, true, resend_id_);
  }

  auto last_bundle = bundle_vec.back();
  // TODO: fix equal message id
  OnPullInternal(last_bundle->GetLastMessageId() + 1, pull_msg->PeerActorId(),
                 pull_msg->ActorId(), pull_msg->QueueId(), service,
                 [](std::shared_ptr<PullResponseMessage> msg) {});
}

int CyclicWriterQueue::TryEvictItems(uint32_t max_evict_size) {
  QueueItem item = FrontProcessed();
  STREAMING_LOG(DEBUG) << "CyclicWriterQueue TryEvictItems queue_id: " << queue_id_
                       << " first_item: (" << item.MsgIdStart() << "," << item.MsgIdEnd()
                       << ")"
                       << " max_data_size_: " << max_data_size_
                       << " data_size_: " << data_size_;

  // There is no valide items in queue.
  if (QueueItem::IsInvalidQueueItem(item)) {
    return 0;
  }

  // Set evict target to QUEUE_INVALID_SEQ_ID(the max uint) will move all items
  // into esbuffer, until reaches `max_evict_size`.
  int count = EvictFromQueue(QUEUE_INVALID_SEQ_ID, max_evict_size);
  STREAMING_LOG(INFO) << count << " items move to esbuffer, current item: ("
                      << item.MsgIdStart() << "," << item.MsgIdEnd() << ")";
  return count;
}

int CyclicWriterQueue::GetItemsLargeThan(uint64_t start_msg_id,
                                         std::vector<QueueItem> &vec) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  int count = 0;
  FindItem(start_msg_id,
           [start_msg_id]() {
             STREAMING_LOG(WARNING)
                 << "GetItemsLargeThan start_msg_id too large: " << start_msg_id;
           },
           [start_msg_id]() {
             STREAMING_LOG(WARNING)
                 << "GetItemsLargeThan start_msg_id too small: " << start_msg_id;
           },
           [this, &vec, &count](std::list<QueueItem>::iterator queue_target,
                                uint64_t first_seq_id, uint64_t last_seq_id,
                                std::list<QueueItem>::iterator esbuffer_target) {
             Iterator(queue_target, esbuffer_target, first_seq_id, last_seq_id,
                      [&vec, &count](QueueItem &item, bool in_elastic_buffer) {
                        vec.push_back(item);
                        count++;
                      });
           });

  STREAMING_LOG(INFO) << "GetItemsLargeThan count: " << count;
  return count;
}

void ReaderQueue::OnConsumed(uint64_t msg_id, uint64_t bundle_id) {
  QueueItem item = FrontProcessed();
  while (item.MsgIdEnd() <= msg_id) {
    PopProcessed();
    if (collocate_) {
      /// If the current object id is not equal to the last collocate object id, it means
      /// that the upstream queue has switch to a new object. Meanwhile, all items in the
      /// last collocate object have been notified, so the object can be released safely.
      /// NOTE: There is a small problem, if we haven't notify the first item in the new
      /// object, the last object will not be released.
      const ObjectID &item_object_id = item.GetCollocateObjectId();
      if (!last_use_collocate_object_id_.IsNil() && !item_object_id.IsNil() &&
          last_use_collocate_object_id_ != item_object_id) {
        STREAMING_LOG(WARNING) << "ReaderQueue switch to new collocate object "
                               << item_object_id << ", releasing the old one "
                               << last_use_collocate_object_id_;
        ReleasePlasmaObject(last_use_collocate_object_id_);
      }
      last_use_collocate_object_id_ = item_object_id;
      STREAMING_LOG(DEBUG) << "OnConsumed " << last_use_collocate_object_id_ << " "
                           << item_object_id;
    }
    item = FrontProcessed();
  }
  Notify(msg_id, bundle_id);
  notify_count_++;
}

void ReaderQueue::NotifyStartLogging(uint64_t barrier_id, uint64_t msg_id,
                                     uint64_t seq_id) {
  STREAMING_LOG(INFO) << "NotifyStartLogging target_msg_id: " << msg_id
                      << " target_seq_id: " << seq_id << " barrier_id: " << barrier_id;
  StartLoggingMessage msg(actor_id_, peer_actor_id_, queue_id_, msg_id, seq_id,
                          barrier_id);
  auto buffer = msg.ToBytes();
  /// Send this notify and wait for respose infinitely until success.
  transport_->SendForResultWithRetry(std::move(buffer));
}

void ReaderQueue::Notify(uint64_t msg_id, uint64_t bundle_id) {
  NotificationMessage msg(actor_id_, peer_actor_id_, queue_id_, msg_id, bundle_id);

  transport_->Send(msg.ToBytes());
}

void ReaderQueue::OnData(QueueItem &item) {
  uint64_t current = current_sys_time_ms();
  if (current < item.TimestampMessage()) {
    last_item_transfer_latency_ = 0;
  } else {
    last_item_transfer_latency_ = current - item.TimestampMessage();
  }

  if (first_received_msg_id_ == QUEUE_INVALID_SEQ_ID) {
    first_received_msg_id_ = item.MsgIdStart();
    STREAMING_LOG(INFO) << "ReaderQueue received first msg msg_id: (" << item.MsgIdStart()
                        << "," << item.MsgIdEnd() << ")"
                        << " size: " << item.DataSize();
  }

  last_recv_seq_id_ = item.SeqId();
  last_recv_msg_id_ = item.MsgIdEnd();
  STREAMING_LOG(DEBUG) << "ReaderQueue::OnData queue_id: " << queue_id_
                       << " seq_id: " << last_recv_seq_id_ << " msg_id: ("
                       << item.MsgIdStart() << "," << item.MsgIdEnd() << ")"
                       << " size: " << item.DataSize();
  Push(item);
  current = current_sys_time_ms();
  if (current < item.TimestampItem()) {
    last_item_latency_ = 0;
  } else {
    last_item_latency_ = current - item.TimestampItem();
  }
  if (last_item_latency_ >= 10) {
    last_item_latency_large_than_10ms_++;
  }
  if (last_item_latency_ >= 5) {
    last_item_latency_large_than_5ms_++;
  }
}

void ReaderQueue::OnResendData(QueueItem &item, uint64_t resend_id,
                               queue::flatbuf::ResendReason reason) {
  if (dropped_resend_id_ == resend_id) {
    STREAMING_LOG(WARNING) << "Drop message id:(" << item.MsgIdStart() << ","
                           << item.MsgIdEnd() << ")."
                           << " resend_id: " << resend_id;
    return;
  }

  /// NOTE. Only pulled messages can be dropped.
  if (reason == queue::flatbuf::ResendReason::PULLED) {
    if (item.MsgIdStart() == first_received_msg_id_ &&
        item.MsgIdStart() == start_msg_id_) {
      STREAMING_LOG(WARNING)
          << "Current resend pulled message id duplicated with first received msg id."
          << " first_received_msg_id_: " << first_received_msg_id_ << " message id:("
          << item.MsgIdStart() << "," << item.MsgIdEnd() << "). Drop it.";
      dropped_resend_id_ = resend_id;
      return;
    }
  }

  last_recv_seq_id_ = item.SeqId();
  last_recv_msg_id_ = item.MsgIdEnd();
  Push(item);
}

bool ReaderQueue::CreateInternalBuffer(
    std::shared_ptr<plasma::PlasmaClient> &plasma_client, const ObjectID &object_id) {
  STREAMING_LOG(WARNING)
      << "[ReaderQueue] The queue is collocated, allocate bufferpool in plasma store: "
      << object_id;
  if (object_id == last_invalid_collocate_object_id_) {
    STREAMING_LOG(WARNING) << "invalid collocate object id.";
    return false;
  }
  plasma::ObjectBuffer object_buffer;
  auto status = plasma_client->Get(&object_id, 1, 10 * 1000, &object_buffer,
                                   /*is_from_worker=*/true);
  STREAMING_LOG(INFO) << "CreateInternalBuffer Get objects status: " << status;
  if (!status.ok() || object_buffer.data == nullptr) {
    STREAMING_LOG(WARNING) << "Get plasma object fail: " << object_id;
    last_invalid_collocate_object_id_ = object_id;
    return false;
  }
  buffer_pool_external_buffer_ =
      std::make_shared<PlasmaBuffer>(object_buffer.data, nullptr);
  SetCollocateObjectId(object_id);
  collocate_ = true;
  plasma_client_ = plasma_client;
  return true;
}

ReaderQueue::~ReaderQueue() {
  if (buffer_pool_external_buffer_ != nullptr) {
    STREAMING_LOG(WARNING)
        << "[ReaderQueue] The queue is collocated, release object in plasma store: "
        << object_id_;
    ReleasePlasmaObject(object_id_);
    plasma_client_ = nullptr;
  }
}

void ReaderQueue::GetProfilingInfo(ReaderQueueProfilingInfo &debug_info) {
  debug_info.seq_id = GetLastRecvSeqId();
  debug_info.last_pop_msg_id = GetLastPopMsgId();
  debug_info.last_pop_seq_id = GetLastPopSeqId();
  debug_info.reconnected_count = GetReconnectedCount();
  debug_info.notify_count = GetNotifyCount();
  debug_info.pending_count = PendingCount();
  debug_info.pull_status = GetPullStatus();
}

}  // namespace streaming
}  // namespace ray
