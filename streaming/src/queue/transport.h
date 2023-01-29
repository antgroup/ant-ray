#ifndef _STREAMING_QUEUE_TRANSPORT_H_
#define _STREAMING_QUEUE_TRANSPORT_H_

#include <condition_variable>
#include <mutex>
#include <queue>

#include "common/buffer.h"
#include "config.h"
#include "logging.h"
#include "message.h"
#include "ray/common/id.h"
#include "ray/streaming/streaming.h"
#include "streaming_queue_generated.h"

namespace ray {
namespace streaming {

enum TransportType { MEMORY, DIRECTCALL, MOCK };

/// duplex
class Transport {
 public:
  /// Send the buffer to peer, don't care about the return value.
  virtual void Send(std::unique_ptr<LocalMemoryBuffer> buffer) = 0;
  /// Send multiple buffers to peer, don't care about the return value.
  virtual void Send(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) = 0;
  /// Send the buffer to peer and wait for result synchronously, retry `retry_cnt` times.
  /// \param[in] buffer to be sent.
  /// \param[in] retry count.
  /// \param[in] timeout for each sending process.
  /// \return the buffer contains result.
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer,
      uint32_t retry_cnt = std::numeric_limits<uint32_t>::max(),
      uint64_t timeout_ms = COMMON_SYNC_CALL_TIMEOUTT_MS) = 0;
  /// Send the buffer to peer asynchronously, an ObjectID returned. The caller can
  /// get the result through the ObjectID.
  virtual ObjectID SendForResult(std::shared_ptr<LocalMemoryBuffer> buffer) = 0;
  /// Get the result.
  virtual std::shared_ptr<LocalMemoryBuffer> Recv(const ObjectID &object_id) = 0;
  virtual void Destroy() = 0;
  virtual TransportType GetTransportType() = 0;
};

template <class T>
class MemoryQueue {
 public:
  MemoryQueue(size_t size) : capacity_(size), destroyed_(false) {}

  void Push(T t, bool notify) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_.size() >= capacity_) {
      writeable_cv_.wait(lock);
    }
    queue_.push(std::move(t));
    if (notify) {
      readable_cv_.notify_one();
    }
  }

  T Pop() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_.empty() && !destroyed_) {
      readable_cv_.wait(lock);
    }

    if (destroyed_) {
      throw std::string("destroyed");
    }
    T t = std::move(queue_.front());
    queue_.pop();
    writeable_cv_.notify_one();

    return t;
  }

  T Front() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    if (queue_.empty()) {
      throw std::string("empty");
    }
    return queue_.front();
  }

  void Destroy() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    destroyed_ = true;
    readable_cv_.notify_all();
    writeable_cv_.notify_all();
  }

  void NotifyReader() {
    if (queue_.size() > 0) {
      readable_cv_.notify_one();
    }
  }

 private:
  std::mutex queue_mutex_;
  std::condition_variable readable_cv_;
  std::condition_variable writeable_cv_;
  std::queue<T> queue_;
  size_t capacity_;
  std::atomic<bool> destroyed_;
};

/// `SimpleMemoryTransport` implements a duplex memory based communication transport.
/// Both synchronous call and asynchronous call are supported, like direct call.
class SimpleMemoryTransport : public Transport {
 public:
  class Item {
   public:
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers_;
    ObjectID object_id_;
    bool need_return_;
  };

 public:
  SimpleMemoryTransport(
      std::function<void(std::vector<std::shared_ptr<LocalMemoryBuffer>>)> async_callback,
      std::function<std::shared_ptr<LocalMemoryBuffer>(
          std::vector<std::shared_ptr<LocalMemoryBuffer>>)>
          sync_callback,
      size_t size = 10240)
      : SimpleMemoryTransport(async_callback, sync_callback,
                              std::make_shared<MemoryQueue<Item>>(size),
                              std::make_shared<MemoryQueue<Item>>(size)) {}

  SimpleMemoryTransport(
      std::function<void(std::vector<std::shared_ptr<LocalMemoryBuffer>>)> async_callback,
      std::function<std::shared_ptr<LocalMemoryBuffer>(
          std::vector<std::shared_ptr<LocalMemoryBuffer>>)>
          sync_callback,
      std::shared_ptr<MemoryQueue<Item>> out, std::shared_ptr<MemoryQueue<Item>> in)
      : memory_queue_out_(out), memory_queue_in_(in), inflight_(false), drop_(false) {
    loop_thread_ = std::thread([this, async_callback, sync_callback] {
      while (true) {
        try {
          Item item = memory_queue_out_->Pop();
          if (item.need_return_ && sync_callback) {
            auto result = sync_callback(item.buffers_);
            memory_queue_in_->Push({{result}, item.object_id_, false}, false);
          } else if (async_callback) {
            async_callback(item.buffers_);
          }
        } catch (std::string e) {
          break;
        }
      }
    });
  }

  virtual ~SimpleMemoryTransport() { Destroy(); }

  void Send(std::unique_ptr<LocalMemoryBuffer> buffer) override {
    if (drop_) {
      STREAMING_LOG(INFO) << "SimpleMemoryTransport drop buffer.";
      return;
    }
    memory_queue_out_->Push({{std::move(buffer)}, ObjectID::Nil(), false}, !inflight_);
  }

  void Send(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) override {
    if (drop_) {
      STREAMING_LOG(INFO) << "SimpleMemoryTransport drop buffer.";
      return;
    }
    memory_queue_out_->Push({buffers, ObjectID::Nil(), false}, !inflight_);
  };

  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer,
      uint32_t retry_cnt = std::numeric_limits<uint32_t>::max(),
      uint64_t timeout_ms = COMMON_SYNC_CALL_TIMEOUTT_MS) override {
    ObjectID id = ObjectID::FromRandom();
    memory_queue_out_->Push({{std::move(buffer)}, id, true}, !inflight_);
    uint64_t start_time_ms = current_sys_time_ms();
    uint64_t current_time_ms = start_time_ms;
    while (current_time_ms < start_time_ms + timeout_ms) {
      current_time_ms = current_sys_time_ms();
      auto result = Recv(id);
      if (result) {
        return result;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return Recv(id);
  }

  ObjectID SendForResult(std::shared_ptr<LocalMemoryBuffer> buffer) override {
    ObjectID id = ObjectID::FromRandom();
    memory_queue_out_->Push({{buffer}, id, true}, !inflight_);
    return id;
  }

  std::shared_ptr<LocalMemoryBuffer> Recv(const ObjectID &object_id) override {
    try {
      Item item;
      do {
        item = memory_queue_in_->Front();
        if (item.object_id_ != object_id) {
          STREAMING_LOG(DEBUG) << "Unexpected object id. expect: " << object_id
                               << " got:" << item.object_id_;
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        memory_queue_in_->Pop();
      } while (item.object_id_ != object_id);
      return item.buffers_[0];
    } catch (std::string e) {
      return nullptr;
    }
  }

  void Destroy() override {
    memory_queue_out_->Destroy();
    memory_queue_in_->Destroy();
    if (loop_thread_.joinable()) {
      loop_thread_.join();
    }
  }

  TransportType GetTransportType() override { return TransportType::MEMORY; }

  /// Trigger inflight mode. If true, all the buffers sent through the transport
  /// will be holded in internal buffer. If false, exit inflight mode and all
  /// internal buffers can be read.
  void SetInFlight(bool inflight) {
    inflight_ = inflight;
    if (!inflight_) {
      memory_queue_out_->NotifyReader();
    }
  }

  void SetDrop(bool drop) { drop_ = drop; }

 private:
  std::shared_ptr<MemoryQueue<Item>> memory_queue_out_;
  std::shared_ptr<MemoryQueue<Item>> memory_queue_in_;
  std::thread loop_thread_;
  bool inflight_;
  bool drop_;
};

class MockTransport : public Transport {
 public:
  void Send(std::unique_ptr<LocalMemoryBuffer> buffer) {}
  void Send(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {}
  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer,
      uint32_t retry_cnt = std::numeric_limits<uint32_t>::max(),
      uint64_t timeout_ms = COMMON_SYNC_CALL_TIMEOUTT_MS) {
    return nullptr;
  }
  ObjectID SendForResult(std::shared_ptr<LocalMemoryBuffer> buffer) {
    return ObjectID::FromRandom();
  }
  std::shared_ptr<LocalMemoryBuffer> Recv(const ObjectID &object_id) { return nullptr; }
  void Destroy() {}
  TransportType GetTransportType() { return TransportType::MOCK; }
};

class DirectCallTransport : public Transport {
 public:
  DirectCallTransport(const ActorID &peer_actor_id, RayFunction &async_func,
                      RayFunction &sync_func)
      : peer_actor_id_(peer_actor_id), async_func_(async_func), sync_func_(sync_func) {
    if (IsInitializedInternal()) {
      worker_id_ = ray::CoreWorkerProcess::GetCoreWorker().GetWorkerID();
    } else {
      worker_id_ = WorkerID::FromRandom();
    }
    STREAMING_LOG(INFO) << "Transport constructor:";
    STREAMING_LOG(INFO) << "async_func lang: " << async_func_.GetLanguage();
    STREAMING_LOG(INFO) << "async_func: "
                        << async_func_.GetFunctionDescriptor()->ToString();
    STREAMING_LOG(INFO) << "sync_func lang: " << sync_func_.GetLanguage();
    STREAMING_LOG(INFO) << "sync_func: "
                        << sync_func_.GetFunctionDescriptor()->ToString();
  }
  virtual ~DirectCallTransport() = default;

  virtual void Destroy() override {}

  virtual void Send(std::unique_ptr<LocalMemoryBuffer> buffer) override;
  virtual void Send(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) override;
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer,
      uint32_t retry_cnt = std::numeric_limits<uint32_t>::max(),
      uint64_t timeout_ms = COMMON_SYNC_CALL_TIMEOUTT_MS) override;
  virtual ObjectID SendForResult(std::shared_ptr<LocalMemoryBuffer> buffer) override;
  virtual std::shared_ptr<LocalMemoryBuffer> Recv(const ObjectID &object_id) override;

  virtual TransportType GetTransportType() override { return TransportType::DIRECTCALL; }

 private:
  std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms);

 private:
  WorkerID worker_id_;
  ActorID peer_actor_id_;
  RayFunction async_func_;
  RayFunction sync_func_;
};

/// `TransportManager` manages a set of Transport objects.
class TransportManager {
 public:
  TransportManager() : io_service_dummy_worker_(io_service_), timer_(io_service_) {
    io_service_thread_ = std::thread([this] { io_service_.run(); });
  }
  virtual ~TransportManager() {
    io_service_.stop();
    if (io_service_thread_.joinable()) {
      io_service_thread_.join();
    }
  }

 protected:
  std::unordered_map<ObjectID, std::shared_ptr<Transport>> out_transports_;
  /// The objects returned by transport, to be waited for.
  std::vector<ObjectID> waiting_objects_;
  std::vector<ObjectID> waiting_queue_ids_;
  std::unordered_map<ObjectID, std::shared_ptr<LocalMemoryBuffer>> pending_buffers_;
  std::mutex mutex_;
  boost::asio::io_service io_service_;
  boost::asio::io_service::work io_service_dummy_worker_;
  std::thread io_service_thread_;
  boost::asio::deadline_timer timer_;

 public:
  /// Send the buffer to peer actor, return the result buffer asynchronously through
  /// `callback`. Each buffer will be retry infinitely if sent fail.
  /// \param[in] the queue id to specify which transport to sent to.
  /// \param[in] the buffer to ben sent.
  /// \param[in] the callback function to be called when the result return from peer.
  void SendForResultWithRetryAsync(
      const ObjectID &queue_id, std::shared_ptr<LocalMemoryBuffer> buffer,
      std::function<void(ObjectID, std::shared_ptr<LocalMemoryBuffer>)> callback);

  std::shared_ptr<Transport> GetOutTransport(const ObjectID &queue_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = out_transports_.find(queue_id);
    if (it == out_transports_.end()) return nullptr;

    return it->second;
  }

  void SetOutTransport(const ObjectID &queue_id, std::shared_ptr<Transport> transport) {
    std::lock_guard<std::mutex> lock(mutex_);
    STREAMING_LOG(INFO) << "TransportManager::SetOutTransport " << queue_id;
    out_transports_.emplace(queue_id, transport);
  }

  void EraseOutTransport(const ObjectID &queue_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    out_transports_.erase(queue_id);
    size_t index = -1;

    auto it = std::find(waiting_queue_ids_.begin(), waiting_queue_ids_.end(), queue_id);
    if (it == waiting_queue_ids_.end()) {
      return;
    }
    index = std::distance(waiting_queue_ids_.begin(), it);
    waiting_queue_ids_.erase(
        std::remove(waiting_queue_ids_.begin(), waiting_queue_ids_.end(), queue_id),
        waiting_queue_ids_.end());
    STREAMING_CHECK(index < waiting_objects_.size());
    waiting_objects_.erase(waiting_objects_.begin() + index);

    pending_buffers_.erase(queue_id);
  }

 protected:
  virtual std::vector<std::shared_ptr<LocalMemoryBuffer>> Recv(
      const std::vector<ObjectID> &object_ids) = 0;

 private:
  void Wait(std::function<void(ObjectID, std::shared_ptr<LocalMemoryBuffer>)> callback);
  void Retry(std::function<void(ObjectID, std::shared_ptr<LocalMemoryBuffer>)> callback);
};

class SimpleMemoryTransportManager : public TransportManager {
 protected:
  virtual std::vector<std::shared_ptr<LocalMemoryBuffer>> Recv(
      const std::vector<ObjectID> &object_ids) override {
    std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers;
    for (unsigned int i = 0; i < waiting_queue_ids_.size(); i++) {
      buffers.push_back(out_transports_[waiting_queue_ids_[i]]->Recv(object_ids[i]));
    }
    return buffers;
  }
};

class DirectCallTransportManager : public TransportManager {
 protected:
  virtual std::vector<std::shared_ptr<LocalMemoryBuffer>> Recv(
      const std::vector<ObjectID> &object_ids) override;
};

}  // namespace streaming
}  // namespace ray
#endif
