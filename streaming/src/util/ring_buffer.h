#ifndef RAY_STREAMING_RING_BUFFER_H
#define RAY_STREAMING_RING_BUFFER_H

#include <atomic>
#include <boost/circular_buffer.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <queue>

#include "logging.h"
#include "ray/common/status.h"

namespace ray {
namespace streaming {

/*!
 * @brief Since limitation of streaming queue, we mayn't sent
 * queue item successful once, so streaming transient buffer is token for
 * transient memory util messages are accepted by streaming queue.
 */
class StreamingTransientBuffer {
 private:
  std::shared_ptr<uint8_t> transient_buffer_;
  // bufferSize is length of last serialization data
  uint32_t transient_buffer_size_ = 0;
  uint32_t max_transient_buffer_size_ = 0;
  bool transient_flag_ = false;

 public:
  inline size_t GetTransientBufferSize() const { return transient_buffer_size_; }

  inline void SetTransientBufferSize(uint32_t new_transient_buffer_size) {
    transient_buffer_size_ = new_transient_buffer_size;
  }

  inline size_t GetMaxTransientBufferSize() const { return max_transient_buffer_size_; }

  inline const uint8_t *GetTransientBuffer() const { return transient_buffer_.get(); }

  inline uint8_t *GetTransientBufferMutable() const { return transient_buffer_.get(); }

  /*!``
   * @brief To reuse transient buffer, we will realloc buffer memory if size of needed
   * message bundle raw data is greater-than original buffer size.
   * @param size buffer size
   */
  inline void ReallocTransientBuffer(uint32_t size) {
    transient_buffer_size_ = size;
    transient_flag_ = true;
    if (max_transient_buffer_size_ > size) {
      return;
    }
    max_transient_buffer_size_ = size;
    transient_buffer_.reset(new uint8_t[size], std::default_delete<uint8_t[]>());
  }

  inline bool IsTransientAvaliable() { return transient_flag_; }

  inline void FreeTransientBuffer(bool is_force = false) {
    transient_buffer_size_ = 0;
    transient_flag_ = false;
    /*
     * Transient buffer always holds max size buffer among all messages, which is
     * wasteful.
     * So expiration time is considerable idea to release large buffer if this transient
     * buffer
     * pointer hold it in long time.
     */
    if (is_force) {
      max_transient_buffer_size_ = 0;
      transient_buffer_.reset();
    }
  }

  virtual ~StreamingTransientBuffer() = default;
};

template <class T>
class AbstractRingBuffer {
 public:
  virtual void Push(const T &) = 0;
  virtual void Push(T &&) = 0;
  virtual void Pop() = 0;
  virtual T &Front() = 0;
  virtual bool Empty() const = 0;
  virtual bool Full() const = 0;
  virtual size_t Size() const = 0;
  virtual size_t Capacity() const = 0;
};

template <class T>
class BlockingQueue : public AbstractRingBuffer<T> {
 private:
  mutable std::mutex ring_buffer_mutex_;
  std::condition_variable empty_cv_;
  std::condition_variable full_cv_;
  std::deque<T> buffer_;
  size_t capacity_;
  bool is_started_;

 public:
  explicit BlockingQueue(size_t size) : capacity_(size), is_started_(true) {}
  virtual ~BlockingQueue() {
    is_started_ = false;
    full_cv_.notify_all();
    empty_cv_.notify_all();
  };

  void Push(const T &t) {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (buffer_.size() >= capacity_ && is_started_) {
      full_cv_.wait(lock);
    }
    if (!is_started_) {
      return;
    }
    buffer_.push_back(t);
    empty_cv_.notify_one();
  }

  void Push(T &&t) { STREAMING_CHECK(false) << "Not yet supported"; }

  void Pop() {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (buffer_.empty() && is_started_) {
      empty_cv_.wait(lock);
    }
    if (!is_started_) {
      return;
    }
    buffer_.pop_front();
    full_cv_.notify_one();
  }

  T &PopAndGet() {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (buffer_.empty() && is_started_) {
      empty_cv_.wait(lock);
    }
    if (!is_started_) {
      return T();
    }
    T &res = buffer_.front();
    buffer_.pop_front();
    full_cv_.notify_one();
    return res;
  }

  T &At(uint32_t index) {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    return buffer_.at(index);
  }

  T &Front() {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    return buffer_.front();
  }

  bool Empty() const {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    return buffer_.empty();
  }

  bool Full() const {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    return buffer_.size() == capacity_;
  }

  size_t Size() const {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    return buffer_.size();
  }

  size_t Capacity() const { return capacity_; }
};

template <class T>
class RingBufferImplThreadSafe : public AbstractRingBuffer<T> {
 private:
  mutable boost::shared_mutex ring_buffer_mutex_;
  boost::circular_buffer<T> buffer_;

 public:
  explicit RingBufferImplThreadSafe(size_t size) : buffer_(size) {}
  virtual ~RingBufferImplThreadSafe() = default;
  void Push(const T &t) {
    boost::unique_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    buffer_.push_back(t);
  }
  void Push(T &&t) { STREAMING_CHECK(false) << "Not yet supported"; }
  void Pop() {
    boost::unique_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    buffer_.pop_front();
  }
  T &Front() {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.front();
  }
  bool Empty() const {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.empty();
  }
  bool Full() const {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.full();
  }
  size_t Size() const {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.size();
  }
  size_t Capacity() const { return buffer_.capacity(); }
};

template <class T>
class RingBufferImplLockFree : public AbstractRingBuffer<T> {
  // `RingBufferItem` is the only supported type which can be stored in
  // `RingBufferImplLockFree`. The memory layout of a `RingBufferItem` object will not be
  // modified after initialization, so the cpu cache miss can be reduced when pushing or
  // poping items from multiple ring buffers.
 public:
  class RingBufferItem {
   public:
    RingBufferItem() : ptr_(new T()) {}
    RingBufferItem(T *ptr) : ptr_(ptr) {}
    RingBufferItem &operator=(const RingBufferItem &wrapper) {
      *ptr_ = std::move(*(wrapper.ptr_));
      return *this;
    }
    RingBufferItem &operator=(T &&ptr) {
      *ptr_ = std::move(ptr);
      return *this;
    }
    RingBufferItem &operator=(T &ptr) = delete;
    RingBufferItem(const RingBufferItem &wrapper) = delete;
    T &Get() { return *ptr_; }

   private:
    T *const ptr_;
  };

 private:
  RingBufferItem *buffer_;
  std::atomic<size_t> capacity_;
  std::atomic<size_t> read_index_;
  std::atomic<size_t> write_index_;

 public:
  explicit RingBufferImplLockFree(size_t size)
      /// Note(wanxing.wwx), using `size+1` as capacity because RingBufferImplLockFree
      /// occupy an extra item in the internal implementation.
      : capacity_(size + 1), read_index_(0), write_index_(0) {
    buffer_ = new RingBufferItem[size + 1];
  }
  virtual ~RingBufferImplLockFree() {
    if (buffer_) {
      delete[] buffer_;
    }
  }

  void Push(const T &t) { STREAMING_CHECK(false) << "Not yet supported"; }

  void Push(T &&t) {
    STREAMING_CHECK(!Full());
    buffer_[write_index_] = std::move(t);
    write_index_ = IncreaseIndex(write_index_);
  }

  void Pop() {
    STREAMING_CHECK(!Empty());
    read_index_ = IncreaseIndex(read_index_);
  }

  T &Front() {
    STREAMING_CHECK(!Empty());
    return buffer_[read_index_].Get();
  }

  bool Empty() const { return write_index_ == read_index_; }

  bool Full() const { return IncreaseIndex(write_index_) == read_index_; }

  size_t Size() const { return (write_index_ + capacity_ - read_index_) % capacity_; }

  size_t Capacity() const { return capacity_; }

 private:
  size_t IncreaseIndex(size_t index) const { return (index + 1) % capacity_; }
};

template <class T>
struct StreamingNode {
  StreamingNode() : message(nullptr), next(nullptr) {}
  T message;
  StreamingNode *next;
};

template <class T>
class StreamingList {
 private:
  StreamingNode<T> *head_;
  StreamingNode<T> *tail_;
  std::atomic_flag lock = ATOMIC_FLAG_INIT;
  size_t write_buffer_size_;
  size_t read_buffer_size_;

 public:
  StreamingList();
  ~StreamingList();
  void Push(const T &message_ptr);
  void Pop();
  bool Empty() const;
  T &Front();
  size_t Size() const;
};

template <class T>
StreamingList<T>::StreamingList() : write_buffer_size_(0), read_buffer_size_(0) {
  head_ = new StreamingNode<T>();
  head_->next = nullptr;
  tail_ = head_;
}

template <class T>
StreamingList<T>::~StreamingList() {
  while (!Empty()) {
    Pop();
  }
  delete head_;
}

template <class T>
void StreamingList<T>::Push(const T &message_ptr) {
  StreamingNode<T> *message_node = new StreamingNode<T>();
  message_node->message = message_ptr;
  while (lock.test_and_set())
    ;
  tail_->next = message_node;
  tail_ = tail_->next;
  write_buffer_size_ += message_ptr->PayloadSize();
  lock.clear();
}

template <class T>
void StreamingList<T>::Pop() {
  StreamingNode<T> *node = head_->next;
  bool is_pop_tail = false;
  if (node == tail_) {
    while (lock.test_and_set())
      ;
    is_pop_tail = true;
  }
  head_->next = node->next;
  read_buffer_size_ += node->message->PayloadSize();
  if (node == tail_) {
    tail_ = head_;
    read_buffer_size_ = 0;
    write_buffer_size_ = 0;
  }
  delete node;
  node = nullptr;
  if (is_pop_tail) {
    lock.clear();
  }
}

template <class T>
bool StreamingList<T>::Empty() const {
  return write_buffer_size_ == read_buffer_size_;
}

template <class T>
size_t StreamingList<T>::Size() const {
  return write_buffer_size_ - read_buffer_size_;
}

template <class T>
T &StreamingList<T>::Front() {
  return head_->next->message;
}

template <class T>
class ListBufferImplLockFree : public AbstractRingBuffer<T> {
 private:
  StreamingList<T> buffer_;
  size_t capacity_;

 public:
  explicit ListBufferImplLockFree(size_t size) : capacity_(size) {}
  virtual ~ListBufferImplLockFree() = default;

  void Push(const T &t) { buffer_.Push(t); }

  void Push(T &&t) { STREAMING_CHECK(false) << "Not yet supported"; }

  void Pop() { buffer_.Pop(); }

  T &Front() { return buffer_.Front(); }

  bool Empty() const { return buffer_.Empty(); }

  bool Full() const { return buffer_.Size() >= capacity_; }

  size_t Size() const { return buffer_.Size(); }

  size_t Capacity() const { return capacity_; }
};

enum class StreamingRingBufferType : uint8_t { SPSC_LOCK, SPSC, SPSC_LIST };

template <class T>
class StreamingRingBuffer {
 private:
  // TODO(lingxuan.zlx):
  // boost circular was used casuse of simplifing implementation,
  // and there are some room space for optimization in future.
  std::shared_ptr<AbstractRingBuffer<T>> message_buffer_;

 public:
  explicit StreamingRingBuffer(size_t buf_size, StreamingRingBufferType buffer_type =
                                                    StreamingRingBufferType::SPSC_LOCK);

  bool Push(const T &msg);

  bool Push(T &&msg);

  T &Front();

  void Pop();

  bool IsFull() const;

  bool IsEmpty() const;

  size_t Size() const;

  size_t Capacity() const;
};

template <class T>
StreamingRingBuffer<T>::StreamingRingBuffer(size_t buf_size,
                                            StreamingRingBufferType buffer_type) {
  switch (buffer_type) {
  case StreamingRingBufferType::SPSC:
    message_buffer_ = std::make_shared<RingBufferImplLockFree<T>>(buf_size);
    break;
  case StreamingRingBufferType::SPSC_LIST:
    message_buffer_ = std::make_shared<ListBufferImplLockFree<T>>(buf_size);
    break;
  case StreamingRingBufferType::SPSC_LOCK:
  default:
    message_buffer_ = std::make_shared<RingBufferImplThreadSafe<T>>(buf_size);
  }
}

template <class T>
bool StreamingRingBuffer<T>::Push(const T &msg) {
  message_buffer_->Push(msg);
  return true;
}

template <class T>
bool StreamingRingBuffer<T>::Push(T &&msg) {
  message_buffer_->Push(std::move(msg));
  return true;
}

template <class T>
T &StreamingRingBuffer<T>::Front() {
  STREAMING_CHECK(!message_buffer_->Empty());
  return message_buffer_->Front();
}

template <class T>
void StreamingRingBuffer<T>::Pop() {
  STREAMING_CHECK(!message_buffer_->Empty());
  message_buffer_->Pop();
}

template <class T>
bool StreamingRingBuffer<T>::IsFull() const {
  return message_buffer_->Full();
}

template <class T>
bool StreamingRingBuffer<T>::IsEmpty() const {
  return message_buffer_->Empty();
}

template <class T>
size_t StreamingRingBuffer<T>::Size() const {
  return message_buffer_->Size();
};

template <class T>
size_t StreamingRingBuffer<T>::Capacity() const {
  return message_buffer_->Capacity();
}
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_RING_BUFFER_H
