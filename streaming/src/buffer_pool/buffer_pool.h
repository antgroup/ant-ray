
#ifndef RAY_STREAMING_BUFFER_POOL_H
#define RAY_STREAMING_BUFFER_POOL_H

#include <condition_variable>
#include <tuple>
#include <vector>

#include "common/buffer.h"
#include "common/status.h"
#include "logging.h"
#include "util/utility.h"

namespace ray {
namespace streaming {

/// A SPSC sequential read/write/release buffer pool
class BufferPool {
 public:
  /// Buffer pool to allocate/release memory by self.
  BufferPool(uint64_t total_size, uint64_t min_buffer_size);

  /// Buffer pool on a provided buffer, provided buffer is never be released by this
  /// buffer pool, mainly for shared-memory case.
  BufferPool(uint8_t *external_buffer, uint64_t size);

  ~BufferPool();

  /// This method doesn't change current_writing_buffer_->data_end, so this buffer pool
  /// can't be used by multi producer.
  /// @param buffer result buffer, if buffer pool doesn't have enough memory, return an
  /// empty buffer
  StreamingStatus GetBuffer(MemoryBuffer *buffer);

  /// This method doesn't change current_writing_buffer_->data_end, so this buffer pool
  /// can't be used by multi producer.
  /// @param min_size minimum size of returned buffer
  /// @param buffer result buffer, if buffer pool doesn't have enough memory, return an
  /// empty buffer
  StreamingStatus GetBuffer(uint64_t min_size, MemoryBuffer *buffer);

  /// Return a buffer of minimal size `min_size`, blocks if pool doesn't have enough
  /// memory.
  /// @param min_size minimum size of returned buffer
  /// @param buffer result buffer
  /// @param timeout timeout in ms
  StreamingStatus GetBufferBlockedTimeout(
      uint64_t min_size, MemoryBuffer *buffer,
      uint64_t timeout_ms = std::numeric_limits<uint64_t>::max());

  /// Mark a buffer as used
  /// @param address buffer start address
  /// @param size buffer size
  StreamingStatus MarkUsed(uint64_t address, uint64_t size);

  /// Mark a buffer as used
  /// @param address buffer address
  /// @param size buffer size
  StreamingStatus MarkUsed(const uint8_t *ptr, uint64_t size);

  /// Return a buffer to buffer pool
  /// @param address buffer start address
  /// @param size buffer size
  StreamingStatus Release(uint64_t address, uint64_t size);

  /// Return a buffer to buffer pool
  /// @param address buffer address
  /// @param size buffer size
  StreamingStatus Release(const uint8_t *ptr, uint64_t size);

  /// Return whether the address is a large buffer.
  /// @param address buffer address
  bool IsLargeBuffer(uint64_t address);

  /// buffer pool usage info for debug
  std::string PrintUsage();

  /// Return how much memory buffer pool used.
  uint64_t memory_used() { return current_size_; }

  /// Return how much memory buffers used.
  uint64_t used() { return used_; }

  uint64_t pool_size() { return pool_size_; }

  /// Return needed size of buffer when pool doesn't have enough space.
  uint64_t needed_size() { return needed_size_; }

  /// Return true if buffer1 is contiguous with buffer2
  static bool IsContiguous(const MemoryBuffer &buffer1, const MemoryBuffer &buffer2) {
    return buffer1.Data() + buffer1.Size() == buffer2.Data();
  }

  /// Return a large buffer of size `size`.
  /// @param size the size of returned buffer
  /// @param buffer result buffer
  void GetDisposableBuffer(uint64_t size, MemoryBuffer *buffer);

  inline bool IsDisposableBuffer(const uint8_t *ptr) {
    AutoSpinLock lk(lock);
    auto it = large_buffer_set_.find(reinterpret_cast<uint64_t>(ptr));
    return it != large_buffer_set_.end();
  }

  inline void SetOOMLimit(uint64_t oom_limit) { oom_limit_size_ = oom_limit; }

  inline uint64_t GetOOMLimit() { return oom_limit_size_; }

 private:
  /// InnerBuf to record buffer usage info.
  /// when data_start == data_end, buffer is null; when data_end < data_start, buffer
  /// usage is wrap around; when data_end + 1 == data_start, buffer usage is wrap around
  /// full. We left a byte to differentiate forward full and wrap around full.
  ///
  /// buffer usage can be in two states:
  /// @code
  /// state 1:
  /// -----------------------------------------------------------------------------------
  /// |                         data_start           data_end                           |
  /// |------- left remained -------|------- used -------|------- right remained -------|
  /// -----------------------------------------------------------------------------------
  ///
  /// state 2 (wrap around):
  /// -----------------------------------------------------------------------------------
  /// |                       data_end                 data_start                       |
  /// |-------- left used --------|------- remained -------|-------- right used --------|
  /// -----------------------------------------------------------------------------------
  /// @endcode
  struct InnerBuf {
    // inclusive. it's also the address returned by malloc for this buffer
    u_int64_t buffer_start;
    u_int64_t buffer_end;  // exclusive
    u_int64_t data_start;  // inclusive
    u_int64_t data_end;    // exclusive
    // If set, it indicates buffer is not in use, release operation can free buffer
    // when buffer content is empty. buffer is used by writer if not set.
    bool marked;

    inline uint64_t used_size() { return data_end - data_start; }
    inline uint64_t buffer_size() { return buffer_end - buffer_start; }
  };

  StreamingStatus FindBuffer(u_int64_t address, InnerBuf **buffer);

  /// create a new InnerBuffer and add it in buffer pool.
  /// Return OutOfMemory if buffer pool is full.
  /// This method will ensure `current_size_ == pool_size_` when buffer pool is full
  /// finally.
  StreamingStatus NewBuffer(uint64_t size, InnerBuf **buffer);

  /// buffer usage info for debug
  static std::string PrintUsage(InnerBuf *buf);

  static bool GetRemained(InnerBuf *buf, uint64_t min_size, uint64_t *address,
                          uint64_t *remained);

  static uint64_t GetMaxRemained(InnerBuf *buf);

  /// pool size in bytes of this buffer pool
  uint64_t pool_size_;

  /// min size in bytes of an internal memory buffer
  uint64_t min_buffer_size_;

  /// The maximum buffer size can be allocated in buffer, oom triggered
  /// if required buffer size exceeds `oom_limit_size_`
  uint64_t oom_limit_size_;

  /// provided buffer address
  uint8_t *external_buffer_;

  InnerBuf *current_writing_buffer_ = nullptr;

  InnerBuf *current_releasing_buffer_ = nullptr;

  /// buffers held by this buffer pool. buffers address increase monotonously.
  std::vector<InnerBuf *> buffers_;

  /// compare function to sort `buffers_``
  static bool compareFunc(const InnerBuf *left, const InnerBuf *right);

  /// Current size of buffer pool.
  /// When buffer pool is full, current_size_ will be equal to pool_size_
  uint64_t current_size_ = 0;

  /// Current usage of buffer pool.
  uint64_t used_ = 0;

  // Benchmark shows that use spin lock have better performance than mutex
  std::atomic_flag lock = ATOMIC_FLAG_INIT;
  uint64_t needed_size_ = 0;
  std::mutex m;
  std::condition_variable buffer_available_cv;

  /// Buffers whose size is larger than buffer pool size. Allocated from `malloc`
  /// directly, and `free` when release buffer.
  std::set<uint64_t> large_buffer_set_;
};

/// Used to manage multiple buffers when bundle is not continuous in buffer pool.
class CompositeBuffer final {
 public:
  CompositeBuffer() : data_(nullptr), size_(0) {}
  void AddBuffer(MemoryBuffer &buffer) {
    buffers_.emplace_back(buffer);
    /// Copy data to a continuous buffer if we have multiple MemoryBuffer in
    /// CompositeBuffer.
    if (buffers_.size() > 1) {
      if (data_) {
        free(data_);
        data_ = nullptr;
        size_ = 0;
      }
      size_ = std::accumulate(
          buffers_.begin(), buffers_.end(), 0,
          [](int size, MemoryBuffer &buffer) { return size + buffer.Size(); });
      data_ = (uint8_t *)malloc(size_);
      uint8_t *p_cur = data_;
      for (auto &buffer : buffers_) {
        memcpy(p_cur, buffer.Data(), buffer.Size());
        p_cur += buffer.Size();
      }
    }
  }

  void Release(std::shared_ptr<BufferPool> buffer_pool) {
    if (1 != buffers_.size()) {
      free(data_);
    }
    /// Release buffers in bufferpool
    for (auto &buffer : buffers_) {
      /// TODO: Release(buffer)
      buffer_pool->Release(buffer.Data(), buffer.Size());
    }
    data_ = nullptr;
    size_ = 0;
  }

  std::vector<MemoryBuffer> GetBuffers() { return buffers_; }

  uint8_t *Data() {
    if (data_) {
      return data_;
    }

    size_t size = buffers_.size();
    if (1 == size) {
      return buffers_[0].Data();
    } else if (0 == size) {
      return nullptr;
    } else {
      STREAMING_CHECK(false) << "CompositeBuffer access Data fail. " << buffers_.size();
    }
    return nullptr;
  }

  size_t DataSize() {
    if (1 == buffers_.size()) {
      return buffers_[0].Size();
    } else {
      return size_;
    }
  }
  size_t BufferCount() { return buffers_.size(); }

 private:
  uint8_t *data_;
  size_t size_;
  std::vector<MemoryBuffer> buffers_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_BUFFER_POOL_H
