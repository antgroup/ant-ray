#ifndef RAY_STREAMING_BUFFER_H
#define RAY_STREAMING_BUFFER_H
#include <cstdint>
#include <cstdio>
#include <memory>

#include "logging.h"
#include "ray/common/buffer.h"

namespace ray {
namespace streaming {

/// Alternative buffer to replace ray::LocalMemoryBuffer for better performance.
/// Only used in transport layer.
class LocalMemoryBuffer final : public ::ray::Buffer {
 public:
  LocalMemoryBuffer() : data_(nullptr), size_(0), own_data_(false), copy_data_(false) {}

  LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data = false)
      : LocalMemoryBuffer(data, size, copy_data, copy_data ? true : false) {}

  /// Construct a LocalMemory
  /// \param data the raw buffer pointer managed by the buffer object
  /// \param size the buffer size which `data` pointed to
  /// \param own_data whether the ownership belong to the buffer object or not. If true,
  /// the raw buffer will be freed in deconstructor
  /// \param copy_data if true, another buffer of the same size is allocated and data
  /// copied
  LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data, bool own_data)
      : own_data_(own_data), copy_data_(copy_data) {
    // F F
    // T T
    // T F
    // --- F T ---
    STREAMING_CHECK(own_data || !copy_data);
    if (copy_data_) {
      data_ = new uint8_t[size];
      memcpy(data_, data, size);
      size_ = size;
    } else {
      data_ = data;
      size_ = size;
    }
  }

  LocalMemoryBuffer &operator=(const LocalMemoryBuffer &) noexcept = default;
  LocalMemoryBuffer(const LocalMemoryBuffer &) noexcept = default;
  LocalMemoryBuffer(LocalMemoryBuffer &&buffer) {
    data_ = buffer.data_;
    size_ = buffer.size_;
    own_data_ = buffer.own_data_;
    copy_data_ = buffer.copy_data_;
    buffer.own_data_ = false;
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return copy_data_; }

  bool IsPlasmaBuffer() const override { return false; }

  ~LocalMemoryBuffer() override {
    if (own_data_) {
      delete[] data_;
      data_ = nullptr;
      size_ = 0;
    }
  }

  std::string PrintRange() {
    std::stringstream ss;
    ss << "[" << reinterpret_cast<void *>(data_) << ", "
       << reinterpret_cast<void *>(data_ + size_) << "]";
    return ss.str();
  }

 private:
  uint8_t *data_;
  size_t size_;
  bool own_data_;
  bool copy_data_;
};

using MemoryBuffer = LocalMemoryBuffer;
/// Represents a byte buffer for plasma object. This can be used to hold the
/// reference to a plasma object (via the underlying plasma::PlasmaBuffer).
class PlasmaBuffer : public ::ray::Buffer {
 public:
  PlasmaBuffer(std::shared_ptr<ray::Buffer> buffer,
               std::function<void(PlasmaBuffer *)> on_delete = nullptr)
      : buffer_(buffer), on_delete_(on_delete) {}

  uint8_t *Data() const override { return const_cast<uint8_t *>(buffer_->Data()); }

  size_t Size() const override { return buffer_->Size(); }

  bool OwnsData() const override { return true; }

  bool IsPlasmaBuffer() const override { return true; }

  ~PlasmaBuffer() {
    if (on_delete_ != nullptr) {
      on_delete_(this);
    }
  };

 private:
  std::shared_ptr<ray::Buffer> buffer_;
  /// Callback to run on destruction.
  std::function<void(PlasmaBuffer *)> on_delete_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_BUFFER_H
