#ifndef RAY_STREAMING_MEMORY_POOL_H
#define RAY_STREAMING_MEMORY_POOL_H
#include "ngx_slab.h"
namespace ray {
namespace streaming {

class StreamingMemoryPool {
 public:
  StreamingMemoryPool(size_t pool_size);
  virtual uint8_t *RequestMemory(size_t size) = 0;
  virtual void ReleaseMemory(uint8_t *buffer) = 0;
  virtual ~StreamingMemoryPool() = default;

 protected:
  virtual void DestroyPool() = 0;

 protected:
  size_t pool_size_;
  bool in_used_;
};

class NginxMemoryPool : public StreamingMemoryPool {
 public:
  explicit NginxMemoryPool(size_t pool_size);
  uint8_t *RequestMemory(size_t size) override;
  void ReleaseMemory(uint8_t *buffer) override;
  ~NginxMemoryPool();

 protected:
  void DestroyPool() override;

 private:
  ngx_slab_pool_t *sp;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MEMORY_POOL_H
