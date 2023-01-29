#include "streaming_memory_pool.h"

namespace ray {
namespace streaming {
StreamingMemoryPool::StreamingMemoryPool(size_t pool_size)
    : pool_size_(pool_size), in_used_(true) {}

NginxMemoryPool::NginxMemoryPool(size_t pool_size)
    : StreamingMemoryPool(pool_size),
      sp(reinterpret_cast<ngx_slab_pool_t *>(malloc(pool_size))) {
  // we assume min pool size is 8192
  pool_size_ = pool_size < (1 << 13) ? (1 << 13) : pool_size;
  sp->addr = reinterpret_cast<u_char *>(sp);
  sp->min_shift = 3;
  sp->end = reinterpret_cast<u_char *>(sp) + pool_size_;
  ngx_slab_sizes_init();
  ngx_slab_init(sp);
}

uint8_t *NginxMemoryPool::RequestMemory(size_t size) {
  return (uint8_t *)ngx_slab_alloc(sp, size);
}

void NginxMemoryPool::ReleaseMemory(uint8_t *buffer) { ngx_slab_free(sp, buffer); }

void NginxMemoryPool::DestroyPool() {
  free(sp);
  in_used_ = false;
}

NginxMemoryPool::~NginxMemoryPool() {
  if (in_used_) {
    DestroyPool();
  }
}
}  // namespace streaming
}  // namespace ray
