#include "elastic_buffer.h"
namespace ray {
namespace streaming {
ElasticBufferConfig::ElasticBufferConfig(bool enable_, uint32_t max_save_buffer_size_,
                                         uint32_t flush_buffer_size_,
                                         uint32_t file_cache_size_,
                                         std::string file_directory_)
    : enable(enable_),
      max_save_buffer_size(max_save_buffer_size_),
      flush_buffer_size(flush_buffer_size_),
      file_cache_size(file_cache_size_),
      file_directory(file_directory_) {}
}  // namespace streaming
}  // namespace ray
