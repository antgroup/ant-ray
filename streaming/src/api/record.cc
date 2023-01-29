#include "record.h"

#include <cstring>

namespace ray {
namespace streaming {

class RecordBufferImpl : public RecordBuffer {
 public:
  RecordBufferImpl(uint8_t *data, size_t data_size) : size_(data_size) {
    buffer_ = std::shared_ptr<uint8_t>(new uint8_t[data_size]);
    memcpy(buffer_.get(), data, data_size);
  }
  uint8_t *Data() override { return reinterpret_cast<uint8_t *>(buffer_.get()); }
  const size_t Size() override { return size_; }
  virtual ~RecordBufferImpl() { std::cout << "Record buffer removed." << std::endl; };

 private:
  std::shared_ptr<uint8_t> buffer_;
  size_t size_;
};

LocalRecord::~LocalRecord() { std::cout << "Local record deconstructing." << std::endl; }

LocalRecord::LocalRecord(const LocalRecord &record) {
  value_ = record.value_;
  stream_name_ = record.stream_name_;
  retract_ = record.retract_;
}

LocalRecord BuildRecordFromBuffer(uint8_t *data, uint32_t data_size) {
  std::shared_ptr<RecordBuffer> buffer =
      std::make_shared<RecordBufferImpl>(data, data_size);
  LocalRecord local_record(std::move(buffer));
  return local_record;
}
}  // namespace streaming
}  // namespace ray