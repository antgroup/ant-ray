#include "test_utils.h"

namespace ray {
namespace streaming {
namespace util {

ray::streaming::MemoryBuffer ToMessageBuffer(DataWriter *writer, const ray::ObjectID &qid,
                                             uint8_t *data, uint32_t size) {
  ray::streaming::MemoryBuffer buffer;
  auto buffer_pool = writer->GetBufferPool(qid);
  STREAMING_CHECK(buffer_pool->GetBuffer(size, &buffer) == StreamingStatus::OK);
  while (buffer.Size() < size) {
    STREAMING_LOG(INFO) << "can't get enough buffer to write, need " << size
                        << ", wait 1 ms, pool usage " << buffer_pool->PrintUsage();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    STREAMING_CHECK(buffer_pool->GetBuffer(size, &buffer) == StreamingStatus::OK);
  }
  std::memcpy(buffer.Data(), data, size);
  return ray::streaming::MemoryBuffer(buffer.Data(), size);
}

ray::streaming::MemoryBuffer CopyToBufferPool(std::shared_ptr<BufferPool> buffer_pool,
                                              uint8_t *data, uint32_t size) {
  ray::streaming::MemoryBuffer buffer;
  STREAMING_CHECK(buffer_pool->GetBuffer(size, &buffer) == StreamingStatus::OK);
  while (buffer.Size() < size) {
    STREAMING_LOG(INFO) << "can't get enough buffer to write, need " << size
                        << ", wait 1 ms, pool usage " << buffer_pool->PrintUsage();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    STREAMING_CHECK(buffer_pool->GetBuffer(size, &buffer) == StreamingStatus::OK);
  }
  std::memcpy(buffer.Data(), data, size);
  buffer_pool->MarkUsed(buffer.Data(), size);
  return ray::streaming::MemoryBuffer(buffer.Data(), size);
}

StreamingMessagePtr MakeMessagePtr(
    const uint8_t *data, uint32_t size, uint64_t seq_id,
    StreamingMessageType msg_type = StreamingMessageType::Message) {
  auto buf = new uint8_t[size];
  std::memcpy(buf, data, size);
  auto msg = std::make_shared<StreamingMessage>(buf, size, seq_id, msg_type);
  delete[] buf;
  return msg;
}

std::shared_ptr<ray::RayFunction> MockFunction() {
  return std::make_shared<ray::RayFunction>(ray::Language::PYTHON,
                                            ray::FunctionDescriptorBuilder::FromVector(
                                                ray::Language::PYTHON, {"", "", "", ""}));
}

int GenerateRandomPort() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> random_gen{2000, 2009};
  return random_gen(gen);
}
}  // namespace util
}  // namespace streaming
}  // namespace ray
