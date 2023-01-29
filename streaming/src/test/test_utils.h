
#ifndef RAY_TEST_UTILS_H
#define RAY_TEST_UTILS_H

#include <unistd.h>

#include "data_writer.h"
#include "gtest/gtest.h"
#include "message.h"
#include "streaming.h"

namespace ray {
namespace streaming {

namespace util {

ray::streaming::MemoryBuffer ToMessageBuffer(DataWriter *writer, const ray::ObjectID &qid,
                                             uint8_t *data, uint32_t size);

ray::streaming::MemoryBuffer CopyToBufferPool(std::shared_ptr<BufferPool> buffer_pool,
                                              uint8_t *data, uint32_t size);

StreamingMessagePtr MakeMessagePtr(const uint8_t *, uint32_t, uint64_t,
                                   StreamingMessageType);

std::shared_ptr<ray::RayFunction> MockFunction();

int GenerateRandomPort();
}  // namespace util
}  // namespace streaming
}  // namespace ray

#endif  // RAY_TEST_UTILS_H
