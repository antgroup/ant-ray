#include "buffer_pool/buffer_pool.h"
#include "gtest/gtest.h"

using namespace ray::streaming;

TEST(BUFFER_POOL_TEST, GetBufferTest) {
  uint64_t min_buffer_size = 1024 * 1024;  // 1M
  int buffer_nums = 5;
  uint64_t pool_size = buffer_nums * min_buffer_size;
  BufferPool pool(pool_size, min_buffer_size);
  for (int i = 0; i < buffer_nums; ++i) {
    ray::streaming::MemoryBuffer buffer{};
    ASSERT_EQ(pool.GetBuffer(&buffer), StreamingStatus::OK);
    EXPECT_EQ(buffer.Size(), min_buffer_size);
    std::cout << "address: " << static_cast<void *>(buffer.Data()) << std::endl;
    ASSERT_EQ(pool.MarkUsed(buffer.Data(), buffer.Size()), StreamingStatus::OK);
  }
}

TEST(BUFFER_POOL_TEST, ExternalGetBufferTest) {
  uint64_t pool_size = 1024 * 1024;
  uint8_t *external_buffer = (uint8_t *)malloc(pool_size);
  BufferPool pool(external_buffer, pool_size);
  ray::streaming::MemoryBuffer buffer{};
  ASSERT_EQ(pool.GetBuffer(&buffer), StreamingStatus::OK);
  EXPECT_EQ(buffer.Size(), pool_size);
  std::cout << "address: " << static_cast<void *>(buffer.Data()) << std::endl;
  ASSERT_EQ(pool.MarkUsed(buffer.Data(), buffer.Size()), StreamingStatus::OK);
}

TEST(BUFFER_POOL_TEST, ReleaseBuffer) {
  uint64_t min_buffer_size = 1024 * 1024;  // 1M
  int buffer_nums = 5;
  uint64_t pool_size = buffer_nums * min_buffer_size;
  BufferPool pool(pool_size, min_buffer_size);
  std::vector<ray::streaming::MemoryBuffer> buffers;
  for (int i = 0; i < buffer_nums; ++i) {
    ray::streaming::MemoryBuffer buffer{};
    ASSERT_EQ(pool.GetBuffer(&buffer), StreamingStatus::OK);
    EXPECT_EQ(buffer.Size(), min_buffer_size);
    std::cout << "buffer start: " << reinterpret_cast<uint64_t>(buffer.Data())
              << std::endl;
    ASSERT_EQ(pool.MarkUsed(buffer.Data(), buffer.Size()), StreamingStatus::OK);
    buffers.push_back(buffer);
  }
  for (auto &buffer : buffers) {
    // std::cout << "usage: " << pool.PrintUsage() << std::endl;
    ASSERT_EQ(pool.Release(buffer.Data(), buffer.Size()), StreamingStatus::OK);
  }
}

TEST(BUFFER_POOL_TEST, CompositeBufferTest) {
  uint64_t min_buffer_size = 1024 * 1024;  // 1M
  int buffer_nums = 5;
  uint64_t pool_size = buffer_nums * min_buffer_size;
  BufferPool pool(pool_size, min_buffer_size);
  CompositeBuffer composite_buffer;
  for (int i = 0; i < buffer_nums; ++i) {
    ray::streaming::MemoryBuffer buffer;
    ASSERT_EQ(pool.GetBuffer(&buffer), StreamingStatus::OK);
    EXPECT_EQ(buffer.Size(), min_buffer_size);
    std::cout << "address: " << static_cast<void *>(buffer.Data()) << std::endl;
    ASSERT_EQ(pool.MarkUsed(buffer.Data(), buffer.Size()), StreamingStatus::OK);
    composite_buffer.AddBuffer(buffer);
  }
  ASSERT_EQ(composite_buffer.BufferCount(), 5);
  composite_buffer.Data();
  STREAMING_LOG(INFO) << "DataSize: " << composite_buffer.DataSize();
}

TEST(BUFFER_POOL_TEST, BufferMarkedTest) {
  uint64_t buffer_pool_size = 4 * 1000 * 1000;
  uint64_t min_buffer_size = 1024 * 1024;
  BufferPool pool(buffer_pool_size, min_buffer_size);
  ray::streaming::MemoryBuffer small_buffer;
  ray::streaming::MemoryBuffer buffer1;
  ray::streaming::MemoryBuffer buffer2;
  ray::streaming::MemoryBuffer buffer3;
  ray::streaming::MemoryBuffer buffer4;
  ASSERT_EQ(pool.GetBufferBlockedTimeout(800 * 1024, &buffer1, 100), StreamingStatus::OK);
  ASSERT_EQ(pool.MarkUsed(buffer1.Data(), 800 * 1024), StreamingStatus::OK);
  STREAMING_LOG(INFO) << pool.PrintUsage();

  /// Mock get little buffer in Java.
  pool.GetBufferBlockedTimeout(1, &small_buffer, 100);

  ASSERT_EQ(pool.GetBufferBlockedTimeout(800 * 1024, &buffer2, 100), StreamingStatus::OK);
  ASSERT_EQ(pool.MarkUsed(buffer2.Data(), 800 * 1024), StreamingStatus::OK);
  STREAMING_LOG(INFO) << pool.PrintUsage();

  pool.GetBufferBlockedTimeout(1, &small_buffer, 100);
  ASSERT_EQ(pool.GetBufferBlockedTimeout(800 * 1024, &buffer3, 100), StreamingStatus::OK);
  ASSERT_EQ(pool.MarkUsed(buffer3.Data(), 800 * 1024), StreamingStatus::OK);
  pool.GetBufferBlockedTimeout(1, &small_buffer, 100);
  STREAMING_LOG(INFO) << pool.PrintUsage();

  ASSERT_EQ(pool.Release(buffer1.Data(), 800 * 1024), StreamingStatus::OK);
  ASSERT_EQ(pool.Release(buffer2.Data(), 800 * 1024), StreamingStatus::OK);
  ASSERT_EQ(pool.Release(buffer3.Data(), 800 * 1024), StreamingStatus::OK);
  STREAMING_LOG(INFO) << pool.PrintUsage();

  /// Allocate a large buffer.
  ASSERT_EQ(pool.GetBufferBlockedTimeout(2 * 1000 * 1000, &buffer4, 100),
            StreamingStatus::OK);
  ASSERT_EQ(pool.MarkUsed(buffer4.Data(), 800 * 1024), StreamingStatus::OK);
  STREAMING_LOG(INFO) << pool.PrintUsage();
}

TEST(BUFFER_POOL_TEST, LargeBufferTest) {
  uint64_t buffer_pool_size = 4 * 1024 * 1024;
  uint64_t min_buffer_size = 1024 * 1024;
  BufferPool pool(buffer_pool_size, min_buffer_size);
  ray::streaming::MemoryBuffer buffer;
  ASSERT_EQ(pool.GetBufferBlockedTimeout(5 * 1024 * 1024, &buffer, 100),
            StreamingStatus::OutOfMemory);
  pool.GetDisposableBuffer(5 * 1024 * 1024, &buffer);
  ASSERT_EQ(pool.MarkUsed(buffer.Data(), 5 * 1024 * 1024), StreamingStatus::OK);
  ASSERT_EQ(pool.Release(buffer.Data(), 5 * 1024 * 1024), StreamingStatus::OK);
}

TEST(BUFFER_POOL_TEST, GetMinBufferOverfitTest) {
  uint64_t min_buffer_size = 10 * 1024 * 1024;  // 10M
  uint64_t fit_buffer_size = 10 * 1024 * 800;   // 8M-
  int buffer_nums = 10;
  uint64_t pool_size = buffer_nums * min_buffer_size;
  BufferPool pool(pool_size, min_buffer_size);
  for (int i = 0; i < buffer_nums; ++i) {
    auto buffer = std::make_shared<MemoryBuffer>();
    pool.GetBuffer(fit_buffer_size, &(*buffer));
    auto st = pool.GetBufferBlockedTimeout(2 * min_buffer_size, &(*buffer), 100);
    EXPECT_EQ(st, StreamingStatus::OK);
    EXPECT_EQ(buffer->Size(), 2 * min_buffer_size);
    ASSERT_EQ(pool.MarkUsed(buffer->Data(), 2 * min_buffer_size), StreamingStatus::OK);
    ASSERT_EQ(pool.Release(buffer->Data(), 2 * min_buffer_size), StreamingStatus::OK);
  }
}

int main(int argc, char **argv) {
  // StreamingLog::StartStreamingLog(StreamingLogLevel::TRACE);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
