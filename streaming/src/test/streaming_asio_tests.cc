#include <cstring>
#include <string>

#include "gtest/gtest.h"
#include "streaming.h"
#include "streaming_asio.h"
#include "test/test_utils.h"

using namespace ray::streaming;
using namespace ray;

#ifdef USE_PANGU
TEST(StreamingAsioTest, streaming_asio_pangu_writer_test) {
  std::string output_file("/zdfs_test/bundle_file");
  output_file += std::to_string(current_time_ms());
  std::list<StreamingMessagePtr> message_list;
  for (int i = 0; i < 5; ++i) {
    uint8_t *data = new uint8_t[i + 1];
    std::memset(data, i, i + 1);
    StreamingMessagePtr message =
        util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);

    message_list.push_back(message);
    delete[] data;
  }
  StreamingMessageBundlePtr message_bundle(new StreamingMessageBundle(
      message_list, 0, 1, 100, StreamingMessageBundleType::Bundle));
  StreamingPanguFileSystem::Init();
  StreamingPanguFileSystem local_writer(output_file);
  std::shared_ptr<uint8_t> writer_ptr(new uint8_t[message_bundle->ClassBytesSize()],
                                      std::default_delete<uint8_t[]>());
  message_bundle->ToBytes(writer_ptr.get());
  local_writer.Open();
  local_writer.Write(writer_ptr.get(), message_bundle->ClassBytesSize());
  local_writer.Close();

  StreamingPanguFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr(new uint8_t[message_bundle->ClassBytesSize()],
                               std::default_delete<uint8_t[]>());
  std::shared_ptr<uint8_t> de_ptr(new uint8_t[message_bundle->ClassBytesSize()],
                                  std::default_delete<uint8_t[]>());

  local_reader.Open();
  local_reader.Read(ptr.get(), message_bundle->ClassBytesSize());
  local_reader.Close();

  message_bundle->ToBytes(de_ptr.get());
  // remove dump file
  local_reader.Delete(output_file);
  StreamingPanguFileSystem::Destory();
  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(ptr.get(),
                                                     message_bundle->ClassBytesSize());
  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(de_ptr.get(),
                                                     message_bundle->ClassBytesSize());
  EXPECT_EQ(std::memcmp(de_ptr.get(), ptr.get(), message_bundle->ClassBytesSize()), 0);
}

TEST(StreamingAsioTest, streaming_asio_pangu_read_all_test) {
  std::string output_file("/zdfs_test/pangu_test_file");
  output_file += std::to_string(current_time_ms());
  STREAMING_LOG(INFO) << "pangu read all test";
  StreamingPanguFileSystem::Init();
  StreamingPanguFileSystem local_file(output_file, true);
  local_file.Delete();

  size_t bytes_size = 100;
  std::shared_ptr<uint8_t> write_bytes(new uint8_t[bytes_size]);
  std::memset(write_bytes.get(), bytes_size, 1);

  StreamingPanguFileSystem local_writer(output_file);
  local_writer.Open();
  local_writer.Write(write_bytes.get(), bytes_size);
  local_writer.Close();

  StreamingPanguFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr;
  local_reader.Open();
  uint32_t data_size = 0;
  local_reader.ReadAll(&ptr, data_size);
  local_reader.Close();

  local_reader.Delete(output_file);
  StreamingPanguFileSystem::Destory();
  EXPECT_EQ(std::memcmp(write_bytes.get(), ptr.get(), bytes_size), 0);
  EXPECT_EQ(data_size, bytes_size);
}
#endif

TEST(StreamingAsioTest, streaming_asio_writer_test) {
  std::string output_file("/tmp/bundle_file");
  std::list<StreamingMessagePtr> message_list;
  for (int i = 0; i < 5; ++i) {
    uint8_t *data = new uint8_t[i + 1];
    std::memset(data, i, i + 1);
    StreamingMessagePtr message =
        util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);
    message_list.push_back(message);
    delete[] data;
  }
  StreamingMessageBundlePtr message_bundle(new StreamingMessageBundle(
      message_list, 0, 1, 100, StreamingMessageBundleType::Bundle));

  StreamingLocalFileSystem local_writer(output_file);
  std::shared_ptr<uint8_t> writer_ptr(new uint8_t[message_bundle->ClassBytesSize()],
                                      std::default_delete<uint8_t[]>());
  message_bundle->ToBytes(writer_ptr.get());
  local_writer.Open();
  local_writer.Write(writer_ptr.get(), message_bundle->ClassBytesSize());
  local_writer.Close();

  StreamingLocalFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr(new uint8_t[message_bundle->ClassBytesSize()],
                               std::default_delete<uint8_t[]>());
  std::shared_ptr<uint8_t> de_ptr(new uint8_t[message_bundle->ClassBytesSize()],
                                  std::default_delete<uint8_t[]>());

  local_reader.Open();
  local_reader.Read(ptr.get(), message_bundle->ClassBytesSize());
  local_reader.Close();

  message_bundle->ToBytes(de_ptr.get());
  // remove dump file
  STREAMING_CHECK(local_reader.Delete());

  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(ptr.get(),
                                                     message_bundle->ClassBytesSize());
  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(de_ptr.get(),
                                                     message_bundle->ClassBytesSize());
  EXPECT_EQ(std::memcmp(de_ptr.get(), ptr.get(), message_bundle->ClassBytesSize()), 0);
}

TEST(StreamingAsioTest, streaming_asio_read_all_test) {
  std::string output_file("/tmp/bundle_file");

  size_t bytes_size = 100;
  std::shared_ptr<uint8_t> write_bytes(new uint8_t[bytes_size]);
  std::memset(write_bytes.get(), bytes_size, 1);

  StreamingLocalFileSystem local_writer(output_file);
  local_writer.Open();
  local_writer.Write(write_bytes.get(), bytes_size);
  local_writer.Close();

  StreamingLocalFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr;
  local_reader.Open();
  uint32_t data_size = 0;
  local_reader.ReadAll(&ptr, data_size);
  local_reader.Close();

  // remove dump file
  STREAMING_CHECK(local_reader.Delete());

  EXPECT_EQ(std::memcmp(write_bytes.get(), ptr.get(), bytes_size), 0);
  EXPECT_EQ(bytes_size, data_size);
}

TEST(StreamingAsioTest, streaming_asio_directory_test) {
  std::string dir_name = "/tmp/test_asio_dir/";
  std::string sub_dir_name = "test/";
  std::string sub_dir_new_name = "newtest/";
  StreamingLocalFileSystem dir(dir_name);
  EXPECT_EQ(false, dir.IsDirectory());
  EXPECT_EQ(true, dir.CreateDirectory());
  EXPECT_EQ(true, dir.IsDirectory());
  EXPECT_EQ(false, dir.ExistsSubDirectory(sub_dir_name));
  EXPECT_EQ(true, dir.CreateSubDirectory(sub_dir_name));
  EXPECT_EQ(true, dir.ExistsSubDirectory(sub_dir_name));
  EXPECT_EQ(false, dir.ExistsSubDirectory(sub_dir_new_name));
  EXPECT_EQ(true, dir.RenameSubDirectory(sub_dir_name, sub_dir_new_name));
  EXPECT_EQ(true, dir.ExistsSubDirectory(sub_dir_new_name));
  EXPECT_EQ(true, dir.DeleteSubDirectory(sub_dir_new_name));
  EXPECT_EQ(false, dir.ExistsSubDirectory(sub_dir_new_name));
  EXPECT_EQ(true, dir.IsDirectory());
  EXPECT_EQ(true, dir.DeleteDirectory());
  EXPECT_EQ(false, dir.IsDirectory());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
