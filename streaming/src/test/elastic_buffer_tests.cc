#include "elasticbuffer/elastic_buffer.h"
#include "gtest/gtest.h"
#include "message/message.h"
#include "queue/queue_item.h"
#include "util/streaming_asio.h"

using namespace ray::streaming;
using namespace ray;

struct TestDU {
  uint32_t size;
  uint8_t *data = nullptr;
};

std::shared_ptr<uint8_t> standard_data;

TEST(ELASTIC_BUFFER, test_local_file_clear) {
  ElasticBuffer<TestDU>::BufferSerilizeFunction ac_func = [](TestDU &ptr) {
    DataUnit du;
    du.size = ptr.size;
    du.data = ptr.data;
    return du;
  };

  ElasticBuffer<TestDU>::BufferDeserilizedFunction deser_func = [](DataUnit &ptr) {
    std::shared_ptr<TestDU> du(new TestDU());
    du->size = ptr.size;
    du->data = ptr.data;
    return du;
  };

  ElasticBuffer<TestDU>::BufferReleaseFunction rel_func = [](TestDU &ptr) {
    if (nullptr != ptr.data) {
      delete[] ptr.data;
    }
  };

  std::vector<ObjectID> id_vec;
  for (uint32_t i = 0; i < 10; i++) {
    id_vec.push_back(ObjectID::FromRandom());
  }
  std::vector<uint64_t> id_index_vec(id_vec.size(), 0);
  ElasticBuffer<TestDU> es_buffer(id_vec, id_index_vec, ac_func, deser_func, rel_func);
  std::string dir = es_buffer.GetElasticBufferConfig().file_directory;
  uint8_t data[100];
  memset(data, -1, sizeof(100));
  StreamingLocalFileSystem dir_manager(dir);
  dir_manager.CreateDirectory();
  for (uint32_t i = 0; i < 10; i++) {
    dir_manager.CreateSubDirectory(id_vec[i].Hex() + "/");
    for (uint32_t j = 0; j < 10; j++) {
      std::string name = dir + id_vec[i].Hex() + "/" + std::to_string(j);
      StreamingLocalFileSystem file_manager(name);
      file_manager.Open();
      file_manager.Write(data, 100);
      file_manager.Close();
    }
  }
  for (uint32_t i = 0; i < 10; i++) {
    es_buffer.MakeSubDirectory(id_vec[i].Hex());
  }
}

TEST(ELASTIC_BUFFER, elastic_buffer) {
  ElasticBuffer<TestDU>::BufferSerilizeFunction ac_func = [](TestDU &ptr) {
    DataUnit du;
    du.size = ptr.size;
    du.data = ptr.data;
    return du;
  };

  ElasticBuffer<TestDU>::BufferDeserilizedFunction deser_func = [](DataUnit &ptr) {
    std::shared_ptr<TestDU> du(new TestDU());
    du->size = ptr.size;
    du->data = ptr.data;
    return du;
  };

  ElasticBuffer<TestDU>::BufferReleaseFunction rel_func = [](TestDU &ptr) {
    if (nullptr != ptr.data) {
      delete[] ptr.data;
    }
  };

  std::vector<ObjectID> id_vec;
  id_vec.push_back(ObjectID::FromRandom());
  id_vec.push_back(ObjectID::FromRandom());
  std::vector<uint64_t> id_index_vec(id_vec.size(), 0);
  ElasticBuffer<TestDU> es_buffer(id_vec, id_index_vec, ac_func, deser_func, rel_func);
  for (int i = 0; i < 100000; ++i) {
    TestDU du;
    du.size = (i / id_vec.size() + 1) % 128;
    du.data = new uint8_t[du.size];
    std::memcpy(du.data, standard_data.get(), du.size);
    es_buffer.Put(id_vec[i % id_vec.size()], i / id_vec.size() + 1, du);
  }
  es_buffer.Clear(id_vec[0], 2000);
  es_buffer.Clear(id_vec[1], 2000);
  es_buffer.Clear(id_vec[0], 30000);
  es_buffer.Clear(id_vec[1], 40000);
  es_buffer.BeginRecovery();
  for (uint64_t i = 30001; i < 50000; ++i) {
    uint64_t f_index = i;
    std::shared_ptr<TestDU> du;
    es_buffer.Find(id_vec[0], f_index, du);
    EXPECT_TRUE(du->size == f_index % 128 &&
                std::memcmp(du->data, standard_data.get(), du->size) == 0);
  }
  es_buffer.EndRecovery();

  for (int i = 100000; i < 200000; ++i) {
    TestDU du;
    du.size = (i / id_vec.size() + 1) % 128;
    du.data = new uint8_t[du.size];
    std::memcpy(du.data, standard_data.get(), du.size);
    es_buffer.Put(id_vec[i % id_vec.size()], i / id_vec.size() + 1, du);
  }

  es_buffer.BeginRecovery();
  for (uint64_t i = 60001; i < 100000; ++i) {
    uint64_t f_index = i;
    std::shared_ptr<TestDU> du;
    es_buffer.Find(id_vec[1], f_index, du);
    EXPECT_TRUE(du->size == f_index % 128 &&
                std::memcmp(du->data, standard_data.get(), du->size) == 0);
  }
  es_buffer.EndRecovery();
}

ElasticBuffer<QueueItem>::BufferSerilizeFunction common_ac_func = [](QueueItem &ptr) {
  DataUnit du;
  du.size = ptr.SerializeSize();
  du.data = new uint8_t[du.size];
  du.use_copy = true;
  ptr.ToSerialize(du.data);
  return du;
};

ElasticBuffer<QueueItem>::BufferDeserilizedFunction common_deser_func =
    [](DataUnit &ptr) {
      std::shared_ptr<QueueItem> item(new QueueItem(ptr.data, ptr.size));
      return item;
    };

class ElasticBufferTest : public testing::Test {
 protected:
  void SetUp() {
    id_vec.clear();
    es_buffer.reset();
    id_vec.push_back(ObjectID::FromRandom());
    id_vec.push_back(ObjectID::FromRandom());
    std::vector<uint64_t> id_index_vec(id_vec.size(), 0);
    es_buffer = std::make_shared<ElasticBuffer<QueueItem>>(
        id_vec, id_index_vec, common_ac_func, common_deser_func);
  }

  void ResetEsConfig(ElasticBufferConfig &es_config,
                     ElasticBufferChannelConfig &channel_config) {
    std::vector<ElasticBufferChannelConfig> channel_configs(id_vec.size(),
                                                            channel_config);
    std::vector<uint64_t> id_index_vec(id_vec.size(), 0);
    es_buffer = std::make_shared<ElasticBuffer<QueueItem>>(
        es_config, channel_configs, id_vec, id_index_vec, common_ac_func,
        common_deser_func);
  }

  // Put item in elastic buffer util it's full.
  int PutData(int from, int to, int channel_index) {
    for (int i = from; i < to; ++i) {
      uint32_t index = i + 1;
      /// +1 to avoid data_size is 0
      uint32_t data_size = (index) % 128 + 1;
      auto buffer = std::make_shared<ray::streaming::LocalMemoryBuffer>(
          standard_data.get(), data_size, true);
      QueueItem item(index, nullptr, 0, buffer->Data(), data_size, 0, 0, index, index,
                     true);
      if (!es_buffer->Put(id_vec[channel_index], index, item)) {
        return i;
      }
    }
    return to;
  }
  void GetAndTest(int from, int to, int channel_index) {
    for (int i = from; i < to; ++i) {
      std::shared_ptr<QueueItem> item;
      es_buffer->Find(id_vec[channel_index], i, item);
      uint32_t index = i;
      EXPECT_TRUE(item->DataSize() == index % 128 + 1 &&
                  std::memcmp(item->Buffer()->Data(), standard_data.get(),
                              item->DataSize()) == 0);
      EXPECT_TRUE(item->SeqId() == index && item->MsgIdStart() == index &&
                  item->MsgIdEnd() == index);
    }
  }

 protected:
  std::vector<ObjectID> id_vec;
  std::shared_ptr<ElasticBuffer<QueueItem>> es_buffer;
};

TEST_F(ElasticBufferTest, in_queue_delete_queue) {
  PutData(0, 800, 0);
  es_buffer->Clear(id_vec[0], 500);
  es_buffer->BeginRecovery();
  GetAndTest(501, 800, 0);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, in_cache_delete_queue) {
  PutData(0, 2000, 0);
  es_buffer->Clear(id_vec[0], 1500);
  es_buffer->BeginRecovery();
  GetAndTest(1501, 2000, 0);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, in_file_delete_queue) {
  PutData(0, 40000, 0);
  es_buffer->Clear(id_vec[0], 39500);
  es_buffer->BeginRecovery();
  GetAndTest(39501, 40000, 0);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, in_cache_delete_cache) {
  PutData(0, 2000, 0);
  es_buffer->Clear(id_vec[0], 500);
  es_buffer->BeginRecovery();
  GetAndTest(501, 2000, 0);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, in_file_delete_cache) {
  PutData(0, 40000, 0);
  es_buffer->Clear(id_vec[0], 39000);
  es_buffer->BeginRecovery();
  GetAndTest(39001, 40000, 0);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, in_file_delete_file) {
  PutData(0, 40000, 0);
  es_buffer->Clear(id_vec[0], 10000);
  es_buffer->BeginRecovery();
  GetAndTest(10001, 40000, 0);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, elastic_buffer_for_queue_item) {
  PutData(0, 50000, 0);
  PutData(0, 50000, 1);
  es_buffer->Clear(id_vec[0], 2000);
  es_buffer->Clear(id_vec[1], 2000);
  es_buffer->Clear(id_vec[0], 30000);
  es_buffer->Clear(id_vec[1], 40000);
  es_buffer->BeginRecovery();
  GetAndTest(30001, 50000, 0);
  es_buffer->EndRecovery();

  PutData(50000, 100000, 0);
  PutData(50000, 100000, 1);
  es_buffer->Clear(id_vec[0], 55000);
  es_buffer->Clear(id_vec[1], 60000);
  es_buffer->BeginRecovery();
  GetAndTest(56000, 100000, 0);
  GetAndTest(60001, 100000, 1);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, elastic_buffer_for_rescale) {
  PutData(0, 50000, 0);
  PutData(0, 50000, 1);
  es_buffer->Clear(id_vec[0], 2000);
  es_buffer->Clear(id_vec[1], 2000);
  es_buffer->Clear(id_vec[0], 30000);
  es_buffer->Clear(id_vec[1], 40000);
  es_buffer->BeginRecovery();
  GetAndTest(30001, 50000, 0);
  es_buffer->EndRecovery();

  id_vec.push_back(ObjectID::FromRandom());
  std::vector<ObjectID> added_vec;
  added_vec.push_back(id_vec.back());
  std::vector<uint64_t> id_index_vec(added_vec.size(), 0);
  es_buffer->AddChannels(added_vec, id_index_vec);

  PutData(50000, 100000, 0);
  PutData(50000, 100000, 1);
  PutData(0, 50000, 2);
  es_buffer->Clear(id_vec[0], 55000);
  es_buffer->Clear(id_vec[1], 60000);
  es_buffer->Clear(id_vec[2], 15000);
  es_buffer->BeginRecovery();
  GetAndTest(55001, 100000, 0);
  GetAndTest(60001, 100000, 1);
  GetAndTest(16500, 50000, 2);
  es_buffer->EndRecovery();

  std::vector<ObjectID> removed_vec;
  removed_vec.push_back(id_vec[0]);
  es_buffer->RemoveChannels(removed_vec);

  id_vec.erase(id_vec.begin(), id_vec.begin() + 1);

  es_buffer->BeginRecovery();
  GetAndTest(60001, 100000, 0);
  GetAndTest(16500, 50000, 1);
  es_buffer->EndRecovery();
}

TEST_F(ElasticBufferTest, elastic_buffer_full) {
  ElasticBufferConfig es_config;
  es_config.enable = true;
  ElasticBufferChannelConfig channel_config{1};
  ResetEsConfig(es_config, channel_config);
  int seq_id = PutData(0, 50000, 0);
  STREAMING_LOG(INFO) << "Elastic buffer is full when put seq id " << seq_id;
  EXPECT_TRUE(PutData(seq_id, seq_id + 1, 0) == seq_id);
}

TEST_F(ElasticBufferTest, channel_config) {
  ElasticBufferConfig es_config;
  es_config.enable = true;
  ElasticBufferChannelConfig channel_config{1};
  const ObjectID id = ObjectID::FromRandom();
  id_vec.push_back(id);
  es_buffer->AddChannel(id, 0, channel_config);
  int channel_index = id_vec.size() - 1;
  int seq_id = PutData(0, 50000, channel_index);
  STREAMING_LOG(INFO) << "Elastic buffer is full when put seq id " << seq_id;
  EXPECT_TRUE(PutData(seq_id, seq_id + 1, channel_index) == seq_id);
}

int main(int argc, char **argv) {
  standard_data.reset(new uint8_t[0xff]);
  for (int i = 0; i < 0xff; ++i) {
    *(standard_data.get() + i) = static_cast<uint8_t>(i & 0x7f);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
