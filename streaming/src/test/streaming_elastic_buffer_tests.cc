#include <memory>
#include "common/status.h"
#include "conf/streaming_config.h"
#include "data_reader.h"
#include "data_writer.h"
#include "elasticbuffer/elastic_buffer.h"
#include "gtest/gtest.h"
#include "message/message.h"
#include "message/message_bundle.h"
#include "persistence.h"
#include "queue/config.h"
#include "queue/queue.h"
#include "queue/queue_item.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/core_worker.h"
#include "ray/util/util.h"
#include "ring_buffer.h"
#include "streaming.h"
#include "test/test_utils.h"
#include "util/utility.h"
using namespace ray;
using namespace ray::streaming;

std::shared_ptr<uint8_t> raw_data;

TEST(ElasticBuffer, construct_destroy) {
  ElasticBuffer<QueueItem>::BufferSerilizeFunction ser_func = [](QueueItem &ptr) {
    DataUnit du;
    du.size = ptr.SerializeSize();
    du.data = new uint8_t[du.size];
    du.use_copy = true;
    ptr.ToSerialize(du.data);
    return du;
  };

  ElasticBuffer<QueueItem>::BufferDeserilizedFunction deser_func = [](DataUnit &ptr) {
    std::shared_ptr<QueueItem> item(new QueueItem(ptr.data, ptr.size));
    return item;
  };

  ElasticBufferConfig config_;
  config_.enable = true;
  config_.file_cache_size = 1 << 20;
  config_.max_save_buffer_size = 1000;
  config_.flush_buffer_size = 800;
  ElasticBufferChannelConfig channel_config_;
  channel_config_.max_file_num = 1000;
  std::shared_ptr<ElasticBuffer<QueueItem>> elasticbuffer_ =
      std::make_shared<ElasticBuffer<QueueItem>>(config_, ser_func, deser_func);

  std::vector<ObjectID> id_vec;

  id_vec.push_back(ObjectID::FromRandom());
  id_vec.push_back(ObjectID::FromRandom());
  id_vec.push_back(ObjectID::FromRandom());

  for (uint32_t i = 0; i < id_vec.size(); ++i) {
    elasticbuffer_->AddChannel(id_vec[i], 0, channel_config_);
  }
  elasticbuffer_->RemoveChannel(id_vec[0]);
  elasticbuffer_ = nullptr;
}

TEST(STREAMINGWRITER_QUEUE, streamingwriter_queue) {
  ElasticBuffer<QueueItem>::BufferSerilizeFunction ser_func = [](QueueItem &ptr) {
    DataUnit du;
    du.size = ptr.SerializeSize();
    du.data = new uint8_t[du.size];
    du.use_copy = true;
    ptr.ToSerialize(du.data);
    return du;
  };

  ElasticBuffer<QueueItem>::BufferDeserilizedFunction deser_func = [](DataUnit &ptr) {
    std::shared_ptr<QueueItem> item(new QueueItem(ptr.data, ptr.size));
    return item;
  };

  ElasticBufferConfig config_;
  config_.enable = true;
  config_.file_cache_size = 1 << 20;
  config_.max_save_buffer_size = 1000;
  config_.flush_buffer_size = 800;
  // config_.max_file_num = 1000;
  std::shared_ptr<ElasticBuffer<QueueItem>> elasticbuffer_ =
      std::make_shared<ElasticBuffer<QueueItem>>(config_, ser_func, deser_func);

  ray::ObjectID queue_id = ray::ObjectID::FromRandom();
  JobID job_id = JobID::FromInt(0);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ray::ActorID actor_id = ray::ActorID::Of(job_id, task_id, 0);
  ray::ActorID peer_actor_id = ray::ActorID::Of(job_id, task_id, 1);
  WriterQueue queue(queue_id, actor_id, peer_actor_id, false, 10 * 1024 * 1024,
                    std::make_shared<MockTransport>(), elasticbuffer_);
  std::shared_ptr<plasma::PlasmaClient> plasmaptr = nullptr;
  queue.CreateInternalBuffer(plasmaptr, 1024);

  uint8_t meta[20];
  uint8_t data[100];
  uint32_t j = 0;

  for (uint32_t i = 1; i <= 5000; ++i) {
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue.GetBufferPool(), data, 100);
    STREAMING_LOG(DEBUG) << "put seqid: " << i
                         << " data address: " << (uint64_t)data_buffer.Data()
                         << " data size: " << data_buffer.Size();
    StreamingStatus status = queue.Push(meta, 20, data_buffer.Data(), data_buffer.Size(),
                                        current_sys_time_ms(), i, i);
    if (status == StreamingStatus::ElasticBufferRemain) {
      queue.SetQueueLimit((i / 10) * 10);
      status = queue.Push(meta, 20, data_buffer.Data(), data_buffer.Size(),
                          current_sys_time_ms(), i, i);
    }

    ++j;
    if (j == 10) {
      queue.MockSend();
      j = 0;
    }
  }
  queue.SetQueueEvictionLimit(2500);
}

TEST(ELASTICBUFFER_BASE, elasticbuffer_base) {
  ElasticBuffer<QueueItem>::BufferSerilizeFunction ser_func = [](QueueItem &ptr) {
    DataUnit du;
    du.size = ptr.SerializeSize();
    STREAMING_LOG(DEBUG) << "dusize " << du.size;
    du.data = new uint8_t[du.size];
    du.use_copy = true;
    ptr.ToSerialize(du.data);
    STREAMING_LOG(DEBUG) << "serialize succeed";
    return du;
  };

  ElasticBuffer<QueueItem>::BufferDeserilizedFunction deser_func = [](DataUnit &ptr) {
    std::shared_ptr<QueueItem> item(new QueueItem(ptr.data, ptr.size));
    return item;
  };
  std::vector<ObjectID> id_vec;
  std::shared_ptr<ElasticBuffer<QueueItem>> es_buffer;

  id_vec.push_back(ObjectID::FromRandom());
  id_vec.push_back(ObjectID::FromRandom());
  id_vec.push_back(ObjectID::FromRandom());
  ElasticBufferConfig config_;
  config_.enable = true;
  config_.file_cache_size = 1 << 20;
  config_.max_save_buffer_size = 1000;
  config_.flush_buffer_size = 800;
  ElasticBufferChannelConfig channel_config;
  channel_config.max_file_num = 1000;
  es_buffer = std::make_shared<ElasticBuffer<QueueItem>>(config_, ser_func, deser_func);
  for (uint32_t i = 0; i < id_vec.size(); ++i) {
    es_buffer->AddChannel(id_vec[i], 0, channel_config);
  }
  STREAMING_LOG(INFO) << "Channel created.";
  for (uint32_t i = 0; i < 5000; ++i) {
    for (uint32_t j = 0; j < id_vec.size(); ++j) {
      uint32_t index = i + 1;
      uint32_t data_size = index % 10 + (j + 1) * 30;
      auto buffer = std::make_shared<ray::streaming::LocalMemoryBuffer>(raw_data.get(),
                                                                        data_size, true);
      QueueItem item(index, nullptr, 0, buffer->Data(), data_size, 0, 0, index, index,
                     true);
      if (!es_buffer->Put(id_vec[j], index, item)) {
        STREAMING_LOG(WARNING) << "Test put data in esbuffer failed seqid: " << i
                               << "queueid" << id_vec[j];
      }
    }
  }
  for (uint32_t i = 0; i < id_vec.size(); ++i) {
    uint32_t index = (i + 1) * 1000;
    es_buffer->Clear(id_vec[i], index);
  }
  es_buffer->BeginRecovery();
  std::shared_ptr<QueueItem> item;
  for (uint32_t j = 0; j < id_vec.size(); ++j) {
    for (uint32_t i = 999; i <= 4000; ++i) {
      uint32_t index = i + 1;
      bool res = es_buffer->Find(id_vec[j], index, item);
      if (index > (j + 1) * 1000) {
        EXPECT_TRUE(
            item->DataSize() == index % 10 + (j + 1) * 30 &&
            std::memcmp(item->Buffer()->Data(), raw_data.get(), item->DataSize()) == 0);
        STREAMING_LOG(DEBUG) << index << " " << j << " item msgid " << item->MsgIdStart()
                             << " qid: " << item->SeqId() << " " << item->DataSize();
        EXPECT_TRUE(item->SeqId() == index && item->MsgIdStart() == index &&
                    item->MsgIdEnd() == index);
      } else {
        EXPECT_FALSE(res);
      }
    }
  }
  es_buffer->EndRecovery();
}

int main(int argc, char **argv) {
  raw_data.reset(new uint8_t[0xff]);
  for (int i = 0; i < 0xff; ++i) {
    *(raw_data.get() + i) = static_cast<uint8_t>(i & 0x7f);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
