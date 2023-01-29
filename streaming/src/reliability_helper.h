#ifndef RAY_STREAMING_RELIABILITY_HELPER_H
#define RAY_STREAMING_RELIABILITY_HELPER_H
#include "barrier_helper.h"
#include "channel.h"
#include "config.h"
#include "data_reader.h"
#include "data_writer.h"
#include "persistence.h"

namespace ray {
namespace streaming {

class ReliabilityHelper;
class DataWriter;
class DataReader;

class ReliabilityHelperFactory {
 public:
  static std::shared_ptr<ReliabilityHelper> GenReliabilityHelper(
      StreamingConfig &config, StreamingBarrierHelper &barrier_helper, DataWriter *writer,
      DataReader *reader);
};

class ReliabilityHelper {
 public:
  ReliabilityHelper(StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  virtual ~ReliabilityHelper() = default;
  // Only exactly same need override this function.
  virtual void Reload();
  // Store bundle meta or skip in replay mode.
  virtual bool StoreBundleMeta(ProducerChannelInfo &channel_info,
                               StreamingMessageBundlePtr &bundle_ptr,
                               bool is_replay = false);
  virtual void CleanupCheckpoint(ProducerChannelInfo &channel_info, uint64_t barrier_id);
  // Filter message by different failover strategies.
  virtual bool FilterMessage(ProducerChannelInfo &channel_info, const uint8_t *data,
                             StreamingMessageType message_type,
                             uint64_t *write_message_id);
  virtual StreamingStatus InitChannelMerger(uint32_t timeout);
  virtual StreamingStatus StashNextMessage(
      std::shared_ptr<StreamingReaderBundle> &message, uint32_t timeout_ms);
  virtual StreamingStatus HandleNoValidItem(ConsumerChannelInfo &channel_info);

 protected:
  StreamingConfig &config_;
  StreamingBarrierHelper &barrier_helper_;
  DataWriter *writer_;
  DataReader *reader_;
};

class AtLeastOnceHelper : public ReliabilityHelper {
 public:
  AtLeastOnceHelper(StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  StreamingStatus InitChannelMerger(uint32_t timeout) override;
  StreamingStatus StashNextMessage(std::shared_ptr<StreamingReaderBundle> &message,
                                   uint32_t timeout_ms) override;
  StreamingStatus HandleNoValidItem(ConsumerChannelInfo &channel_info) override;
};

class ExactlyOnceHelper : public ReliabilityHelper {
 public:
  ExactlyOnceHelper(StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  bool FilterMessage(ProducerChannelInfo &channel_info, const uint8_t *data,
                     StreamingMessageType message_type,
                     uint64_t *write_message_id) override;
  virtual ~ExactlyOnceHelper() = default;
};

class ExactlySameHelper : public ReliabilityHelper {
 public:
  ExactlySameHelper(StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  virtual ~ExactlySameHelper() = default;
  void Reload() override;
  bool StoreBundleMeta(ProducerChannelInfo &channel_info,
                       StreamingMessageBundlePtr &bundle_ptr,
                       bool is_replay = false) override;
  void CleanupCheckpoint(ProducerChannelInfo &channel_info, uint64_t barrier_id) override;

 private:
  // Reload meta info list from outside storage.
  void ReloadByQueueId(ProducerChannelInfo &channel_info,
                       std::vector<StreamingMessageBundleMetaPtr> &meta_vec);
  // Resend message bundles according reloaded meta information.
  void ReplayByQueueId(ProducerChannelInfo &channel_info,
                       StreamingMessageList &message_list_fusion,
                       std::vector<StreamingMessageBundleMetaPtr> &meta_vec);

 private:
  std::shared_ptr<StreamingPersistence> persistence_helper_;
  static constexpr uint32_t kMaxThreadPoolNum = 8;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_RELIABILITY_H
