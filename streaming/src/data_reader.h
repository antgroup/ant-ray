#ifndef RAY_STREAMING_READER_H
#define RAY_STREAMING_READER_H

#include <cstdlib>
#include <functional>
#include <ostream>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "dynamic_step.h"
#include "http_profiler.h"
#include "message_bundle.h"
#include "priority_queue.h"
#include "queue/receiver.h"
#include "reliability/barrier_helper.h"
#include "rescale/rescale.h"
#include "runtime_context.h"
#include "streaming_config.h"

namespace ray {
namespace streaming {
class ReliabilityHelper;
class AtLeastOnceHelper;

enum class BundleCheckStatus : uint32_t {
  OkBundle = 0,
  BundleToBeThrown = 1,
  BundleToBeSplit = 2
};

static inline std::ostream &operator<<(std::ostream &os,
                                       const BundleCheckStatus &status) {
  os << static_cast<std::underlying_type<BundleCheckStatus>::type>(status);
  return os;
}

struct StreamingReaderMsgPtrComparator {
  explicit StreamingReaderMsgPtrComparator(ReliabilityLevel strategy)
      : comp_strategy(strategy){};
  StreamingReaderMsgPtrComparator(){};
  ReliabilityLevel comp_strategy = ReliabilityLevel::EXACTLY_ONCE;

  bool operator()(const std::shared_ptr<StreamingReaderBundle> &a,
                  const std::shared_ptr<StreamingReaderBundle> &b);
};

class DataReader : public Scalable, public DirectCallReceiver {
 public:
  friend class ReliabilityHelper;
  friend class AtLeastOnceHelper;
  // For http profiler access inner fields.
  friend class HttpProfiler;
  friend class MockReader;

  /// init Streaming reader
  /// \param input_ids[in], input channel id vector
  /// \param init_params[in], init specific parameters
  /// \param streaming_msg_id[in], start message id vector for all channels
  /// \param timer_interval[in], timer interval for fetching data
  /// \param creation_status[out], creation status vector of all channels
  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<StreamingQueueInitialParameter> &init_params,
            const std::vector<uint64_t> &streaming_msg_ids, int64_t timer_interval,
            std::vector<TransferCreationStatus> &creation_status);

  void Init(const std::vector<ObjectID> &input_ids,
            const std::vector<StreamingQueueInitialParameter> &init_params,
            int64_t timer_interval, bool is_rescale = false);
  /*!
   * get latest message from input queues
   * @param timeout_ms
   * @param msg, return the latest message
   */
  StreamingStatus GetBundle(const uint32_t timeout_ms,
                            std::shared_ptr<StreamingReaderBundle> &message);

  /*!
   * get offset infomation
   */
  void GetOffsetInfo(std::unordered_map<ObjectID, ConsumerChannelInfo> *&offset_map);
  /*!
   * notify input queues to clear data before the offset.
   * used when checkpoint is done.
   * @param channel_info consumer channel info
   * @param offset
   */
  void NotifyConsumedItem(ConsumerChannelInfo &channel_info, uint64_t offset);

  void Stop();

  explicit DataReader(std::shared_ptr<RuntimeContext> &runtime_context);

  virtual ~DataReader();

  void NotifyConsumed(std::shared_ptr<StreamingReaderBundle> &message);

  void Rescale(const std::vector<ObjectID> &id_vec,
               const std::vector<ObjectID> &target_id_vec,
               std::vector<StreamingQueueInitialParameter> &actor_handle_vec) override;

  void Rescale(const std::vector<ObjectID> &id_vec,
               std::vector<StreamingQueueInitialParameter> &init_params);

  void ClearPartialCheckpoint(uint64_t global_barrier_id, uint64_t partial_barrier_id);

  void OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) override;
  std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer) override;
  void SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) override;

  StreamingStatus InitChannelMerger(uint32_t timeout_ms);

  const StreamingConfig &GetConfig() const;

  std::string GetProfilingInfo() const;

 protected:
  void ReportTimer();
  void ScaleUp(
      const std::vector<ray::ObjectID> &id_vec,
      std::vector<StreamingQueueInitialParameter> &scale_init_parameter_vec) override;
  void ScaleDown(const std::vector<ray::ObjectID> &id_vec) override;
  void RefreshChannelInfo(ConsumerChannelInfo &channel_info);

  virtual void SplitBundle(std::shared_ptr<StreamingReaderBundle> &message,
                           uint64_t last_msg_id);

 private:
  /// Create all channel transfers for stream setting up.
  /// \param_out creation_status
  StreamingStatus InitChannel(std::vector<TransferCreationStatus> &creation_status);

  StreamingStatus GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                        std::shared_ptr<StreamingReaderBundle> &message,
                                        uint32_t timeout_ms, uint32_t wait_time_ms);

  /// (Case 1)When reader restarted, writer will keep sending bundles until reader request
  /// a pull. During this time, reader will receive messages whose msg_id is larger or
  /// equal than expected, and these messages should not be processed.
  ///
  /// (Case 2)When writer restarted, it will resend all messages from last checkpoint.
  /// reader should throw duplicated messages in exactly once and exactly same mode.
  ///
  /// This function check whether messages in this bundle should be processed
  /// \param message The message to be checked
  /// \return 3 types of result will be returned:
  ///     1. BundleToBeThrown: All messages in this bundle should not be processed.
  ///     2. OkBundle: All messages in this bundle should be processed.
  ///     3. BundleToBeSplit: Part of messages in this bundle should be processed.
  BundleCheckStatus CheckBundle(const std::shared_ptr<StreamingReaderBundle> &message);

  /// Handle barrier message when getting merged bundle msg.
  /// The difference between udc msg and other kinds of barriers is that we need to return
  /// the udc msg immediately regardless of whether aligned or not, but other kinks of
  /// barrier will be returned only when the barrier was aligned.
  void HandleMergedBarrierMessage(std::shared_ptr<StreamingReaderBundle> &message,
                                  bool &is_valid_break, ConsumerChannelInfo &offset_info);

  StreamingStatus GetMergedMessageBundle(std::shared_ptr<StreamingReaderBundle> &message,
                                         bool &is_valid_break, uint32_t timeout_ms,
                                         int64_t start_ts);
  StreamingStatus DestroyChannel(const std::vector<ObjectID> &ids);

  bool BarrierAlign(std::shared_ptr<StreamingReaderBundle> &message);

  void AdvanceChannelInfoMapIter();

  /// To check bundle whether it should be dropped out.
  /// \param[in] message, message bundle fetched from channel
  /// \return
  bool IsValidBundle(std::shared_ptr<StreamingReaderBundle> &message);

  void NotifyStartLogging(uint64_t barrier_id);

  int32_t GetTargetBarrierCount(const std::vector<ObjectID> &queue_ids);

 protected:
  std::shared_ptr<RuntimeContext> runtime_context_;
  std::unordered_map<ObjectID, ConsumerChannelInfo> channel_info_map_;
  std::unordered_map<ObjectID, ConsumerChannelInfo>::iterator channel_info_map_iter_;
  std::shared_ptr<Config> transfer_config_;

 private:
  struct StreamingRescaleRAII {
    DataReader *p_;
    TransferState origin_state_;
    explicit StreamingRescaleRAII(DataReader *p) : p_(p) {
      STREAMING_LOG(INFO) << "reader runner suspended";
      origin_state_ = p_->runtime_context_->transfer_state_;
      {
        AutoSpinLock lock(p_->transfer_changing_);
        p_->runtime_context_->transfer_state_ = TransferState::Rescaling;
      }
      p_->runtime_context_->ShutdownTimer();
    }
    ~StreamingRescaleRAII() {
      STREAMING_LOG(INFO) << "reader runner restart";
      p_->runtime_context_->transfer_state_ = origin_state_;
      p_->runtime_context_->EnableTimer(std::bind(&DataReader::ReportTimer, p_));
    }
  };

 protected:
  std::unordered_map<ObjectID, std::shared_ptr<ConsumerChannel>> channel_map_;
  std::vector<ObjectID> input_queue_ids_;

  std::vector<ObjectID> unready_queue_ids_;

  std::unique_ptr<PriorityQueue<std::shared_ptr<StreamingReaderBundle>,
                                StreamingReaderMsgPtrComparator, ObjectID>>
      reader_merger_;

  std::unordered_map<uint64_t, uint32_t> global_barrier_cnt_;
  std::unordered_map<uint64_t, uint32_t> partial_barrier_cnt_;
  std::unordered_map<uint64_t, uint32_t> endofdata_barrier_cnt_;
  std::unordered_map<uint64_t, uint32_t> udc_msg_barrier_cnt_;
  // Fetch next item from this original channel.
  std::shared_ptr<StreamingReaderBundle> last_fetched_queue_item_;
  // Notify this last item in reader cached item.
  std::shared_ptr<StreamingReaderBundle> reader_cache_item_memory_;
  std::unordered_map<ObjectID, uint64_t> last_message_id_;
  std::unordered_map<ObjectID, uint64_t> initial_message_id_;

  int64_t timer_interval_;
  int64_t last_bundle_ts_;
  int64_t last_message_ts_;
  int64_t last_message_latency_;
  // Last bundle latency from upstream to downstream.
  int64_t last_bundle_latency_;
  int64_t last_bundle_unit_;

  uint64_t last_getbundle_ts_;
  uint64_t last_getbundle_latency_;

  ObjectID last_read_q_id_;
  std::shared_ptr<StreamingDynamicStep> step_udpater;

  static const uint32_t kReadItemTimeout;

  StreamingBarrierHelper barrier_helper_;
  std::shared_ptr<ReliabilityHelper> reliability_helper_;
  // For accessing transfer info thread-safe.
  std::atomic_flag transfer_changing_ = ATOMIC_FLAG_INIT;

  std::shared_ptr<QueueMessageHandler> queue_handler_;
  int32_t target_global_barrier_cnt_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_READER_H
