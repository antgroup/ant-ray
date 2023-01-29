#ifndef RAY_STREAMING_BARRIER_HELPER_H
#define RAY_STREAMING_BARRIER_HELPER_H
#include <queue>
#include <unordered_map>

#include "common/status.h"
#include "ray/common/id.h"

namespace ray {
namespace streaming {
constexpr uint64_t INVALID_BARRIER_ID = std::numeric_limits<uint64_t>::max();
class StreamingBarrierHelper final {
  using BarrierIdQueue = std::shared_ptr<std::queue<uint64_t>>;

 public:
  StreamingBarrierHelper() {}
  /// No duplicated barrier helper should be loaded in data writer or data
  /// reader, so we mark BarrierHelper as a nocopyable object.
  StreamingBarrierHelper(const StreamingBarrierHelper &barrier_helper) = delete;

  StreamingBarrierHelper operator=(const StreamingBarrierHelper &barrier_helper) = delete;

  virtual ~StreamingBarrierHelper() = default;

  /// Get barrier id from queue-barrier map by given message id.
  /// \param_in q_id, channel id
  /// \param_in barrier_id, barrier or checkpoint of long runtime job
  /// \param_out message_id, message id of barrier
  StreamingStatus GetMsgIdByBarrierId(const ObjectID &q_id, uint64_t barrier_id,
                                      uint64_t &message_id);

  /// Append new message id to queue-barrier map.
  /// \param_in q_id, channel id
  /// \param_in barrier_id, barrier or checkpoint of long running job
  /// \param_in message_id, message id of barrier
  void SetMsgIdByBarrierId(const ObjectID &q_id, uint64_t barrier_id,
                           uint64_t message_id);

  /// Check whether barrier id in queue-barrier map.
  /// \param_in barrier_id, barrier id or checkpoint id
  bool Contains(uint64_t barrier_id);

  /// Remove barrier info from queue-barrier map by given seq id.
  void ReleaseBarrierMapSeqIdById(uint64_t barrier_id);

  /// Remove all barrier info from queue-barrier map.
  void ReleaseAllBarrierMapSeqId();

  /// Fetch barrier id list from queue-barrier map.
  void GetAllBarrier(std::vector<uint64_t> &barrier_id_vec);

  /// Get barrier map capacity of current version.
  uint32_t GetBarrierMapSize();

  /// We assume there are multiple barriers in one checkpoint, so barrier id
  /// should belong to a checkpoint id.
  /// \param_in barrier_id, barrier id
  /// \param_in checkpoint_id, checkpoint id
  void MapBarrierToCheckpoint(uint64_t barrier_id, uint64_t checkpoint_id);

  /// Get checkpoint id by given barrier id
  /// \param_in barrier_id, barrier id
  /// \param_out checkpoint_id, checkpoint id
  StreamingStatus GetCheckpointIdByBarrierId(uint64_t barrier_id,
                                             uint64_t &checkpoint_id);

  /// Clear barrier-checkpoint relation if elements of barrier id vector are
  /// equal to or less than given barrier id.
  /// \param_in barrier_id
  void ReleaseBarrierMapCheckpointByBarrierId(const uint64_t barrier_id);

  /// Get barrier id by lastest message id and channel
  /// \param_in q_id, channel id
  /// \param_in message_id, lastest message id of barrier data
  /// \param_out barrier_id, barrier id
  /// \param_in is_pop, whether pop out from queue
  StreamingStatus GetBarrierIdByLastMessageId(const ObjectID &q_id, uint64_t message_id,
                                              uint64_t &barrier_id, bool is_pop = false);

  /// Put new barrier id in map by channel index and lastest message id.
  /// \param_in q_id, channel id
  /// \param_in message_id, lastest message id of barrier data
  /// \param_in barrier_id, barrier id
  void SetBarrierIdByLastMessageId(const ObjectID &q_id, uint64_t message_id,
                                   uint64_t barrier_id);

  /// \param_in q_id, channel id
  /// \param_in checkpoint_id, checkpoint id of long running job
  void GetCurrentMaxCheckpointIdInQueue(const ObjectID &q_id,
                                        uint64_t &checkpoint_id) const;

  /// \param_in q_id, channel id
  /// \param_in checkpoint_id, checkpoint id of long running job
  void SetCurrentMaxCheckpointIdInQueue(const ObjectID &q_id,
                                        const uint64_t checkpoint_id);

  void AddLoggingModeBarrier(const ObjectID &q_id, const uint64_t checkpoint_id,
                             const uint64_t bundle_id);

  std::pair<uint64_t, uint64_t> GetCurrentLoggingModeCheckpointId(const ObjectID &q_id);

  /// Remote all barrier ids before the target checkpoint_id
  /// [1, 2, 3, 4], if the target id 2, the result will be [3, 4]
  void ReleaseLoggingModeCheckpoints(
      const ObjectID &q_id, const uint64_t checkpoint_id,
      std::list<std::pair<uint64_t, uint64_t>> &removed_list);

 private:
  // Global barrier map set (global barrier id -> (channel id -> seq id))
  std::unordered_map<uint64_t, std::unordered_map<ObjectID, uint64_t>>
      global_barrier_map_;

  // Message id map to barrier id of each queue(continuous barriers hold same last message
  // id)
  // message id -> (queue id -> list(barrier id)).
  // Thread unsafe to assign value in user's thread but collect it in loopforward thread.
  std::unordered_map<uint64_t, std::unordered_map<ObjectID, BarrierIdQueue>>
      global_reversed_barrier_map_;

  std::unordered_map<uint64_t, uint64_t> barrier_checkpoint_map_;

  std::unordered_map<ObjectID, uint64_t> max_message_id_map_;

  // We assume default max checkpoint is 0.
  std::unordered_map<ObjectID, uint64_t> current_max_checkpoint_id_map_;

  std::mutex message_id_map_barrier_mutex_;

  std::mutex global_barrier_mutex_;

  std::mutex barrier_map_checkpoint_mutex_;

  // Logging mode barriers for cyclic channels.
  // (global barrier id -> [(barrier_id1, seq_id), (barrier_id2, seq_id), ...])
  std::unordered_map<ObjectID, std::list<std::pair<uint64_t, uint64_t>>>
      logging_mode_barriers_;
  // mutex to protect `logging_mode_barriers_`
  std::mutex logging_mode_barriers_mutex_;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_BARRIER_HELPER_H
