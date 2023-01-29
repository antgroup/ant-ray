#ifndef RAY_STREAMING_RESCALE_CONTEXT_H
#define RAY_STREAMING_RESCALE_CONTEXT_H
#include "channel.h"
namespace ray {
namespace streaming {
class RescaleContext {
 public:
  inline void UpdateRescaleTargetSet(const std::vector<ObjectID> &ids) {
    STREAMING_LOG(INFO) << "Original collection size : " << update_set_.size()
                        << ", Updated size : " << ids.size();
    std::copy(ids.begin(), ids.end(), std::inserter(update_set_, update_set_.end()));
  }
  /// Clear all info from context if rescale event is finished or aborted.
  inline void ClearContext() {
    update_set_.clear();
    scale_up_added_.clear();
    scale_down_removed_.clear();
  }

 public:
  // Update id set.
  // Consumer throw final barrier to upper level if all upstream targets in this
  // collection have sent barrier to it.
  std::unordered_set<ObjectID> update_set_;
  std::vector<ObjectID> scale_up_added_;
  // diff vector for destroying queue connection in scaledown
  std::vector<ObjectID> scale_down_removed_;
};

class Scalable {
 public:
  virtual void Rescale(const std::vector<ObjectID> &id_vec,
                       const std::vector<ObjectID> &target_id_vec,
                       std::vector<StreamingQueueInitialParameter> &actor_handle_vec) = 0;
  virtual void RescaleRollback() {
    STREAMING_LOG(INFO) << "Begin to rollback rescale.";
    if (!rescale_context_.scale_down_removed_.empty()) {
      // Don't need recreate scale down channel because they shouldn't be
      // removed if partial checkpoint is unfinished. So we only clear markable
      // variables from context.
    }

    if (!rescale_context_.scale_up_added_.empty()) {
      this->ScaleDown(rescale_context_.scale_up_added_);
    }
    rescale_context_.ClearContext();
    STREAMING_LOG(INFO) << "Finish to rollback rescale.";
  }

 protected:
  virtual void ScaleUp(
      const std::vector<ray::ObjectID> &id_vec,
      std::vector<StreamingQueueInitialParameter> &scale_init_parameter_vec) = 0;
  virtual void ScaleDown(const std::vector<ray::ObjectID> &id_vec) = 0;

 protected:
  RescaleContext rescale_context_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_RESCALE_CONTEXT_H
