#include "ray/core_worker/task_manager.h"

namespace ray {
void TaskManager::WarnAboutHangingTasks() const {
  auto now = current_time_ms();
  size_t count = 0;
  std::vector<TaskID> task_ids;
  const size_t max_tasks_to_print = 10;
  auto hanging_tasks_threashold_ms = RayConfig::instance().hanging_tasks_threashold_ms();
  absl::MutexLock lock(&mu_);
  for (auto it : submissible_tasks_) {
    if (it.second.pending &&
        now - it.second.submission_time > hanging_tasks_threashold_ms) {
      if (it.second.spec.IsActorCreationTask()) {
        // TODO(kfstorm): We have to exclude actor creation tasks here because they stay
        // in task manager even if actors are created successfully.
        continue;
      }
      ++count;
      if (count < max_tasks_to_print) {
        task_ids.push_back(it.first);
      }
    }
  }
  if (count > 0) {
    std::ostringstream oss;
    oss << "There are " << count << " tasks pending for more than "
        << RayConfig::instance().hanging_tasks_threashold_ms() << "ms. First "
        << max_tasks_to_print << " tasks:";
    for (const auto &task_id : task_ids) {
      auto it = submissible_tasks_.find(task_id);
      RAY_CHECK(it != submissible_tasks_.end());
      oss << " Task { " << it->second.spec.DebugString() + " } submitted "
          << (now - it->second.submission_time) / 1000 << "s ago, "
          << it->second.num_retries_left << " retries left;";
    }
    RAY_LOG(WARNING) << oss.str();
  }
}
}  // namespace ray
