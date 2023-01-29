#include "ray/core_worker/core_worker.h"

namespace ray {
void CoreWorker::WarnAboutHangingTasksPeriodically() {
  periodical_runner_.RunFnPeriodically(
      [this] { task_manager_->WarnAboutHangingTasks(); },
      RayConfig::instance().hanging_tasks_warning_internal_ms());
}
}  // namespace ray
