#include "ray/raylet/job_manager.h"

#include "ray/event/event.h"

namespace ray {
namespace raylet {

JobManager::JobManager() {
  auto job_name_callback = [this](rpc::Event &event) -> void {
    auto it = event.custom_fields().find("job_id");
    if (it == event.custom_fields().end() || it->second == "") {
      return;
    }
    auto job_table_data = this->GetJobData(::ray::JobID::FromHex(it->second));
    if (job_table_data != nullptr) {
      event.mutable_custom_fields()->insert({"job_name", job_table_data->job_name()});
    }
  };
  ::ray::RayEventContext::Instance().SetEventPostProcessor(job_name_callback);
}

bool JobManager::OnJobSubmitted(std::shared_ptr<JobTableData> job_table_data) {
  RAY_CHECK(job_table_data && job_table_data->state() == rpc::JobTableData::SUBMITTED);
  auto job_id = JobID::FromBinary(job_table_data->job_id());
  return jobs_.emplace(job_id, job_table_data).second;
}

bool JobManager::OnJobStarted(std::shared_ptr<JobTableData> job_table_data) {
  RAY_CHECK(job_table_data && job_table_data->state() == rpc::JobTableData::RUNNING);
  auto job_id = JobID::FromBinary(job_table_data->job_id());
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    jobs_.emplace(job_id, std::move(job_table_data));
    return true;
  }
  return iter->second->state() == rpc::JobTableData::SUBMITTED;
}

bool JobManager::OnJobFailedOrFinished(const JobID &job_id) {
  return jobs_.erase(job_id) != 0;
}

std::shared_ptr<JobTableData> JobManager::GetJobData(const JobID &job_id) const {
  auto iter = jobs_.find(job_id);
  return iter == jobs_.end() ? nullptr : iter->second;
}

const absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>>
    &JobManager::GetAllJobData() const {
  return jobs_;
}

}  // namespace raylet
}  // namespace ray