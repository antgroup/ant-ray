#include "ray/gcs/gcs_server/gcs_job_distribution_formatter.h"

#include "ray/common/task/scheduling_resources_util.h"
#include "ray/util/json.h"

namespace ray {
namespace gcs {

std::string JobDistributionFormatter::Format(
    std::shared_ptr<GcsJobDistribution> job_distribution) {
  rapidjson::Document doc(rapidjson::kArrayType);
  rapidjson::Document::AllocatorType &allocator = doc.GetAllocator();
  const auto &job_scheduling_contexts = job_distribution->GetAllJobSchedulingContexts();
  for (const auto &entry : job_scheduling_contexts) {
    rapidjson::Document doc_job(rapidjson::kObjectType);

    auto job_scheduling_context = entry.second;
    const auto &job_config = job_scheduling_context->GetJobConfig();
    rapidjson::Document doc_job_config(rapidjson::kObjectType);
    doc_job_config.AddMember("job_id", job_config.job_id_.Hex(), allocator);
    doc_job_config.AddMember("nodegroup_id", job_config.nodegroup_id_, allocator);
    doc_job_config.AddMember("num_java_workers_per_process",
                             job_config.num_java_workers_per_process_, allocator);
    doc_job_config.AddMember(
        "java_worker_process_default_memory_gb",
        FromMemoryUnitsToGiB(job_config.java_worker_process_default_memory_units_),
        allocator);
    doc_job_config.AddMember(
        "python_worker_process_default_memory_gb",
        FromMemoryUnitsToGiB(job_config.python_worker_process_default_memory_units_),
        allocator);
    doc_job_config.AddMember("total_memory_gb",
                             FromMemoryUnitsToGiB(job_config.total_memory_units_),
                             allocator);
    doc_job_config.AddMember("max_total_memory_gb",
                             FromMemoryUnitsToGiB(job_config.max_total_memory_units_),
                             allocator);

    const auto &node_to_worker_processes =
        job_scheduling_context->GetNodeToWorkerProcesses();
    rapidjson::Document doc_worker_processes_dist(rapidjson::kArrayType);
    for (const auto &entry : node_to_worker_processes) {
      const auto &worker_processes = entry.second;
      for (const auto &worker_process_entry : worker_processes) {
        auto worker_process = worker_process_entry.second;
        rapidjson::Document doc_worker_process(rapidjson::kObjectType);
        doc_worker_process.AddMember("node_id", worker_process->GetNodeID().Hex(),
                                     allocator);
        doc_worker_process.AddMember(
            "worker_process_id", worker_process->GetWorkerProcessID().Hex(), allocator);
        doc_worker_process.AddMember("job_id", worker_process->GetJobID().Hex(),
                                     allocator);
        doc_worker_process.AddMember("is_shared", worker_process->IsShared(), allocator);
        doc_worker_process.AddMember(
            "slot_capacity", (uint32_t)worker_process->GetSlotCapacity(), allocator);
        doc_worker_process.AddMember("available_slot_count",
                                     (uint32_t)worker_process->GetAvailableSlotCount(),
                                     allocator);

        uint64_t acquired_mem_resource = 0;
        auto constraint_resources = worker_process->GetConstraintResources();
        ray::ExtractMemoryResourceUnits(constraint_resources, &acquired_mem_resource);
        doc_worker_process.AddMember("acquired_resources", acquired_mem_resource,
                                     allocator);

        doc_worker_processes_dist.PushBack(doc_worker_process, allocator);
      }
    }

    doc_job.AddMember("job_config", doc_job_config, allocator);
    doc_job.AddMember("worker_processes_dist", doc_worker_processes_dist, allocator);
    doc.PushBack(doc_job, allocator);
  }
  return rapidjson::to_string(doc, true);
}

}  // namespace gcs
}  // namespace ray