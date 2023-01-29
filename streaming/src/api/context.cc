#include "context.h"

namespace ray {
namespace streaming {
RuntimeContext::RuntimeContext(int task_id, int task_index, int parallelism,
                               const std::unordered_map<std::string, std::string> &config)
    : task_id_(task_id),
      task_index_(task_index),
      parallelism_(parallelism),
      config_(config) {}

RuntimeContext::RuntimeContext(RuntimeContext &context)
    : RuntimeContext(context.GetTaskId(), context.GetTaskIndex(),
                     context.GetParallelism(), context.GetConfig()) {}

SourceContext::SourceContext(int task_id, int task_index, int parallelism,
                             std::unordered_map<std::string, std::string> &config,
                             std::vector<std::shared_ptr<Collector>> &collectors)
    : RuntimeContext(task_id, task_index, parallelism, config), collectors_(collectors){};

SourceContext::SourceContext(RuntimeContext &context,
                             std::vector<std::shared_ptr<Collector>> &collectors)
    : RuntimeContext(context), collectors_(collectors) {}

SourceContext::SourceContext(std::shared_ptr<RuntimeContext> &context,
                             std::vector<std::shared_ptr<Collector>> &collectors)
    : SourceContext(*context.get(), collectors) {}

std::vector<std::shared_ptr<Collector>> &SourceContext::GetCollectors() {
  return this->collectors_;
}
void SourceContext::Collect(LocalRecord &record) {
  for (auto &collector : collectors_) {
    collector->Collect(record);
  }
}

void SourceContext::Collect(LocalRecord &&record) {
  for (auto &collector : collectors_) {
    collector->Collect(record);
  }
}

StreamRuntimeContext::StreamRuntimeContext(RuntimeContext &runtime_context)
    : RuntimeContext(runtime_context) {}
}  // namespace streaming
}  // namespace ray