#pragma once
#include <string>
#include <unordered_map>
#include <vector>

#include "collector.h"
#include "record.h"

namespace ray {
namespace streaming {
/// It's assumed that every opertor and processor would hold runtime context
/// to choose what they should detect and fetch running needed informations.
class RuntimeContext {
 public:
  RuntimeContext(int task_id, int task_index, int parallelism,
                 const std::unordered_map<std::string, std::string> &config = {});
  RuntimeContext(RuntimeContext &context);
  inline int GetTaskIndex() { return task_index_; }
  inline int GetTaskId() { return task_id_; }
  inline int GetParallelism() { return parallelism_; };
  inline std::unordered_map<std::string, std::string> &GetConfig() { return config_; }

  virtual ~RuntimeContext() = default;

 protected:
  // Unique id of whole DAG.
  int task_id_;
  // Unique index of one vertex.
  int task_index_;
  // Total parallelism in this vertex.
  int parallelism_;
  // Vertex config or operator information in string format key-value.
  std::unordered_map<std::string, std::string> config_;
};

/// SourceContext differs from other context. It's to say that an aditional
/// collector vector could be used to forward data from source operator to
/// next non-source level.
class SourceContext : public RuntimeContext {
 public:
  SourceContext(RuntimeContext &context,
                std::vector<std::shared_ptr<Collector>> &collectors);

  SourceContext(std::shared_ptr<RuntimeContext> &context,
                std::vector<std::shared_ptr<Collector>> &collectors);

  SourceContext(int task_id, int task_index, int parallelism,
                std::unordered_map<std::string, std::string> &config,
                std::vector<std::shared_ptr<Collector>> &collectors);
  void Collect(LocalRecord &record);
  void Collect(LocalRecord &&record);
  std::vector<std::shared_ptr<Collector>> &GetCollectors();

 protected:
  std::vector<std::shared_ptr<Collector>> collectors_;
};

/// StreamRuntimeContext is specific context for processor runtime.
class StreamRuntimeContext : public RuntimeContext {
 public:
  StreamRuntimeContext(RuntimeContext &runtime_context);
};

}  // namespace streaming
}  // namespace ray