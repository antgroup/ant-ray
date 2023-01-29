#include "operator.h"

#include "logging.h"
namespace ray {
namespace streaming {
Operator::Operator(OperatorType operator_type, const std::shared_ptr<Function> &func,
                   const std::unordered_map<std::string, std::string> &config)
    : operator_type_(operator_type), func_(func), op_config_(config) {}

OperatorType Operator::GetOperatorType() { return operator_type_; }

std::unordered_map<std::string, std::string> Operator::GetConfig() { return op_config_; }

void Operator::SetConfig(std::unordered_map<std::string, std::string> &config) {
  op_config_ = config;
}

void Operator::Open(std::vector<std::shared_ptr<Collector>> &collectors,
                    std::shared_ptr<RuntimeContext> &runtime_context) {
  this->context_ = runtime_context;
  this->collectors_ = collectors;
  this->func_->Open(this->context_);
}

void Operator::Finish() { this->func_->Finish(); }

void Operator::Close() { this->func_->Close(); }

bool Operator::SaveCheckpoint(int checkpoint_id) {
  return this->func_->SaveCheckpoint(checkpoint_id);
}

bool Operator::LoadCheckpoint(int checkpoint_id) {
  return this->func_->LoadCheckpoint(checkpoint_id);
}

bool Operator::DeleteCheckpoint(int checkpoint_id) {
  return this->func_->DeleteCheckpoint(checkpoint_id);
}
void Operator::Collect(LocalRecord &record) {
  for (auto collector : this->collectors_) {
    collector->Collect(record);
  }
}

SourceOperator::SourceOperator(const std::shared_ptr<SourceFunction> &func)
    : Operator(OperatorType::SOURCE, func), source_func_(func) {}

void SourceOperator::Open(std::vector<std::shared_ptr<Collector>> &collectors,
                          std::shared_ptr<RuntimeContext> &runtime_context) {
  Operator::Open(collectors, runtime_context);
  this->source_context_ = std::make_shared<SourceContext>(runtime_context, collectors);
}

void SourceOperator::Fetch(int checkpoint_id) {
  this->source_func_->Fetch(this->source_context_, checkpoint_id);
}

OneInputOperator::OneInputOperator(
    const std::shared_ptr<Function> &func,
    const std::unordered_map<std::string, std::string> &op_config)
    : Operator(OperatorType::ONT_INPUT, func, op_config) {}

MapOperator::MapOperator(std::shared_ptr<MapFunction> &func)
    : OneInputOperator(func), map_func_(func) {}

void MapOperator::Process(LocalRecord &record) {
  auto local_record = this->map_func_->Map(record);
  this->Collect(local_record);
}

SinkOperator::SinkOperator(std::shared_ptr<SinkFunction> &func)
    : OneInputOperator(func), sink_func_(func) {}

void SinkOperator::Process(LocalRecord &record) { this->sink_func_->Sink(record); }

ForwardCollector::ForwardCollector(std::shared_ptr<OneInputOperator> succeeding_operator)
    : succeeding_operator_(succeeding_operator) {}

void ForwardCollector::Collect(LocalRecord &record) {
  succeeding_operator_->Process(record);
}

ChainedOperator::ChainedOperator(std::vector<std::shared_ptr<Operator>> &operator_vec)
    : Operator(operator_vec[0]->GetOperatorType()), operator_vec_(operator_vec) {
  this->head_operator_ = operator_vec_[0];
  this->tail_operator_ = operator_vec_.back();
}

void ChainedOperator::Open(std::vector<std::shared_ptr<Collector>> &collectors,
                           std::shared_ptr<RuntimeContext> &runtime_context) {
  STREAMING_LOG(INFO) << "Open chained operator.";
  // NOTE(lingxuan.zlx): we assume all of operators are linage.
  for (size_t i = 0; i < operator_vec_.size() - 1; ++i) {
    auto &current_operator = operator_vec_[i];
    auto &next_operator = operator_vec_[i + 1];
    std::vector<std::shared_ptr<Collector>> succeeding_collector_vec;
    auto succeeding_collector = std::make_shared<ForwardCollector>(
        std::dynamic_pointer_cast<OneInputOperator>(next_operator));
    succeeding_collector_vec.push_back(
        std::dynamic_pointer_cast<Collector>(succeeding_collector));
    // Create new runtime context for each operator.
    current_operator->Open(succeeding_collector_vec, runtime_context);
  }
  this->tail_operator_->Open(collectors, runtime_context);
}

void ChainedOperator::Finish() {
  for (auto &operator_ : this->operator_vec_) {
    operator_->Finish();
  }
}

void ChainedOperator::Close() {
  for (auto &operator_ : this->operator_vec_) {
    operator_->Close();
  }
}

OperatorType ChainedOperator::GetOperatorType() {
  return head_operator_->GetOperatorType();
}
bool ChainedOperator::SaveCheckpoint(int checkpoint_id) {
  // TODO(lingxuan.zlx) : save checkpoint and return result flag.
  return true;
}

bool ChainedOperator::LoadCheckpoint(int checkpoint_id) { return true; }

bool ChainedOperator::DeleteCheckpoint(int checkpoint_id) { return true; }

ChainedOneInputOperator::ChainedOneInputOperator(
    std::vector<std::shared_ptr<Operator>> &operator_vec)
    : ChainedOperator(operator_vec), OneInputOperator() {
  input_operator_ = std::dynamic_pointer_cast<OneInputOperator>(head_operator_);
}

void ChainedOneInputOperator::Open(std::vector<std::shared_ptr<Collector>> &collectors,
                                   std::shared_ptr<RuntimeContext> &runtime_context) {
  ChainedOperator::Open(collectors, runtime_context);
}

void ChainedOneInputOperator::Process(LocalRecord &record) {
  STREAMING_LOG(DEBUG) << "Input process";
  this->input_operator_->Process(record);
}

ChainedSourceOperator::ChainedSourceOperator(
    std::vector<std::shared_ptr<Operator>> &operator_vec)
    : ChainedOperator(operator_vec), SourceOperator(nullptr) {
  source_operator_ = std::dynamic_pointer_cast<SourceOperator>(head_operator_);
}

void ChainedSourceOperator::Open(std::vector<std::shared_ptr<Collector>> &collectors,
                                 std::shared_ptr<RuntimeContext> &runtime_context) {
  ChainedOperator::Open(collectors, runtime_context);
}

void ChainedSourceOperator::Fetch(int checkpoint_id) {
  STREAMING_LOG(DEBUG) << "Source fetch checkpoint " << checkpoint_id;
  this->source_operator_->Fetch(checkpoint_id);
}

}  // namespace streaming
}  // namespace ray