#pragma once
#include <memory>
#include <unordered_map>
#include <vector>

#include "collector.h"
#include "context.h"
#include "function.h"

namespace ray {
namespace streaming {
/// We divide all of operator into these three types:
/// SOURCE, ONE_INPUT and TWO_INPUT.
/// It's noted that source operator must have a fetch function
/// to get outside into inside and then provide related data
/// for downstream processing.
/// For ONE_INPUT operator, it only accepte one-way input that
// differ from TWO_INPUT operator who can merge two-way input.
enum class OperatorType { SOURCE = 1, ONT_INPUT, TWO_INPUT };

class Operator {
 public:
  Operator(OperatorType operator_type, const std::shared_ptr<Function> &func = nullptr,
           const std::unordered_map<std::string, std::string> &op_config = {});
  virtual void Open(std::vector<std::shared_ptr<Collector>> &collectors,
                    std::shared_ptr<RuntimeContext> &runtime_context);
  virtual void Finish();
  virtual void Close();
  virtual OperatorType GetOperatorType();
  virtual bool SaveCheckpoint(int checkpoint_id);
  virtual bool LoadCheckpoint(int checkpoint_id);
  virtual bool DeleteCheckpoint(int checkpoint_id);
  virtual std::unordered_map<std::string, std::string> GetConfig();
  virtual void SetConfig(std::unordered_map<std::string, std::string> &config);
  virtual void Collect(LocalRecord &record);
  virtual ~Operator() = default;

 protected:
  OperatorType operator_type_;
  std::shared_ptr<Function> func_;
  std::unordered_map<std::string, std::string> op_config_;
  std::shared_ptr<RuntimeContext> context_;
  std::vector<std::shared_ptr<Collector>> collectors_;
};

class SourceOperator : public Operator {
 public:
  SourceOperator(const std::shared_ptr<SourceFunction> &func);
  void Open(std::vector<std::shared_ptr<Collector>> &collectors,
            std::shared_ptr<RuntimeContext> &runtime_context);
  virtual void Fetch(int checkpoint_id);
  virtual ~SourceOperator() = default;

 protected:
  std::shared_ptr<SourceContext> source_context_;
  std::shared_ptr<SourceFunction> source_func_;
};

class OneInputOperator : public Operator {
 public:
  OneInputOperator(const std::shared_ptr<Function> &func = nullptr,
                   const std::unordered_map<std::string, std::string> &op_config = {});
  virtual void Process(LocalRecord &record) = 0;
  virtual ~OneInputOperator() = default;
};

class MapOperator : public OneInputOperator {
 public:
  MapOperator(std::shared_ptr<MapFunction> &func);
  void Process(LocalRecord &record) override;

 protected:
  std::shared_ptr<MapFunction> map_func_;
};

class SinkOperator : public OneInputOperator {
 public:
  SinkOperator(std::shared_ptr<SinkFunction> &func);
  void Process(LocalRecord &record) override;

 protected:
  std::shared_ptr<SinkFunction> sink_func_;
};

class TowInputOperator : public Operator {
 public:
  virtual void Process(LocalRecord &left_record, LocalRecord &right_record) = 0;
  virtual ~TowInputOperator() = default;
};

/// ForwardCollector is auto-generated for local chained operator.
class ForwardCollector : public Collector {
 public:
  ForwardCollector(std::shared_ptr<OneInputOperator> succeeding_operator);
  // \param_in record collect record to succeeding operator processing.
  void Collect(LocalRecord &record) override;

 private:
  std::shared_ptr<OneInputOperator> succeeding_operator_;
};

class ChainedOperator : public Operator {
 public:
  ChainedOperator(std::vector<std::shared_ptr<Operator>> &operator_vec);
  virtual void Open(std::vector<std::shared_ptr<Collector>> &collectors,
                    std::shared_ptr<RuntimeContext> &runtime_context) override;
  virtual void Finish() override;
  virtual void Close() override;
  virtual OperatorType GetOperatorType() override;
  virtual bool SaveCheckpoint(int checkpoint_id) override;
  virtual bool LoadCheckpoint(int checkpoint_id) override;
  virtual bool DeleteCheckpoint(int checkpoint_id) override;
  virtual ~ChainedOperator() = default;

 protected:
  std::vector<std::shared_ptr<Operator>> operator_vec_;
  std::shared_ptr<Operator> head_operator_;
  // NOTE(lingxuan.zlx): tail opertor might be set collections,
  // it's assumed to be one operator to simpilify currently.
  std::shared_ptr<Operator> tail_operator_;
};

class ChainedSourceOperator : public ChainedOperator, public SourceOperator {
 public:
  ChainedSourceOperator(std::vector<std::shared_ptr<Operator>> &operator_vec);
  void Open(std::vector<std::shared_ptr<Collector>> &collectors,
            std::shared_ptr<RuntimeContext> &runtime_context) override;
  void Fetch(int checkpoint_id) override;

 private:
  std::shared_ptr<SourceOperator> source_operator_;
};

class ChainedOneInputOperator : public ChainedOperator, public OneInputOperator {
 public:
  ChainedOneInputOperator(std::vector<std::shared_ptr<Operator>> &operator_vec);
  void Open(std::vector<std::shared_ptr<Collector>> &collectors,
            std::shared_ptr<RuntimeContext> &runtime_context) override;
  void Process(LocalRecord &record) override;

 private:
  std::shared_ptr<OneInputOperator> input_operator_;
};

}  // namespace streaming
}  // namespace ray