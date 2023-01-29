#pragma once
#include "collector.h"
#include "context.h"
#include "function.h"
#include "operator.h"
#include "stream.h"

namespace ray {
namespace streaming {
class Processor {
 public:
  virtual void Open(std::vector<std::shared_ptr<Collector>> &collectors,
                    std::shared_ptr<StreamRuntimeContext> &runtime_context) = 0;
  virtual void Finish() = 0;
  virtual void Close() = 0;
  virtual bool SaveCheckpoint(int checkpoint_id) = 0;
  virtual bool LoadCheckpoint(int checkpoint_id) = 0;
  virtual bool DeleteCheckpoint(int checkpoint_id) = 0;
  virtual void Process(LocalRecord &record) = 0;
  virtual ~Processor() = default;
};

class StreamProcessor : public Processor {
 public:
  StreamProcessor(std::shared_ptr<Operator> stream_operator);
  virtual void Open(std::vector<std::shared_ptr<Collector>> &collectors,
                    std::shared_ptr<StreamRuntimeContext> &runtime_context) override;
  virtual void Finish() override;
  virtual void Close() override;
  virtual bool SaveCheckpoint(int checkpoint_id) override;
  virtual bool LoadCheckpoint(int checkpoint_id) override;
  virtual bool DeleteCheckpoint(int checkpoint_id) override;
  virtual ~StreamProcessor() = default;

 protected:
  std::shared_ptr<Operator> operator_;
  std::vector<std::shared_ptr<Collector>> collectors_;
  std::shared_ptr<StreamRuntimeContext> runtime_context_;
};

class SourceProcessor : public StreamProcessor {
 public:
  SourceProcessor(std::shared_ptr<SourceOperator> source_operator);

  virtual void Process(LocalRecord &record) override;
  virtual ~SourceProcessor() = default;

 private:
  std::shared_ptr<SourceOperator> source_operator_;
};

class StreamTask {
 public:
  virtual void Run() = 0;
  virtual ~StreamTask() = default;
};

class LocalStreamTask : public StreamTask {
 public:
  LocalStreamTask(std::shared_ptr<StreamingContext> streaming_context);
  void Open();
  virtual void Run() override;

 private:
  std::shared_ptr<Operator> ConvertNativeDataStreamToOperator(
      std::shared_ptr<NativeDataStream> native_data_stream);

 private:
  std::shared_ptr<StreamingContext> streaming_context_;
  std::shared_ptr<SourceProcessor> source_processor_;
};
}  // namespace streaming
}  // namespace ray