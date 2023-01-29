#pragma once
#include <boost/dll.hpp>
#include <functional>

#include "context.h"
#include "record.h"

#define STREAMING_FUNC_ALIAS(A, B) BOOST_DLL_ALIAS(A, B)

namespace ray {
namespace streaming {
enum class FunctionType { SOURCE = 1, MAP, FLAT_MAP, SINK };
enum class FunctionLang { CPP = 1, JAVA, PYTHON };

class Function {
 public:
  virtual void Open(std::shared_ptr<RuntimeContext> &runtime_context) = 0;
  virtual void Finish() {}
  virtual void Close() {}
  // \param_in checkpoint id : save function checkpoint when operator triggered.
  virtual bool SaveCheckpoint(int checkpoint_id) { return true; }
  // \param_in checkpoint id : load function checkpoint when operator triggered.
  virtual bool LoadCheckpoint(int checkpoint_id) { return true; }
  // \param_in checkpoint id : delete function checkpoint when operator triggered.
  virtual bool DeleteCheckpoint(int checkpoint_id) { return true; }
};

class NativeFunction : public Function {
 public:
  NativeFunction(const std::string &lib_path, const std::string &creator_name,
                 FunctionType func_type)
      : lib_path_(lib_path), creator_name_(creator_name), func_type_(func_type) {}
  void Open(std::shared_ptr<RuntimeContext> &runtime_context) override {}
  const std::string &LibPath() { return lib_path_; }
  const std::string &CreatorName() { return creator_name_; }
  FunctionType Type() { return func_type_; }
  virtual ~NativeFunction() = default;

 protected:
  std::string lib_path_;
  std::string creator_name_;
  FunctionType func_type_;
};

/// Source function has defined an input entry where
/// Fetcher must be invoke in and called by stream task.
class SourceFunction : public Function {
 public:
  virtual void Init(int parallelism, int index) = 0;
  virtual void Fetch(std::shared_ptr<SourceContext> &source_context,
                     int checkpoint_id) = 0;
  virtual ~SourceFunction() = default;
};

class MapFunction : public Function {
 public:
  virtual LocalRecord Map(LocalRecord &record) = 0;
  virtual ~MapFunction() = default;
};

class FlatMapFunction : public Function {
 public:
  virtual void FlatMap(LocalRecord &record, std::shared_ptr<Collector> collector) = 0;
};

class SourceLambdaFunction : public SourceFunction {
 public:
  SourceLambdaFunction(std::function<LocalRecord(void)> &&source_func);
  virtual void Open(std::shared_ptr<RuntimeContext> &runtime_context) {}
  virtual void Init(int parallelism, int index) override {}
  virtual void Fetch(std::shared_ptr<SourceContext> &source_context,
                     int checkpoint_id) override;
  virtual ~SourceLambdaFunction() = default;

 private:
  std::function<LocalRecord(void)> source_func_;
};

class MapLambdaFunction : public MapFunction {
 public:
  MapLambdaFunction(std::function<LocalRecord(LocalRecord &)> &&map_func);
  virtual void Open(std::shared_ptr<RuntimeContext> &runtime_context) override {}
  virtual LocalRecord Map(LocalRecord &record) override;

 private:
  std::function<LocalRecord(LocalRecord &)> map_func_;
};

class SinkFunction : public Function {
 public:
  virtual void Sink(LocalRecord &record) = 0;
  virtual ~SinkFunction() = default;
};

class SinkLambdaFunction : public SinkFunction {
 public:
  SinkLambdaFunction(std::function<void(LocalRecord &)> &&sink_func);
  virtual void Open(std::shared_ptr<RuntimeContext> &runtime_context) override {}
  virtual void Sink(LocalRecord &record) override;

 private:
  std::function<void(LocalRecord &)> sink_func_;
};

class ConsoleSinkFunction : public SinkFunction {
 public:
  void Sink(LocalRecord &record) override;
  void Open(std::shared_ptr<RuntimeContext> &runtime_context) override;
};
}  // namespace streaming
}  // namespace ray