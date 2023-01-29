#pragma once
#include <atomic>

#include "function.h"
#include "operator.h"

namespace ray {
namespace streaming {
class StreamingContext;

class Stream {
 public:
  // NOTE(lingxuan.zlx): It uses raw pointer to create new stream vertex since
  // link-style would not pass std::shared_ptr object.
  // \param_in streaming_context, current stream context for creating new stream.
  // \param_in input stream, upstream vertex for this node.
  // \param_in stream operator, runtime function and operator for current stream.
  Stream(std::weak_ptr<StreamingContext> streaming_context, Stream *input_stream,
         std::shared_ptr<Operator> stream_operator);

  // \param_in streaming_context, current stream context for creating new stream.
  // \param_in stream operator, runtime function and operator for current stream.
  Stream(std::weak_ptr<StreamingContext> streaming_context,
         std::shared_ptr<Operator> stream_operator);

  // \param_in input stream, upstream vertex for this node.
  // \param_in stream operator, runtime function and operator for current stream.
  Stream(Stream *input_stream, std::shared_ptr<Operator> stream_operator);
  std::weak_ptr<StreamingContext> GetStreamingContext();
  Stream *InputStream();
  std::shared_ptr<Operator> GetOperator();
  int32_t Id();

  virtual ~Stream();

 protected:
  // To avoid cycle reference since DataStream and StreamingContext holds
  // each other objects/
  std::weak_ptr<StreamingContext> streaming_context_;
  Stream *input_stream_;
  std::shared_ptr<Operator> operator_;
  int32_t id_;
  std::string name_;
};

class DataStreamSink;

class DataStream : public Stream, public std::enable_shared_from_this<DataStream> {
 public:
  DataStream(std::shared_ptr<StreamingContext> streaming_context,
             std::shared_ptr<Operator> stream_operator);
  DataStream(DataStream *input_stream, std::shared_ptr<Operator> stream_operator);
  std::shared_ptr<DataStream> Map(std::shared_ptr<MapFunction> map_func);
  std::shared_ptr<DataStream> Map(std::function<LocalRecord(LocalRecord &)> &&map_func);
  std::shared_ptr<DataStreamSink> Sink(std::shared_ptr<SinkFunction> sink_func);
  std::shared_ptr<DataStreamSink> Sink(std::function<void(LocalRecord &)> &&sink_func);
  std::shared_ptr<DataStream> Self();
  virtual ~DataStream();
};

class NativeDataStreamSink;

/// Pointer a native function or operator to dynamic library by locating a so file.
class NativeDataStream : public DataStream {
 public:
  NativeDataStream(std::shared_ptr<StreamingContext> streaming_context,
                   const std::string &native_lib_path, const std::string &creator_name,
                   FunctionType func_type);
  NativeDataStream(DataStream *input_stream, const std::string &native_lib_path,
                   const std::string &creator_name, FunctionType func_type);
  std::shared_ptr<NativeDataStream> Map(const std::string &native_lib_path,
                                        const std::string &creator_name);
  std::shared_ptr<NativeDataStreamSink> Sink(const std::string &native_lib_path,
                                             const std::string &creator_name);
  const std::string &LibPath() { return native_lib_path_; }
  const std::string &CreatorName() { return creator_name_; }
  FunctionType Type() { return func_type_; }
  virtual ~NativeDataStream() = default;

 private:
  // Library filesystem path.
  std::string native_lib_path_;
  // Library function creator name that will be used for indexing factory function symbol.
  std::string creator_name_;
  FunctionType func_type_;
};

class DataStreamSource : public DataStream {
 public:
  static std::shared_ptr<DataStreamSource> FromSource(
      std::shared_ptr<StreamingContext> streaming_context,
      std::shared_ptr<SourceFunction> source_func);
  static std::shared_ptr<DataStreamSource> FromSource(
      std::shared_ptr<StreamingContext> streaming_context,
      std::function<LocalRecord(void)> &&source_func);
  virtual ~DataStreamSource() = default;

 private:
  DataStreamSource(std::shared_ptr<StreamingContext> streaming_context,
                   std::shared_ptr<SourceFunction> source_func);
};

class NativeDataStreamSource : public NativeDataStream {
 public:
  static std::shared_ptr<NativeDataStreamSource> FromSource(
      std::shared_ptr<StreamingContext> streaming_context, const std::string &lib_path,
      const std::string &creator_name);
  virtual ~NativeDataStreamSource() = default;

 private:
  NativeDataStreamSource(std::shared_ptr<StreamingContext> streaming_context,
                         const std::string &lib_path, const std::string &creator_name);
};

class DataStreamSink : public DataStream {
 public:
  DataStreamSink(DataStream *input, std::shared_ptr<SinkOperator> sink_operator);
  virtual ~DataStreamSink();
};

class NativeDataStreamSink : public NativeDataStream {
 public:
  NativeDataStreamSink(DataStream *input, const std::string &lib_path,
                       const std::string &creator_name);
  virtual ~NativeDataStreamSink() = default;
};

using NativeOperatorConvertFunction =
    std::function<std::shared_ptr<Operator>(std::shared_ptr<NativeDataStream>)>;
static NativeOperatorConvertFunction empty_convert;

class StreamingContext {
 public:
  int32_t GeneratedId();
  void WithConfig(const std::unordered_map<std::string, std::string> &job_config);
  std::unordered_map<std::string, std::string> &GetConfig();

  template <typename T>
  std::shared_ptr<T> AddStream(T *stream) {
    std::shared_ptr<T> data_stream(stream);
    this->stream_vec_.push_back(std::dynamic_pointer_cast<DataStream>(data_stream));
    return data_stream;
  }

  std::vector<std::shared_ptr<DataStream>> AllStreamSink();

  std::shared_ptr<ChainedSourceOperator> BuildLocalChainedOperator(
      const NativeOperatorConvertFunction converter = empty_convert);

  static std::shared_ptr<StreamingContext> BuildContext();

  virtual ~StreamingContext();

 private:
  StreamingContext();

 private:
  // Sink stream is leaf node of DAG, we can access the whole DAG
  // vertices by indexing upstream of one stream.
  std::vector<std::shared_ptr<DataStream>> stream_vec_;
  std::string job_name_;
  std::unordered_map<std::string, std::string> job_config_;
  std::atomic<int32_t> id_generator_;
};

}  // namespace streaming
}  // namespace ray