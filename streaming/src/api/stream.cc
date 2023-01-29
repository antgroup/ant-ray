#include "stream.h"

#include "logging.h"

namespace ray {
namespace streaming {

Stream::Stream(std::weak_ptr<StreamingContext> streaming_context, Stream *input_stream,
               std::shared_ptr<Operator> stream_operator)
    : streaming_context_(streaming_context),
      input_stream_(input_stream),
      operator_(stream_operator) {
  id_ = streaming_context.lock()->GeneratedId();
  STREAMING_LOG(INFO) << "Generate stream id " << id_;
}

Stream::Stream(std::weak_ptr<StreamingContext> streaming_context,
               std::shared_ptr<Operator> stream_operator)
    : Stream(streaming_context, nullptr, stream_operator) {}

Stream::Stream(Stream *input_stream, std::shared_ptr<Operator> stream_operator)
    : Stream(input_stream->GetStreamingContext(), input_stream, stream_operator) {}

int32_t Stream::Id() { return id_; }

Stream::~Stream() { STREAMING_LOG(INFO) << "Deconstructor " << id_; }
std::weak_ptr<StreamingContext> Stream::GetStreamingContext() {
  return streaming_context_;
}

Stream *Stream::InputStream() { return input_stream_; }

std::shared_ptr<Operator> Stream::GetOperator() { return operator_; }

DataStream::DataStream(std::shared_ptr<StreamingContext> streaming_context,
                       std::shared_ptr<Operator> stream_operator)
    : Stream(streaming_context, stream_operator) {}

DataStream::DataStream(DataStream *input_stream,
                       std::shared_ptr<Operator> stream_operator)
    : Stream(input_stream, stream_operator) {}

std::shared_ptr<DataStream> DataStream::Map(std::shared_ptr<MapFunction> map_func) {
  // TODO(lingxuan.zlx) : we might check weak_ptr nullptr or not and then access
  // context member function.
  return streaming_context_.lock()->AddStream(
      new DataStream(this, std::make_shared<MapOperator>(map_func)));
}
std::shared_ptr<DataStream> DataStream::Map(
    std::function<LocalRecord(LocalRecord &)> &&map_func) {
  return this->Map(std::make_shared<MapLambdaFunction>(std::move(map_func)));
}

std::shared_ptr<DataStreamSink> DataStream::Sink(
    std::function<void(LocalRecord &)> &&sink_func) {
  return this->Sink(std::make_shared<SinkLambdaFunction>(std::move(sink_func)));
}

std::shared_ptr<DataStreamSink> DataStream::Sink(
    // Same situation with Map function.
    std::shared_ptr<SinkFunction> sink_func) {
  return streaming_context_.lock()->AddStream(
      new DataStreamSink(this, std::make_shared<SinkOperator>(sink_func)));
}

std::shared_ptr<DataStream> DataStream::Self() { return shared_from_this(); }

DataStream::~DataStream() { STREAMING_LOG(INFO) << "Destructor DataStream " << Id(); }

std::shared_ptr<DataStreamSource> DataStreamSource::FromSource(
    std::shared_ptr<StreamingContext> streaming_context,
    std::shared_ptr<SourceFunction> source_func) {
  // NOTE(lingxuan.zlx): there is no weak_ptr in first source created, so we
  // no need pass and use data stream weak_ptr context.
  STREAMING_LOG(INFO) << "From source build.";
  return streaming_context->AddStream(
      new DataStreamSource(streaming_context, source_func));
}

std::shared_ptr<DataStreamSource> DataStreamSource::FromSource(
    std::shared_ptr<StreamingContext> streaming_context,
    std::function<LocalRecord(void)> &&source_func) {
  return DataStreamSource::FromSource(
      streaming_context, std::make_shared<SourceLambdaFunction>(std::move(source_func)));
}

DataStreamSource::DataStreamSource(std::shared_ptr<StreamingContext> streaming_context,
                                   std::shared_ptr<SourceFunction> source_func)
    : DataStream(streaming_context, std::make_shared<SourceOperator>(source_func)) {}

DataStreamSink::DataStreamSink(DataStream *input_stream,
                               std::shared_ptr<SinkOperator> sink_operator)
    : DataStream(input_stream, sink_operator) {}

DataStreamSink::~DataStreamSink() {
  STREAMING_LOG(INFO) << "Destructor DataStream Sink " << Id();
}

StreamingContext::StreamingContext() : id_generator_(0) {}

int32_t StreamingContext::GeneratedId() { return ++id_generator_; }

void StreamingContext::WithConfig(
    const std::unordered_map<std::string, std::string> &job_config) {
  job_config_.insert(job_config.begin(), job_config.end());
}
std::unordered_map<std::string, std::string> &StreamingContext::GetConfig() {
  return job_config_;
}

std::shared_ptr<StreamingContext> StreamingContext::BuildContext() {
  return std::shared_ptr<StreamingContext>(new StreamingContext());
}

std::vector<std::shared_ptr<DataStream>> StreamingContext::AllStreamSink() {
  std::vector<std::shared_ptr<DataStream>> result;
  std::copy_if(this->stream_vec_.begin(), this->stream_vec_.end(),
               std::back_inserter(result), [](std::shared_ptr<DataStream> &data_stream) {
                 return std::dynamic_pointer_cast<DataStreamSink>(data_stream) != nullptr;
               });
  return result;
}

std::shared_ptr<ChainedSourceOperator> StreamingContext::BuildLocalChainedOperator(
    const NativeOperatorConvertFunction convert) {
  std::vector<std::shared_ptr<Operator>> operator_vec;
  for (auto &data_stream : this->stream_vec_) {
    // Load native data stream function from library file.
    auto native_data_stream = std::dynamic_pointer_cast<NativeDataStream>(data_stream);
    if (native_data_stream != nullptr) {
      operator_vec.push_back(convert(native_data_stream));
    } else {
      operator_vec.push_back(data_stream->GetOperator());
    }
  }

  std::shared_ptr<ChainedSourceOperator> chained_operator =
      std::make_shared<ChainedSourceOperator>(operator_vec);
  return chained_operator;
}
StreamingContext::~StreamingContext() {
  STREAMING_LOG(INFO) << "Streaming Context Destrcuting.";
}

NativeDataStream::NativeDataStream(std::shared_ptr<StreamingContext> streaming_context,
                                   const std::string &native_lib_path,
                                   const std::string &creator_name,
                                   FunctionType func_type)
    : DataStream(streaming_context, nullptr),
      native_lib_path_(native_lib_path),
      creator_name_(creator_name),
      func_type_(func_type) {}

NativeDataStream::NativeDataStream(DataStream *input_stream,
                                   const std::string &native_lib_path,
                                   const std::string &creator_name,
                                   FunctionType func_type)
    : DataStream(input_stream, nullptr),
      native_lib_path_(native_lib_path),
      creator_name_(creator_name),
      func_type_(func_type) {}

std::shared_ptr<NativeDataStream> NativeDataStream::Map(
    const std::string &native_lib_path, const std::string &creator_name) {
  return this->streaming_context_.lock()->AddStream(
      new NativeDataStream(this, native_lib_path, creator_name, FunctionType::MAP));
}

std::shared_ptr<NativeDataStreamSink> NativeDataStream::Sink(
    const std::string &native_lib_path, const std::string &creator_name) {
  return this->streaming_context_.lock()->AddStream(
      new NativeDataStreamSink(this, native_lib_path, creator_name));
}

std::shared_ptr<NativeDataStreamSource> NativeDataStreamSource::FromSource(
    std::shared_ptr<StreamingContext> streaming_context, const std::string &lib_path,
    const std::string &creator_name) {
  return streaming_context->AddStream(
      new NativeDataStreamSource(streaming_context, lib_path, creator_name));
}

NativeDataStreamSource::NativeDataStreamSource(
    std::shared_ptr<StreamingContext> streaming_context, const std::string &lib_path,
    const std::string &creator_name)
    : NativeDataStream(streaming_context, lib_path, creator_name, FunctionType::SOURCE) {}
NativeDataStreamSink::NativeDataStreamSink(DataStream *input, const std::string &lib_path,
                                           const std::string &creator_name)
    : NativeDataStream(input, lib_path, creator_name, FunctionType::SINK) {}

}  // namespace streaming
}  // namespace ray