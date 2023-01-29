#include "task.h"

#include "function_utils.h"
#include "logging.h"
namespace ray {
namespace streaming {

StreamProcessor::StreamProcessor(std::shared_ptr<Operator> stream_operator)
    : operator_(stream_operator) {}
void StreamProcessor::Open(std::vector<std::shared_ptr<Collector>> &collectors,
                           std::shared_ptr<StreamRuntimeContext> &runtime_context) {
  this->collectors_ = collectors;
  this->runtime_context_ = runtime_context;
  if (this->operator_) {
    auto operator_runtime_context =
        std::dynamic_pointer_cast<RuntimeContext>(runtime_context);
    this->operator_->Open(collectors, operator_runtime_context);
  }
}

void StreamProcessor::Finish() { this->operator_->Finish(); }

void StreamProcessor::Close() { this->operator_->Close(); }

bool StreamProcessor::SaveCheckpoint(int checkpoint_id) {
  return this->operator_->SaveCheckpoint(checkpoint_id);
}

bool StreamProcessor::LoadCheckpoint(int checkpoint_id) {
  return this->operator_->LoadCheckpoint(checkpoint_id);
}

bool StreamProcessor::DeleteCheckpoint(int checkpoint_id) {
  return this->operator_->DeleteCheckpoint(checkpoint_id);
}

SourceProcessor::SourceProcessor(std::shared_ptr<SourceOperator> source_operator)
    : StreamProcessor(source_operator), source_operator_(source_operator) {}

void SourceProcessor::Process(LocalRecord &record) {
  // NOTE(lingxuan.zlx): fetch by specific checkpoint id from runtime context.
  this->source_operator_->Fetch(0);
}
LocalStreamTask::LocalStreamTask(std::shared_ptr<StreamingContext> streaming_context)
    : streaming_context_(streaming_context) {}

void LocalStreamTask::Open() {
  STREAMING_LOG(INFO) << "Local stream task open.";
  auto local_chained_operator = this->streaming_context_->BuildLocalChainedOperator(
      std::bind(&LocalStreamTask::ConvertNativeDataStreamToOperator, this,
                std::placeholders::_1));

  std::vector<std::shared_ptr<Collector>> collectors;
  std::shared_ptr<RuntimeContext> runtime_context =
      std::make_shared<RuntimeContext>(1, 0, 1);

  this->source_processor_ = std::make_shared<SourceProcessor>(
      std::dynamic_pointer_cast<SourceOperator>(local_chained_operator));

  std::shared_ptr<StreamRuntimeContext> stream_context(
      new StreamRuntimeContext(*runtime_context.get()));

  this->source_processor_->Open(collectors, stream_context);
}

void LocalStreamTask::Run() {
  // Trigger an empty local record for local stream task.
  LocalRecord local_record(BuildRecordFromBuffer(nullptr, 0));
  this->source_processor_->Process(local_record);
}

std::shared_ptr<Operator> LocalStreamTask::ConvertNativeDataStreamToOperator(
    std::shared_ptr<NativeDataStream> native_data_stream) {
  if (!native_data_stream) {
    STREAMING_LOG(WARNING) << "Native data stream nullptr.";
    return nullptr;
  }
  STREAMING_LOG(INFO) << "Convert native stream " << native_data_stream->CreatorName()
                      << " to runner from library " << native_data_stream->LibPath();
  auto func = std::shared_ptr<Function>(FunctionUtils::GetFunctionByLoad(
      native_data_stream->LibPath(), native_data_stream->CreatorName()));
  if (FunctionType::SOURCE == native_data_stream->Type()) {
    auto source_func = std::dynamic_pointer_cast<SourceFunction>(func);
    return std::make_shared<SourceOperator>(source_func);
  } else if (FunctionType::MAP == native_data_stream->Type()) {
    auto map_func = std::dynamic_pointer_cast<MapFunction>(func);
    return std::make_shared<MapOperator>(map_func);
  } else if (FunctionType::SINK == native_data_stream->Type()) {
    auto sink_func = std::dynamic_pointer_cast<SinkFunction>(func);
    return std::make_shared<SinkOperator>(sink_func);
  } else {
    STREAMING_LOG(WARNING) << "No such function type found.";
    return nullptr;
  }
}

}  // namespace streaming
}  // namespace ray