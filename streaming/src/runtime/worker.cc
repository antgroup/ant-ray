#include "worker.h"

#include "function_utils.h"
#include "logging.h"

namespace ray {
namespace streaming {

bool Worker::RegisterContext(const std::string conf) {
  STREAMING_LOG(INFO) << "Register config " << conf;
  return true;
}

void Worker::RunLocalFunction(const std::string lib_path,
                              const std::string function_creator) {
  STREAMING_LOG(INFO) << "Run local function " << function_creator << "from " << lib_path;
  auto func = FunctionUtils::GetFunctionByLoad(lib_path, function_creator);
  STREAMING_LOG(INFO) << "Build context";
  auto stream_context = StreamingContext::BuildContext();
  STREAMING_LOG(INFO) << "New source func";
  std::shared_ptr<SourceFunction> source_func(reinterpret_cast<SourceFunction *>(func));
  STREAMING_LOG(INFO) << "Create data stream pipeline.";
  DataStreamSource::FromSource(stream_context, source_func)
      ->Map([](LocalRecord &record) {
        STREAMING_LOG(INFO) << "Lambda map function.";
        return record;
      })
      ->Sink([](LocalRecord &record) { STREAMING_LOG(INFO) << "Lambda sink functin."; });
  STREAMING_LOG(INFO) << "Create local stream task.";
  std::shared_ptr<LocalStreamTask> local_stream_task(new LocalStreamTask(stream_context));
  local_stream_task->Open();
  local_stream_task->Run();
}

Worker *CreateWorker() {
  STREAMING_LOG(INFO) << "Create Worker.";
  return new Worker();
}
}  // namespace streaming
}  // namespace ray