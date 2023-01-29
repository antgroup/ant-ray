#include "function.h"

#include "logging.h"
#include "utility.h"

namespace ray {
namespace streaming {

void ConsoleSinkFunction::Sink(LocalRecord &record) {
  STREAMING_LOG(INFO) << "Sink "
                      << StreamingUtility::Byte2hex(record.GetValue()->Data(),
                                                    record.GetValue()->Size());
}
void ConsoleSinkFunction::Open(std::shared_ptr<RuntimeContext> &runtime_context) {
  STREAMING_LOG(INFO) << "Open console sink function.";
}

MapLambdaFunction::MapLambdaFunction(std::function<LocalRecord(LocalRecord &)> &&map_func)
    : map_func_(map_func) {}

LocalRecord MapLambdaFunction::Map(LocalRecord &record) {
  return this->map_func_(record);
}

SinkLambdaFunction::SinkLambdaFunction(std::function<void(LocalRecord &)> &&sink_func)
    : sink_func_(sink_func) {}
void SinkLambdaFunction::Sink(LocalRecord &record) { this->sink_func_(record); }

SourceLambdaFunction::SourceLambdaFunction(std::function<LocalRecord(void)> &&source_func)
    : source_func_(source_func) {}

void SourceLambdaFunction::Fetch(std::shared_ptr<SourceContext> &source_context,
                                 int checkpoint_id) {
  source_context->Collect(this->source_func_());
}

}  // namespace streaming
}  // namespace ray