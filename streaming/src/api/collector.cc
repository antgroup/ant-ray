#include "collector.h"

#include "logging.h"
#include "utility.h"

namespace ray {
namespace streaming {

void DummyCollector::Collect(LocalRecord &record) {
  STREAMING_LOG(INFO) << "Collect "
                      << StreamingUtility::Byte2hex(record.GetValue()->Data(),
                                                    record.GetValue()->Size());
}

}  // namespace streaming
}  // namespace ray