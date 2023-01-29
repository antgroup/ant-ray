#include <cstring>
#include <string>

#include "api/operator.h"
#include "api/record.h"
#include "api/stream.h"
#include "common/buffer.h"
#include "util/utility.h"

using namespace ray;
using namespace ray::streaming;

LocalRecord BuildRecord() {
  uint8_t data[] = {0x02, 0x02, 0x0f, 0x07, 0x1f, 0x3f, 0xff};
  uint32_t data_size = 7;
  LocalRecord local_record(BuildRecordFromBuffer(data, data_size));
  return local_record;
}

class TestMapFunction : public MapFunction {
 public:
  LocalRecord Map(LocalRecord &record) {
    STREAMING_LOG(DEBUG) << "TestMapFunction "
                         << StreamingUtility::Byte2hex(record.GetValue()->Data(),
                                                       record.GetValue()->Size());

    *record.GetValue()->Data() = *record.GetValue()->Data() + 1;
    return record;
  }

  void Open(std::shared_ptr<RuntimeContext> &runtime_context) override {
    STREAMING_LOG(INFO) << "Open TestMapFunction.";
  }
};

class TestSourceFunction : public SourceFunction {
 public:
  void Init(int parallelism, int index) {
    STREAMING_LOG(INFO) << "Init test source function, parallelism " << parallelism
                        << ", index " << index;
  }
  void Fetch(std::shared_ptr<SourceContext> &source_context, int checkpoint_id) {
    auto local_record = BuildRecord();
    source_context->Collect(local_record);
  }
  void Open(std::shared_ptr<RuntimeContext> &runtime_context) override {
    STREAMING_LOG(INFO) << "Open TestSourceFunction.";
  }
};
