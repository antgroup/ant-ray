#include <cstring>
#include <string>

#include "api/operator.h"
#include "api/record.h"
#include "api/stream.h"
#include "common/buffer.h"
#include "function_test.h"
#include "gtest/gtest.h"
#include "util/utility.h"

using namespace ray;
using namespace ray::streaming;

TEST(OperatorTest, DummyCollectorTest) {
  std::shared_ptr<DummyCollector> dummy_collector(new DummyCollector());
  uint8_t data[] = {0x01, 0x02, 0x0f, 0x07};
  uint32_t data_size = 4;
  LocalRecord record(BuildRecordFromBuffer(data, data_size));
  dummy_collector->Collect(record);
}

TEST(RecordTest, LocalRecordTest) {
  std::shared_ptr<DummyCollector> dummy_collector(new DummyCollector());

  uint8_t data[] = {0x02, 0x02, 0x0f, 0x07, 0x1f, 0x3f, 0xff};
  uint32_t data_size = 7;
  LocalRecord local_record(BuildRecordFromBuffer(data, data_size));
  std::string stream_name("AnyStream");
  local_record.SetStream(stream_name);
  dummy_collector->Collect(local_record);
  EXPECT_EQ(stream_name, local_record.GetStream());
}

TEST(ForwardCollector, MapFunctionForward) {
  std::shared_ptr<MapFunction> map_func(new TestMapFunction());
  std::shared_ptr<MapOperator> map_operator = std::make_shared<MapOperator>(map_func);
  std::shared_ptr<ForwardCollector> forward_collector =
      std::make_shared<ForwardCollector>(map_operator);
  uint8_t data[] = {0x02, 0x02, 0x0f, 0x07, 0x1f, 0x3f, 0xff};
  uint32_t data_size = 7;
  LocalRecord local_record(BuildRecordFromBuffer(data, data_size));
  forward_collector->Collect(local_record);
  std::shared_ptr<DummyCollector> dummy_collector(new DummyCollector());
  dummy_collector->Collect(local_record);
  EXPECT_EQ(*local_record.GetValue()->Data(), 0x03);
}

TEST(ChainedOneInputOperatorTest, ChainedMapForward) {
  std::shared_ptr<MapFunction> map_func1(new TestMapFunction());
  std::shared_ptr<MapOperator> map_operator1 = std::make_shared<MapOperator>(map_func1);

  std::shared_ptr<MapFunction> map_func2(new TestMapFunction());
  std::shared_ptr<MapOperator> map_operator2 = std::make_shared<MapOperator>(map_func2);

  std::vector<std::shared_ptr<Operator>> operator_vec;
  operator_vec.push_back(map_operator1);
  operator_vec.push_back(map_operator2);

  std::shared_ptr<DummyCollector> dummy_collector(new DummyCollector());
  std::vector<std::shared_ptr<Collector>> collectors;
  collectors.push_back(std::dynamic_pointer_cast<Collector>(dummy_collector));

  std::shared_ptr<RuntimeContext> runtime_context =
      std::make_shared<RuntimeContext>(1, 0, 1);

  std::shared_ptr<ChainedOneInputOperator> chained_operator =
      std::make_shared<ChainedOneInputOperator>(operator_vec);
  chained_operator->Open(collectors, runtime_context);

  uint8_t data[] = {0x02, 0x02, 0x0f, 0x07, 0x1f, 0x3f, 0xff};
  uint32_t data_size = 7;
  LocalRecord local_record(BuildRecordFromBuffer(data, data_size));
  chained_operator->Process(local_record);
  EXPECT_EQ(*local_record.GetValue()->Data(), 0x04);
}

TEST(ChainedSourceOperatorTest, ChainedSourceForward) {
  std::shared_ptr<SourceFunction> source_func(new TestSourceFunction());
  std::shared_ptr<SourceOperator> source_operator =
      std::make_shared<SourceOperator>(source_func);

  std::shared_ptr<MapFunction> map_func2(new TestMapFunction());
  std::shared_ptr<MapOperator> map_operator2 = std::make_shared<MapOperator>(map_func2);

  std::vector<std::shared_ptr<Operator>> operator_vec;
  operator_vec.push_back(source_operator);
  operator_vec.push_back(map_operator2);

  std::shared_ptr<DummyCollector> dummy_collector(new DummyCollector());
  std::vector<std::shared_ptr<Collector>> collectors;
  collectors.push_back(std::dynamic_pointer_cast<Collector>(dummy_collector));

  std::shared_ptr<RuntimeContext> runtime_context =
      std::make_shared<RuntimeContext>(1, 0, 1);

  std::shared_ptr<ChainedSourceOperator> chained_operator =
      std::make_shared<ChainedSourceOperator>(operator_vec);
  chained_operator->Open(collectors, runtime_context);

  chained_operator->Fetch(1);
}

TEST(ChainedSourceOperatorTest, ChainedSourceSinkForward) {
  std::shared_ptr<SourceFunction> source_func(new TestSourceFunction());
  std::shared_ptr<SourceOperator> source_operator =
      std::make_shared<SourceOperator>(source_func);

  std::shared_ptr<SinkFunction> sink_func(new ConsoleSinkFunction());
  std::shared_ptr<SinkOperator> sink_operator = std::make_shared<SinkOperator>(sink_func);

  std::vector<std::shared_ptr<Operator>> operator_vec;
  operator_vec.push_back(source_operator);
  operator_vec.push_back(sink_operator);

  std::vector<std::shared_ptr<Collector>> collectors;

  std::shared_ptr<RuntimeContext> runtime_context =
      std::make_shared<RuntimeContext>(1, 0, 1);

  std::shared_ptr<ChainedSourceOperator> chained_operator =
      std::make_shared<ChainedSourceOperator>(operator_vec);
  chained_operator->Open(collectors, runtime_context);

  chained_operator->Fetch(1);
}

TEST(DataStreamTest, StreamingContextBuild) {
  auto stream_context = StreamingContext::BuildContext();
  {
    std::shared_ptr<SourceFunction> source_func(new TestSourceFunction());
    std::shared_ptr<MapFunction> map_func1(new TestMapFunction());
    std::shared_ptr<SinkFunction> sink_func(new ConsoleSinkFunction());
    std::shared_ptr<DataStream> source =
        DataStreamSource::FromSource(stream_context, source_func);
    EXPECT_EQ(source->Id(), 1);
    std::shared_ptr<DataStream> source2 = source;
    STREAMING_LOG(INFO) << "Link source to map.";
    auto map = source->Map(map_func1);
    STREAMING_LOG(INFO) << "Link map to sink.";
    auto sink = map->Sink(sink_func);
    STREAMING_LOG(INFO) << "Sink stream created.";
    EXPECT_EQ(source2->Id(), 1);
    EXPECT_EQ(map->Id(), 2);
  }
  auto sink_stream_vec = stream_context->AllStreamSink();
  EXPECT_EQ(sink_stream_vec[0]->Id(), 3);
  EXPECT_EQ(sink_stream_vec[0]->InputStream()->Id(), 2);
  EXPECT_EQ(sink_stream_vec[0]->InputStream()->InputStream()->Id(), 1);
}

TEST(DataStreamTest, StreamingContextBuildChain) {
  auto stream_context = StreamingContext::BuildContext();
  {
    std::shared_ptr<SourceFunction> source_func(new TestSourceFunction());
    std::shared_ptr<MapFunction> map_func1(new TestMapFunction());
    std::shared_ptr<SinkFunction> sink_func(new ConsoleSinkFunction());
    DataStreamSource::FromSource(stream_context, source_func)
        ->Map(map_func1)
        ->Sink(sink_func);
    EXPECT_EQ(stream_context->GeneratedId(), 4);
    STREAMING_LOG(INFO) << stream_context.use_count();
  }
  auto sink_stream_vec = stream_context->AllStreamSink();
  EXPECT_EQ(sink_stream_vec[0]->Id(), 3);
  EXPECT_EQ(sink_stream_vec[0]->InputStream()->Id(), 2);
  EXPECT_EQ(sink_stream_vec[0]->InputStream()->InputStream()->Id(), 1);
  STREAMING_LOG(INFO) << stream_context.use_count();

  auto local_chained_operator = stream_context->BuildLocalChainedOperator();
  std::shared_ptr<RuntimeContext> runtime_context =
      std::make_shared<RuntimeContext>(1, 0, 1);

  std::vector<std::shared_ptr<Collector>> collectors;
  local_chained_operator->Open(collectors, runtime_context);

  local_chained_operator->Fetch(1);
}

TEST(LambdaFunction, mapFunction) {
  std::shared_ptr<MapFunction> func =
      std::make_shared<MapLambdaFunction>([](LocalRecord &record) {
        STREAMING_LOG(INFO) << "Userdefined function.";
        return record;
      });
  uint8_t data[] = {0x01, 0x02, 0x0f, 0x07};
  uint32_t data_size = 4;
  LocalRecord record(BuildRecordFromBuffer(data, data_size));
  func->Map(record);
}

TEST(LambdaFunction, sinkFunction) {
  std::shared_ptr<SinkFunction> func = std::make_shared<SinkLambdaFunction>(
      [](LocalRecord &record) { STREAMING_LOG(INFO) << "Userdefined sink function."; });
  uint8_t data[] = {0x01, 0x02, 0x0f, 0x07};
  uint32_t data_size = 4;
  LocalRecord record = BuildRecordFromBuffer(data, data_size);
  func->Sink(record);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
