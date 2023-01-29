#include <cstring>
#include <string>

#include "function_test.h"
#include "function_utils.h"
#include "gtest/gtest.h"
#include "runtime/task.h"
#include "runtime/worker.h"

using namespace ray;
using namespace ray::streaming;
std::string example_so_path;

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

  std::shared_ptr<SourceProcessor> source_processor(
      new SourceProcessor(std::dynamic_pointer_cast<SourceOperator>(chained_operator)));
  std::shared_ptr<StreamRuntimeContext> stream_context(
      new StreamRuntimeContext(*runtime_context.get()));
  source_processor->Open(collectors, stream_context);
  LocalRecord local_record(BuildRecordFromBuffer(nullptr, 0));
  source_processor->Process(local_record);
}

TEST(LocalStreamTask, LocalSourceOperatorTest) {
  auto stream_context = StreamingContext::BuildContext();
  {
    // load、resolve symbols
    std::shared_ptr<SourceFunction> source_func(new TestSourceFunction());
    std::shared_ptr<MapFunction> map_func1(new TestMapFunction());
    std::shared_ptr<SinkFunction> sink_func(new ConsoleSinkFunction());
    DataStreamSource::FromSource(stream_context, source_func)
        ->Map(map_func1)
        ->Sink(sink_func);
  }
  std::shared_ptr<LocalStreamTask> local_stream_task(new LocalStreamTask(stream_context));
  local_stream_task->Open();
  for (int i = 0; i < 10; ++i) {
    local_stream_task->Run();
  }
}

TEST(LocalStreamTask, LocalLambdaTest) {
  auto stream_context = StreamingContext::BuildContext();
  {
    DataStreamSource::FromSource(stream_context,
                                 []() {
                                   STREAMING_LOG(INFO) << "Lambda source function.";
                                   return BuildRecord();
                                 })
        ->Map([](LocalRecord &record) {
          STREAMING_LOG(INFO) << "Lambda map function.";
          return record;
        })
        ->Sink(
            [](LocalRecord &record) { STREAMING_LOG(INFO) << "Lambda sink functin."; });
  }
  std::shared_ptr<LocalStreamTask> local_stream_task(new LocalStreamTask(stream_context));
  local_stream_task->Open();
  local_stream_task->Run();
}

TEST(DynamicLoad, boost_load_lambda) {
  auto func = FunctionUtils::GetFunctionByLoad(example_so_path,
                                               "CreateLambdaExampleSourceFunction");
  auto stream_context = StreamingContext::BuildContext();
  {
    std::shared_ptr<Function> source_func(func);
    DataStreamSource::FromSource(stream_context,
                                 std::dynamic_pointer_cast<SourceFunction>(source_func))
        ->Map([](LocalRecord &record) {
          STREAMING_LOG(INFO) << "Lambda map function.";
          return record;
        })
        ->Sink(
            [](LocalRecord &record) { STREAMING_LOG(INFO) << "Lambda sink functin."; });
  }
  std::shared_ptr<LocalStreamTask> local_stream_task(new LocalStreamTask(stream_context));
  local_stream_task->Open();
  local_stream_task->Run();
}

TEST(DynamicLoad, boost_load_class) {
  auto func =
      FunctionUtils::GetFunctionByLoad(example_so_path, "CreateExampleSourceFunction");
  auto stream_context = StreamingContext::BuildContext();
  {
    std::shared_ptr<Function> source_func(func);
    DataStreamSource::FromSource(stream_context,
                                 std::dynamic_pointer_cast<SourceFunction>(source_func))
        ->Map([](LocalRecord &record) {
          STREAMING_LOG(INFO) << "Lambda map function.";
          return record;
        })
        ->Sink(
            [](LocalRecord &record) { STREAMING_LOG(INFO) << "Lambda sink functin."; });
  }
  std::shared_ptr<LocalStreamTask> local_stream_task(new LocalStreamTask(stream_context));
  local_stream_task->Open();
  local_stream_task->Run();
}

TEST(DynamicLoad, all_symbol_function) {
  auto stream_context = StreamingContext::BuildContext();
  {
    NativeDataStreamSource::FromSource(stream_context, example_so_path,
                                       "CreateLambdaExampleSourceFunction")
        ->Map(example_so_path, "CreateLambdaExampleMapFunction")
        ->Sink(example_so_path, "CreateLambdaExampleSinkFunction");
  }
  std::shared_ptr<LocalStreamTask> local_stream_task(new LocalStreamTask(stream_context));
  local_stream_task->Open();
  local_stream_task->Run();
}

TEST(Worker, WorkerRunLocalFunction) {
  Worker worker_main;
  worker_main.RunLocalFunction(example_so_path, "CreateExampleSourceFunction");
  worker_main.RunLocalFunction(example_so_path, "CreateLambdaExampleSourceFunction");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  example_so_path = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
