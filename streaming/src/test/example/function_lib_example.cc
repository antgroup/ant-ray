#include "function_lib_example.h"

#include <iostream>

#include "api/record.h"

class ExampleSourceFunction : public ray::streaming::SourceFunction {
 public:
  void Init(int parallelism, int index) {}
  void Fetch(std::shared_ptr<ray::streaming::SourceContext> &source_context,
             int checkpoint_id) {
    std::cout << "Fetch data from example source function." << std::endl;
    uint8_t data[] = {0x01, 0x02, 0x03};
    source_context->Collect(ray::streaming::BuildRecordFromBuffer(data, 3));
  }
  void Open(std::shared_ptr<ray::streaming::RuntimeContext> &runtime_context) {
    std::cout << "In Example source function open." << std::endl;
  }
};
ray::streaming::Function *CreateExampleSourceFunction() {
  std::cout << "New example source function" << std::endl;
  return new ExampleSourceFunction();
}

ray::streaming::Function *CreateLambdaExampleSourceFunction() {
  std::cout << "New example source function" << std::endl;
  auto func = new ray::streaming::SourceLambdaFunction([]() {
    std::cout << "Lambda source function." << std::endl;
    uint8_t data[] = {0x01, 0x02, 0x03};
    return ray::streaming::BuildRecordFromBuffer(data, 3);
  });
  return func;
}

ray::streaming::Function *CreateLambdaExampleMapFunction() {
  std::cout << "New example map function" << std::endl;
  ;
  auto func =
      new ray::streaming::MapLambdaFunction([](ray::streaming::LocalRecord &record) {
        std::cout << "Lambda map function." << std::endl;
        return record;
      });
  return func;
}

ray::streaming::Function *CreateLambdaExampleSinkFunction() {
  std::cout << "New example map function" << std::endl;
  auto func =
      new ray::streaming::SinkLambdaFunction([](ray::streaming::LocalRecord &record) {
        std::cout << "Lambda sink functin." << std::endl;
      });
  return func;
}