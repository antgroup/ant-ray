#include "streaming_test_suite.h"

namespace ray {
namespace streaming {

const std::shared_ptr<RayFunction> StreamingQueueTestSuite::ASYNC_CALL_FUNC =
    std::make_shared<RayFunction>(
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(ray::Language::PYTHON,
                                                   {"", "", "async_call_func", ""}));
const std::shared_ptr<RayFunction> StreamingQueueTestSuite::SYNC_CALL_FUNC =
    std::make_shared<RayFunction>(
        ray::Language::PYTHON,
        ray::FunctionDescriptorBuilder::FromVector(ray::Language::PYTHON,
                                                   {"", "", "sync_call_func", ""}));

}  // namespace streaming
}  // namespace ray