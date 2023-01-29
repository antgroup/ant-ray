#include "dynamic_step.h"
namespace ray {
namespace streaming {

uint64_t StreamingDynamicStep::kMaxStep = 0xff;
uint64_t StreamingDynamicStep::kMinStep = 4;

StreamingDynamicStep::StreamingDynamicStep() : step_(StreamingDynamicStep::kMinStep) {}

void StreamingDynamicStep::InitStep(uint64_t init_step) { step_ = init_step; }

StreamingAverageStep::StreamingAverageStep() : stable_cnt_(1) {}

// For example, if given init step 10 and step series [2,4,6,7,4,5,1,5,5,5].
// Averagestep updater will produce step vector is following :
// session 1 : [(10 + 2) / 2] = 6, stable count = 1
// session 2 : [(6 + 4)  / 2] = 5, stable count = 1
// session 3 : [(5 + 6)  / 2] = 5, stable count = 2
// session 4 : [(5 + 7)  / 2] = 6, stable count = 1
// session 5 : [(6 + 4)  / 2] = 5, stable count = 1
// session 6 : [(5 + 5)  / 2] = 5, stable count = 2
// session 7 : [(5 + 1)  / 2] = 3, stable count = 1, but 3 < 4(min step), final step 4
// session 8 : [(4 + 5)  / 2] = 4, stable count = 2
// session 9 : [(4 + 5)  / 2] = 4, stable count = 3, final step 4 * 2 = 8
// session 10: [(8 + 5)  / 2] = 6, stable count = 1
// See more in UnitTest streaming_dynamic_step_tests

void StreamingAverageStep::UpdateStep(uint64_t step) {
  uint64_t orignal_step = GetStep();
  uint64_t new_step = (step + orignal_step) / 2;
  if (orignal_step == new_step) {
    if (++stable_cnt_ >= 3) {
      new_step *= 2;
    }
  } else {
    stable_cnt_ = 1;
  }
  if (new_step > StreamingDynamicStep::kMaxStep) {
    new_step = kMaxStep;
  }
  if (new_step < kMinStep) {
    new_step = kMinStep;
  }
  StreamingDynamicStep::UpdateStep(new_step);
};
}  // namespace streaming
}  // namespace ray
