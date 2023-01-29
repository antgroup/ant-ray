#ifndef RAY_STREAMING_DYNAMIC_STEP_H
#define RAY_STREAMING_DYNAMIC_STEP_H
#include <cstdint>
namespace ray {
namespace streaming {
class StreamingDynamicStep {
 public:
  StreamingDynamicStep();
  virtual void InitStep(uint64_t init_step);
  virtual inline void UpdateStep(uint64_t step) { step_ = step; };
  virtual inline uint64_t GetStep() { return step_; };
  virtual ~StreamingDynamicStep() = default;

  virtual inline void SetMaxStep(uint64_t max_step) { kMaxStep = max_step; };

 public:
  static uint64_t kMaxStep;
  static uint64_t kMinStep;

 private:
  uint64_t step_;
};

class StreamingDefaultStep : public StreamingDynamicStep {
 public:
  inline void UpdateStep(uint64_t step) override{
      // Skip update step in default mode
  };
};

class StreamingAverageStep : public StreamingDynamicStep {
 public:
  StreamingAverageStep();
  void UpdateStep(uint64_t step) override;

 private:
  uint32_t stable_cnt_;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_DYNAMIC_STEP_H
