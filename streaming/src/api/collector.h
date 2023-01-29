#pragma once
#include <memory>

#include "record.h"
namespace ray {
namespace streaming {

/// An interface to transfer data between vertices or
/// downstream and upstream of chained operators.
class Collector {
 public:
  // \param_in record, pass a record/data to next gate.
  virtual void Collect(LocalRecord &record) = 0;
};

class DummyCollector : public Collector {
 public:
  // \param_in record, logging record in console.
  void Collect(LocalRecord &record) override;
};
}  // namespace streaming
}  // namespace ray