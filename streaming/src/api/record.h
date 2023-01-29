#pragma once
#include <iostream>
#include <memory>
#include <string>

namespace ray {
namespace streaming {

/// Record<T> is for generic-type exchange among different
/// operators.
template <class T>
class Record {
 public:
  Record(T &value) : value_(value) {}
  Record(T &&value) : value_(value) {}
  Record(const Record &record) {
    value_ = record.value_;
    stream_name_ = record.stream_name_;
    retract_ = record.retract_;
  }
  T &GetValue() { return value_; }
  void SetValue(T &value) { value_ = value; }
  void SetValue(T &&value) { value_ = value; }
  void SetStream(std::string &stream_name) { stream_name_ = stream_name; }
  std::string &GetStream() { return stream_name_; }
  bool IsRetract() { return retract_; }
  void SetRetract(bool retract) { retract_ = retract; }

  virtual ~Record() = default;

 protected:
  T value_;
  std::string stream_name_;
  bool retract_;
  // For window timer later.
  int64_t timestamp_ = -1;
};

/// RecordBuffer interface wrap entries that would be changed
/// dynamic in runtime.
class RecordBuffer {
 public:
  virtual uint8_t *Data() = 0;
  virtual const size_t Size() = 0;
  virtual ~RecordBuffer() = default;
};

/// Why this local record we need?
/// Many record symbols object might be created if users
/// compile their function library with libstreaming_api.a
/// together, so the samoe record buffer will be double-free.
/// To avoid above situation, it's suggested to create a non-generic
/// data object named local record whos implementation should be
/// hidden in source files.
class LocalRecord {
 public:
  LocalRecord(std::shared_ptr<RecordBuffer> &value) : value_(value) {}
  LocalRecord(std::shared_ptr<RecordBuffer> &&value) : value_(value) {}
  LocalRecord(const LocalRecord &record);
  std::shared_ptr<RecordBuffer> GetValue() { return value_; }
  void SetValue(std::shared_ptr<RecordBuffer> &value) { value_ = value; }
  void SetValue(std::shared_ptr<RecordBuffer> &&value) { value_ = value; }
  void SetStream(std::string &stream_name) { stream_name_ = stream_name; }
  std::string &GetStream() { return stream_name_; }
  bool IsRetract() { return retract_; }
  void SetRetract(bool retract) { retract_ = retract; }

  virtual ~LocalRecord();

 protected:
  std::shared_ptr<RecordBuffer> value_;
  std::string stream_name_;
  bool retract_;
  // For window timer later.
  int64_t timestamp_ = -1;
};

// using LocalRecord = Record<std::shared_ptr<RecordBuffer>>;

LocalRecord BuildRecordFromBuffer(uint8_t *data, uint32_t data_size);

}  // namespace streaming
}  // namespace ray