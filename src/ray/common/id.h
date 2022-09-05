// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <inttypes.h>
#include <limits.h>

#include <boost/flyweight.hpp>
#include <chrono>
#include <cstring>
#include <msgpack.hpp>
#include <mutex>
#include <random>
#include <string>

#include "ray/common/constants.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "ray/util/visibility.h"

namespace ray {

class TaskID;
class WorkerID;
class UniqueID;
class JobID;

/// TODO(qwang): These 2 helper functions should be removed
/// once we separated the `WorkerID` from `UniqueID`.
///
/// A helper function that get the `DriverID` of the given job.
WorkerID ComputeDriverIdFromJob(const JobID &job_id);

using ObjectIDIndexType = uint32_t;
// Declaration.
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

inline unsigned char hex_to_uchar(const char c, bool &err) {
  unsigned char num = 0;
  if (c >= '0' && c <= '9') {
    num = c - '0';
  } else if (c >= 'a' && c <= 'f') {
    num = c - 'a' + 0xa;
  } else if (c >= 'A' && c <= 'F') {
    num = c - 'A' + 0xA;
  } else {
    err = true;
  }
  return num;
}

/// The `ID`s of Ray.
///
/// Please refer to the specification of Ray UniqueIDs.
/// https://github.com/ray-project/ray/blob/master/src/ray/design_docs/id_specification.md

template <typename T, size_t LENGTH>
class BaseID {
 public:
  static constexpr int64_t kLength = LENGTH;

  BaseID() { id_ = std::string(kLength, 0xff); }

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static T FromRandom() {
    std::string data(T::Size(), 0);
    FillRandom(&data);
    return T::FromBinary(data);
  }

  static T FromBinary(const std::string &binary) {
    RAY_CHECK(binary.size() == T::Size() || binary.size() == 0)
        << "expected size is " << T::Size() << ", but got data size is " << binary.size();
    T t;
    if (binary.size() > 0) {
      t.id_ = binary;
    }
    return t;
  }

  static T FromHex(const std::string &hex_str) {
    T id;

    if (2 * T::Size() != hex_str.size()) {
      RAY_LOG(ERROR) << "incorrect hex string length: 2 * " << T::Size()
                     << " != " << hex_str.size() << ", hex string: " << hex_str;
      return T::Nil();
    }

    std::string binary(T::Size(), 0);
    uint8_t *data = reinterpret_cast<uint8_t *>(binary.data());
    for (size_t i = 0; i < T::Size(); i++) {
      char first = hex_str[2 * i];
      char second = hex_str[2 * i + 1];
      bool err = false;
      data[i] = (hex_to_uchar(first, err) << 4) + hex_to_uchar(second, err);
      if (err) {
        RAY_LOG(ERROR) << "incorrect hex character, hex string: " << hex_str;
        return T::Nil();
      }
    }
    id.id_ = binary;

    return id;
  }

  static const T &Nil() {
    static const T nil_id;
    return nil_id;
  }

  static constexpr size_t Size() { return kLength; }

  size_t Hash() const { return MurmurHash64A(Data(), T::Size(), 0); }

  bool IsNil() const {
    static T nil_id = T::Nil();
    return *this == nil_id;
  }

  bool operator==(const BaseID &rhs) const { return id_.get() == rhs.id_.get(); }

  bool operator!=(const BaseID &rhs) const { return !(*this == rhs); }

  const uint8_t *Data() const {
    return reinterpret_cast<const uint8_t *>(id_.get().data());
  }

  std::string Binary() const { return id_.get(); }

  std::string Hex() const {
    constexpr char hex[] = "0123456789abcdef";
    const uint8_t *id = Data();
    std::string result;
    result.reserve(T::Size());
    for (size_t i = 0; i < T::Size(); i++) {
      unsigned int val = id[i];
      result.push_back(hex[val >> 4]);
      result.push_back(hex[val & 0xf]);
    }
    return result;
  }

  MSGPACK_DEFINE(id_);

 protected:
  BaseID(const std::string &binary) {
    RAY_CHECK(binary.size() == Size() || binary.size() == 0)
        << "expected size is " << Size() << ", but got data " << binary << " of size "
        << binary.size();
    if (binary.size() > 0) {
      id_ = binary;
    }
  }

  struct IDTag {};

  boost::flyweight<std::string, boost::flyweights::tag<IDTag>> id_;
};

template <typename T, size_t LENGTH>
size_t hash_value(const BaseID<T, LENGTH> &id) {
  return id.Hash();
}

class UniqueID : public BaseID<UniqueID, kUniqueIDSize> {
 public:
  UniqueID() : BaseID() {}

 protected:
  UniqueID(const std::string &binary);
};

class JobID : public BaseID<JobID, 4> {
 public:
  static JobID FromInt(uint32_t value);

  uint32_t ToInt();

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static JobID FromRandom() = delete;

  JobID() : BaseID() {}
};

class ActorID : public BaseID<ActorID, kActorIDUniqueBytesLength + JobID::kLength> {
 private:
  static constexpr size_t kUniqueBytesLength = kActorIDUniqueBytesLength;

 public:
  /// Creates an `ActorID` by hashing the given information.
  ///
  /// \param job_id The job id to which this actor belongs.
  /// \param parent_task_id The id of the task which created this actor.
  /// \param parent_task_counter The counter of the parent task.
  ///
  /// \return The random `ActorID`.
  static ActorID Of(const JobID &job_id,
                    const TaskID &parent_task_id,
                    const size_t parent_task_counter);

  /// Creates a nil ActorID with the given job.
  ///
  /// \param job_id The job id to which this actor belongs.
  ///
  /// \return The `ActorID` with unique bytes being nil.
  static ActorID NilFromJob(const JobID &job_id);

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static ActorID FromRandom() = delete;

  /// Constructor of `ActorID`.
  ActorID() : BaseID() {}

  /// Get the job id to which this actor belongs.
  ///
  /// \return The job id to which this actor belongs.
  JobID JobId() const;
};

class TaskID : public BaseID<TaskID, kTaskIDUniqueBytesLength + ActorID::kLength> {
 private:
  static constexpr size_t kUniqueBytesLength = kTaskIDUniqueBytesLength;

 public:
  TaskID() : BaseID() {}

  static TaskID ComputeDriverTaskId(const WorkerID &driver_id);

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static TaskID FromRandom() = delete;

  /// The ID generated for driver task.
  static TaskID ForDriverTask(const JobID &job_id);

  /// Generate driver task id for the given job.
  static TaskID FromRandom(const JobID &job_id);

  /// Creates a TaskID for an actor creation task.
  ///
  /// \param actor_id The ID of the actor that will be created
  ///        by this actor creation task.
  ///
  /// \return The ID of the actor creation task.
  static TaskID ForActorCreationTask(const ActorID &actor_id);

  /// Creates a TaskID for actor task.
  ///
  /// \param job_id The ID of the job to which this task belongs.
  /// \param parent_task_id The ID of the parent task which submitted this task.
  /// \param parent_task_counter A count of the number of tasks submitted by the
  ///        parent task before this one.
  /// \param actor_id The ID of the actor to which this task belongs.
  ///
  /// \return The ID of the actor task.
  static TaskID ForActorTask(const JobID &job_id,
                             const TaskID &parent_task_id,
                             size_t parent_task_counter,
                             const ActorID &actor_id);

  /// Creates a TaskID for normal task.
  ///
  /// \param job_id The ID of the job to which this task belongs.
  /// \param parent_task_id The ID of the parent task which submitted this task.
  /// \param parent_task_counter A count of the number of tasks submitted by the
  ///        parent task before this one.
  ///
  /// \return The ID of the normal task.
  static TaskID ForNormalTask(const JobID &job_id,
                              const TaskID &parent_task_id,
                              size_t parent_task_counter);

  /// Given a base task ID, create a task ID that represents the n-th execution
  /// of that task. This task ID should be used to seed any TaskIDs or
  /// ObjectIDs created during that task.
  /// See https://github.com/ray-project/ray/issues/20713.
  ///
  /// \param task_id The task ID generated by the task's parent.
  /// \param attempt_number The current execution number. 0 means the first
  /// execution.
  /// \return The ID of the n-th execution of the task.
  static TaskID ForExecutionAttempt(const TaskID &task_id, uint64_t attempt_number);

  /// Get the id of the actor to which this task belongs.
  ///
  /// \return The `ActorID` of the actor which creates this task.
  ActorID ActorId() const;

  /// Returns whether this is the ID of an actor creation task.
  bool IsForActorCreationTask() const;

  /// Get the id of the job to which this task belongs.
  ///
  /// \return The `JobID` of the job which creates this task.
  JobID JobId() const;
};

class ObjectID : public BaseID<ObjectID, sizeof(ObjectIDIndexType) + TaskID::kLength> {
 private:
  static constexpr size_t kIndexBytesLength = sizeof(ObjectIDIndexType);

 public:
  /// The maximum number of objects that can be returned or put by a task.
  static constexpr int64_t kMaxObjectIndex = ((int64_t)1 << kObjectIdIndexSize) - 1;

  ObjectID() : BaseID() {}

  /// The maximum index of object.
  ///
  /// It also means the max number of objects created (put or return) by one task.
  ///
  /// \return The maximum index of object.
  static uint64_t MaxObjectIndex() { return kMaxObjectIndex; }

  /// Get the index of this object in the task that created it.
  ///
  /// \return The index of object creation according to the task that created
  /// this object.
  ObjectIDIndexType ObjectIndex() const;

  /// Compute the task ID of the task that created the object.
  ///
  /// \return The task ID of the task that created this object.
  TaskID TaskId() const;

  /// Compute the object ID of an object created by a task, either via an object put
  /// within the task or by being a task return object.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index The index of the object created by the task.
  ///
  /// \return The computed object ID.
  static ObjectID FromIndex(const TaskID &task_id, ObjectIDIndexType index);

  /// Create an object id randomly.
  ///
  /// Warning: this can duplicate IDs after a fork() call. We assume this
  /// never happens.
  ///
  /// \return A random object id.
  static ObjectID FromRandom();

  /// Compute the object ID that is used to track an actor's lifetime. This
  /// object does not actually have a value; it is just used for counting
  /// references (handles) to the actor.
  ///
  /// \param actor_id The ID of the actor to track.
  /// \return The computed object ID.
  static ObjectID ForActorHandle(const ActorID &actor_id);

  static bool IsActorID(const ObjectID &object_id);
  static ActorID ToActorID(const ObjectID &object_id);

 private:
  /// A helper method to generate an ObjectID.
  static ObjectID GenerateObjectId(const std::string &task_id_binary,
                                   ObjectIDIndexType object_index = 0);
};

class PlacementGroupID
    : public BaseID<PlacementGroupID,
                    kPlacementGroupIDUniqueBytesLength + JobID::kLength> {
 private:
  static constexpr size_t kUniqueBytesLength = kPlacementGroupIDUniqueBytesLength;

 public:
  /// Creates a `PlacementGroupID` by hashing the given information.
  ///
  /// \param job_id The job id to which this actor belongs.
  ///
  /// \return The random `PlacementGroupID`.
  static PlacementGroupID Of(const JobID &job_id);

  static PlacementGroupID FromRandom() = delete;

  /// Constructor of `PlacementGroupID`.
  PlacementGroupID() : BaseID() {}

  /// Get the job id to which this placement group belongs.
  ///
  /// \return The job id to which this placement group belongs.
  JobID JobId() const;
};

typedef std::pair<PlacementGroupID, int64_t> BundleID;

std::ostream &operator<<(std::ostream &os, const UniqueID &id);
std::ostream &operator<<(std::ostream &os, const JobID &id);
std::ostream &operator<<(std::ostream &os, const ActorID &id);
std::ostream &operator<<(std::ostream &os, const TaskID &id);
std::ostream &operator<<(std::ostream &os, const ObjectID &id);
std::ostream &operator<<(std::ostream &os, const PlacementGroupID &id);

#define DEFINE_UNIQUE_ID(type)                                                           \
  class RAY_EXPORT type : public UniqueID {                                              \
   public:                                                                               \
    explicit type(const UniqueID &from) { id_ = from.Binary(); }                         \
    type() : UniqueID() {}                                                               \
    static type FromRandom() { return type(UniqueID::FromRandom()); }                    \
    static type FromBinary(const std::string &binary) { return type(binary); }           \
    static type Nil() { return type(UniqueID::Nil()); }                                  \
    static constexpr size_t Size() { return kUniqueIDSize; }                             \
                                                                                         \
   private:                                                                              \
    explicit type(const std::string &binary) {                                           \
      RAY_CHECK(binary.size() == Size() || binary.size() == 0)                           \
          << "expected size is " << Size() << ", but got data " << binary << " of size " \
          << binary.size();                                                              \
      if (binary.size() > 0) {                                                           \
        id_ = binary;                                                                    \
      }                                                                                  \
    }                                                                                    \
  };

#include "ray/common/id_def.h"

#undef DEFINE_UNIQUE_ID

// Restore the compiler alignment to default (8 bytes).
#pragma pack(pop)

}  // namespace ray

namespace std {

#define DEFINE_UNIQUE_ID(type)                                           \
  template <>                                                            \
  struct hash<::ray::type> {                                             \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };                                                                     \
  template <>                                                            \
  struct hash<const ::ray::type> {                                       \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };

DEFINE_UNIQUE_ID(UniqueID);
DEFINE_UNIQUE_ID(JobID);
DEFINE_UNIQUE_ID(ActorID);
DEFINE_UNIQUE_ID(TaskID);
DEFINE_UNIQUE_ID(ObjectID);
DEFINE_UNIQUE_ID(PlacementGroupID);
#include "ray/common/id_def.h"

#undef DEFINE_UNIQUE_ID
}  // namespace std
