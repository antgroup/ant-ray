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

#include "ray/exported_api/cc_api.h"
#include "ray/common/id.h"

// #include "ray/core_worker/core_worker.h"

using ::ray_exported::ray::ObjectID;

namespace ray_exported {
namespace ray {

ObjectID ObjectID::FromBinary(const std::string &binary) {
  auto id = ::ray::ObjectID::FromBinary(binary);
  void *ptr = reinterpret_cast<void *>(new ::ray::ObjectID(id));
  ObjectID v(ptr);
  return v;
}

ObjectID::~ObjectID() {
  delete reinterpret_cast<::ray::ObjectID *>(raw_ptr_);
  raw_ptr_ = nullptr;
}

ObjectID::ObjectID(void *raw_ptr) : raw_ptr_(raw_ptr) {}

ObjectID::ObjectID(const ObjectID &id) {
  raw_ptr_ = new ::ray::ObjectID(*reinterpret_cast<::ray::ObjectID *>(id.RawPtr()));
}

ObjectID &ObjectID::operator=(const ObjectID &id) {
  delete reinterpret_cast<::ray::ObjectID *>(raw_ptr_);
  raw_ptr_ = new ::ray::ObjectID(*reinterpret_cast<::ray::ObjectID *>(id.RawPtr()));
  return *this;
}

std::ostream &operator<<(std::ostream &os, const ObjectID &id) {
  return os << reinterpret_cast<::ray::ObjectID *>(id.RawPtr());
}

ray::ObjectID::ObjectID(ObjectID &&id) : raw_ptr_(id.raw_ptr_) { id.raw_ptr_ = nullptr; }

ObjectID &ray::ObjectID::operator=(ObjectID &&id) {
  raw_ptr_ = id.raw_ptr_;
  id.raw_ptr_ = nullptr;
  return *this;
}

ActorID ActorID::FromBinary(const std::string &binary) {
  auto id = ::ray::ActorID::FromBinary(binary);
  void *ptr = reinterpret_cast<void *>(new ::ray::ActorID(id));
  ActorID v(ptr);
  return v;
}

ActorID::~ActorID() {
  delete reinterpret_cast<::ray::ActorID *>(raw_ptr_);
  raw_ptr_ = nullptr;
}

ActorID::ActorID(void *raw_ptr) : raw_ptr_(raw_ptr) {}

ActorID::ActorID(const ActorID &id) {
  raw_ptr_ = new ::ray::ActorID(*reinterpret_cast<::ray::ActorID *>(id.RawPtr()));
}

ActorID &ActorID::operator=(const ActorID &id) {
  delete reinterpret_cast<::ray::ActorID *>(raw_ptr_);
  raw_ptr_ = new ::ray::ActorID(*reinterpret_cast<::ray::ActorID *>(id.RawPtr()));
  return *this;
}

std::ostream &operator<<(std::ostream &os, const ActorID &id) {
  return os << reinterpret_cast<::ray::ActorID *>(id.RawPtr());
}

ray::ActorID::ActorID(ActorID &&id) : raw_ptr_(id.raw_ptr_) { id.raw_ptr_ = nullptr; }

ActorID &ray::ActorID::operator=(ActorID &&id) {
  raw_ptr_ = id.raw_ptr_;
  id.raw_ptr_ = nullptr;
  return *this;
}

}  // namespace ray
}  // namespace ray_exported

#define DEFINE_HASH(type)                                                    \
  std::size_t std::hash<::ray_exported::ray::type>::operator()(       \
      const ::ray_exported::ray::type &value) const {                 \
    return reinterpret_cast<::ray::type *>(value.RawPtr())->Hash();          \
  }                                                                          \
  std::size_t std::hash<const ::ray_exported::ray::type>::operator()( \
      const ::ray_exported::ray::type &value) const {                 \
    return reinterpret_cast<::ray::type *>(value.RawPtr())->Hash();          \
  }

DEFINE_HASH(ObjectID);
DEFINE_HASH(ActorID);

#undef DEFINE_HASH
