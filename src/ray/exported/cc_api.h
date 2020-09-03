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

#include <memory>
#include <ostream>
#include <string>

namespace ray_exported {
namespace ray {

class ObjectID {
 public:
  static ObjectID FromBinary(const std::string &binary);
  ~ObjectID();
  void *RawPtr() const { return raw_ptr_; };

  ObjectID(const ObjectID &);
  ObjectID &operator=(const ObjectID &);
  ObjectID(ObjectID &&);
  ObjectID &operator=(ObjectID &&);

 private:
  explicit ObjectID(void *);
  void *raw_ptr_;
};

std::ostream &operator<<(std::ostream &os, const ObjectID &id);

class ActorID {
 public:
  static ActorID FromBinary(const std::string &binary);
  ~ActorID();
  void *RawPtr() const { return raw_ptr_; };

  ActorID(const ActorID &);
  ActorID &operator=(const ActorID &);
  ActorID(ActorID &&);
  ActorID &operator=(ActorID &&);

 private:
  explicit ActorID(void *);
  void *raw_ptr_;
};

std::ostream &operator<<(std::ostream &os, const ActorID &id);

}  // namespace ray
}  // namespace ray_exported_cc_api

namespace std {
#define DECLARE_HASH(type)                                               \
  template <>                                                            \
  struct hash<::ray_exported_cc_api::ray::type> {                        \
    size_t operator()(const ::ray_exported_cc_api::ray::type &id) const; \
  };                                                                     \
  template <>                                                            \
  struct hash<const ::ray_exported_cc_api::ray::type> {                  \
    size_t operator()(const ::ray_exported_cc_api::ray::type &id) const; \
  };

// DECLARE_HASH(ActorID);
DECLARE_HASH(ObjectID);
DECLARE_HASH(ActorID);

#undef DECLARE_HASH
}  // namespace std
