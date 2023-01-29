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

#include "ray/common/id_util.h"

namespace ray {

UniqueID GenerateActorWorkerAssignmentId(const JobID &job_id) {
  std::string data(UniqueID::Size(), 0);
  FillRandom(&data);
  std::memcpy(const_cast<char *>(data.data()), job_id.Data(), JobID::Size());
  return UniqueID::FromBinary(data);
}

JobID GetJobIdFromActorWokerAssignmentId(const UniqueID &id) {
  return JobID::FromBinary(
      std::string(reinterpret_cast<const char *>(id.Data()), JobID::Size()));
}

}  // namespace ray