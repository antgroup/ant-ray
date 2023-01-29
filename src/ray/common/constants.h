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

#include <limits.h>
#include <stdint.h>
#include <string>
#include <vector>

/// Length of Ray full-length IDs in bytes.
constexpr size_t kUniqueIDSize = 28;

/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. This is the maximum width of the object index in bits.
constexpr int kObjectIdIndexSize = 32;
static_assert(kObjectIdIndexSize % CHAR_BIT == 0,
              "ObjectID prefix not a multiple of bytes");

/// Raylet exit code on plasma store socket error.
constexpr int kRayletStoreErrorExitCode = 100;

/// ANT-INTERNAL:
/// It means that the raylet should restart at current node.
constexpr int kExitAndRestartNodeCode = 119;
/// It means that the raylet should restart at other nodes.
constexpr int kExitAndReplaceNodeCode = 120;
/// It means that the raylet should not restart any more.
constexpr int kExitNodeCode = 121;

/// Prefix for the object table keys in redis.
constexpr char kObjectTablePrefix[] = "ObjectTable";
/// Prefix for the task table keys in redis.
constexpr char kTaskTablePrefix[] = "TaskTable";

constexpr char kWorkerDynamicOptionPlaceholder[] =
    "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER";

constexpr char kNodeManagerPortPlaceholder[] = "RAY_NODE_MANAGER_PORT_PLACEHOLDER";

constexpr char kWorkerCommandJobIdPlaceholder[] = "{job_id}";

/// Public DNS address which is is used to connect and get local IP.
constexpr char kPublicDNSServerIp[] = "8.8.8.8";
constexpr int kPublicDNSServerPort = 53;
constexpr double kMaxResourceCapacity = 512.0;

constexpr char NODEGROUP_RESOURCE_DEFAULT[] = "NAMESPACE_LABEL_RAY_DEFAULT";

constexpr char kEnvVarKeyJobId[] = "RAY_JOB_ID";
constexpr char kEnvVarKeyJobDir[] = "RAY_JOB_DIR";
/// A parent directory of job data.
constexpr char kEnvVarKeyJobDataDirBase[] = "RAY_JOB_DATA_DIR_BASE";
/// for cross-langueage serialization
constexpr int kMessagePackOffset = 9;
/// Filename of "shim process" that sets up Python worker environment.
/// Should be kept in sync with SETUP_WORKER_FILENAME in ray.ray_constants.
constexpr char kSetupWorkerFilename[] = "setup_worker.py";

/// Default job data directory.
/// This static directory needs to be the same as defined in
/// `RuntimeContextImpl.java` and `runtime_context.py`.
constexpr char kDefaultJobDataDirBase[] = "/tmp/ray/data";

constexpr char kDefaultShapeGroup[] = "DEFAULT_SHAPE_GROUP";

constexpr char kLabelKeyActorID[] = "actor_id";

static std::vector<std::string> kPresetLabelKeys = {kLabelKeyActorID};