#pragma once
#include <ray/api.h>

#include "function.h"
#include "stream.h"

namespace ray {
namespace streaming {

enum class JobVertexType { SOURCE = 1, MAP, SINK };

/// A jobvertex pojo contains vertex informations.
struct JobVertex {
  static JobVertex BuildFromStream(std::shared_ptr<NativeDataStream> native_data_stream);
  int32_t vertex_id;
  std::string vertex_lib_path;
  std::string vertex_creator_name;
  JobVertexType vertex_type;
  MSGPACK_DEFINE(vertex_id, vertex_lib_path, vertex_creator_name, vertex_type);
};

/// A jobgraph pojo contains all vertex informations of entire graph.
struct JobGraph {
  std::vector<JobVertex> vertex_list;
  std::unordered_map<std::string, std::string> job_config;
  MSGPACK_DEFINE(vertex_list, job_config);
};
}  // namespace streaming
}  // namespace ray

MSGPACK_ADD_ENUM(ray::streaming::JobVertexType);
