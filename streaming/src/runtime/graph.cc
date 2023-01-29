#include "graph.h"
namespace ray {
namespace streaming {
JobVertex JobVertex::BuildFromStream(
    std::shared_ptr<NativeDataStream> native_data_stream) {
  return {.vertex_id = native_data_stream->Id(),
          .vertex_lib_path = native_data_stream->LibPath(),
          .vertex_creator_name = native_data_stream->CreatorName(),
          .vertex_type = static_cast<JobVertexType>(native_data_stream->Type())};
}

}  // namespace streaming
}  // namespace ray