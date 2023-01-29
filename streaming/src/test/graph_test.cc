
#include "runtime/graph.h"

#include <cstring>
#include <string>

#include "gtest/gtest.h"

using namespace ray;
using namespace ray::streaming;

TEST(JobGraph, graph_pack) {
  JobVertex vertex = {.vertex_id = 0,
                      .vertex_lib_path = "/a/b",
                      .vertex_creator_name = "name",
                      .vertex_type = JobVertexType::SOURCE};
  auto buffer = ray::internal::Serializer::Serialize(vertex);
  auto vertex_copy =
      ray::internal::Serializer::Deserialize<JobVertex>(buffer.data(), buffer.size());
  EXPECT_EQ(vertex.vertex_id, vertex_copy.vertex_id);
  EXPECT_EQ(vertex.vertex_lib_path, vertex_copy.vertex_lib_path);
  EXPECT_EQ(vertex.vertex_creator_name, vertex_copy.vertex_creator_name);
  EXPECT_EQ(vertex.vertex_type, vertex_copy.vertex_type);
}

TEST(JobGraph, graph_vec_pack) {
  JobVertex vertex1 = {.vertex_id = 0,
                       .vertex_lib_path = "/a/b",
                       .vertex_creator_name = "name",
                       .vertex_type = JobVertexType::SOURCE};

  JobVertex vertex2 = {.vertex_id = 1,
                       .vertex_lib_path = "/a/b",
                       .vertex_creator_name = "name",
                       .vertex_type = JobVertexType::MAP};

  JobVertex vertex3 = {.vertex_id = 2,
                       .vertex_lib_path = "/a/b",
                       .vertex_creator_name = "name",
                       .vertex_type = JobVertexType::SINK};

  JobGraph graph;
  graph.vertex_list.push_back(vertex1);
  graph.vertex_list.push_back(vertex2);
  graph.vertex_list.push_back(vertex3);

  auto buffer = ray::internal::Serializer::Serialize(graph);
  auto graph_copy =
      ray::internal::Serializer::Deserialize<JobGraph>(buffer.data(), buffer.size());
  EXPECT_EQ(graph_copy.vertex_list[0].vertex_id, graph.vertex_list[0].vertex_id);
  EXPECT_EQ(graph_copy.vertex_list[1].vertex_type, graph.vertex_list[1].vertex_type);
}

TEST(JobGraph, native_stream_2_job_vertex) {
  std::shared_ptr<StreamingContext> stream_context = StreamingContext::BuildContext();
  std::shared_ptr<NativeDataStream> native_data_stream(
      new NativeDataStream(stream_context, "a.so", "functiona", FunctionType::SOURCE));

  JobVertex vertex = {.vertex_id = native_data_stream->Id(),
                      .vertex_lib_path = native_data_stream->LibPath(),
                      .vertex_creator_name = native_data_stream->CreatorName(),
                      .vertex_type = JobVertexType::SOURCE};
  auto vertex_copy = JobVertex::BuildFromStream(native_data_stream);
  EXPECT_EQ(vertex.vertex_id, vertex_copy.vertex_id);
  EXPECT_EQ(vertex.vertex_lib_path, vertex_copy.vertex_lib_path);
  EXPECT_EQ(vertex.vertex_creator_name, vertex_copy.vertex_creator_name);
  EXPECT_EQ(vertex.vertex_type, vertex_copy.vertex_type);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
