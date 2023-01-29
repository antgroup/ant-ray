package io.ray.streaming.jobgraph;

/** Different roles for a node. */
public enum VertexType {
  source,
  process,
  sink,
  union,
  join
}
