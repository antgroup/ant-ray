package io.ray.streaming.jobgraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PlanGraphvizGenerator {

  private JobGraph plan;
  private List<JobEdge> jobEdges;
  private static final String NODE_FORMAT = "%s [label=\"%s\"]\n";

  public PlanGraphvizGenerator(JobGraph plan) {
    this.plan = plan;
    this.jobEdges = new ArrayList<>(plan.getJobEdges());
  }

  public String getGraphviz() {
    Collections.sort(
        jobEdges,
        (o1, o2) -> {
          int i = Integer.compare(o1.getSourceVertexId(), o2.getSourceVertexId());
          if (i == 0) {
            return Integer.compare(o1.getTargetVertexId(), o2.getTargetVertexId());
          } else {
            return i;
          }
        });

    StringBuilder builder = new StringBuilder("digraph G {\n");
    for (JobEdge edge : jobEdges) {
      builder.append(
          String.format(
              "%d -> %d [label = \"%s\"]\n",
              edge.getSourceVertexId(),
              edge.getTargetVertexId(),
              edge.getPartition().getPartitionType()));
    }
    for (JobVertex vertex : plan.getJobVertices()) {
      builder.append(String.format(NODE_FORMAT, vertex.getVertexId(), vertex.getVertexString()));
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public String toString() {
    return plan.toString();
  }
}
