package io.ray.streaming.jobgraph;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/** Job edge is connection and partition rules of upstream and downstream execution nodes. */
public class JobEdge implements Serializable {

  private final int sourceVertexId;
  private final int targetVertexId;
  private final int sourceOperatorId;
  private final int targetOperatorId;
  private final Partition<?> partition;

  /**
   * This parameter represents the type of edge, a value of 1 means it is the edge to the right of
   * join stream, a value of 0 means other types of edges.
   */
  private int edgeType;

  /** To represent whether the edge is in a DCG. */
  private boolean cyclic;

  /** Collector for output. Should be set up in runtime. */
  private Collector<?> collector;

  public JobEdge(int sourceVertexId, int targetVertexId, Partition<?> partition) {
    this(sourceVertexId, targetVertexId, sourceVertexId, targetVertexId, partition);
  }

  public JobEdge(
      int sourceVertexId,
      int targetVertexId,
      int sourceOperatorId,
      int targetOperatorId,
      Partition<?> partition) {
    this(sourceVertexId, targetVertexId, sourceOperatorId, targetOperatorId, partition, 0, false);
  }

  public JobEdge(
      int sourceVertexId,
      int targetVertexId,
      int sourceOperatorId,
      int targetOperatorId,
      Partition<?> partition,
      int edgeType,
      boolean cyclic) {
    this.sourceVertexId = sourceVertexId;
    this.targetVertexId = targetVertexId;
    this.sourceOperatorId = sourceOperatorId;
    this.targetOperatorId = targetOperatorId;
    this.partition = partition;
    this.edgeType = edgeType;
    this.cyclic = cyclic;
  }

  public void setEdgeType(int edgeType) {
    this.edgeType = edgeType;
  }

  public int getEdgeType() {
    return this.edgeType;
  }

  public int getSourceVertexId() {
    return sourceVertexId;
  }

  public int getTargetVertexId() {
    return targetVertexId;
  }

  public int getSourceOperatorId() {
    return this.sourceOperatorId;
  }

  public int getTargetOperatorId() {
    return this.targetOperatorId;
  }

  public Partition<?> getPartition() {
    return partition;
  }

  public boolean isCyclic() {
    return cyclic;
  }

  public void setCyclic(boolean isCyclic) {
    this.cyclic = isCyclic;
  }

  public Collector<?> getCollector() {
    return collector;
  }

  public void setCollector(Collector<?> collector) {
    this.collector = collector;
  }

  @Override
  public String toString() {
    return "Edge(" + "from:" + sourceVertexId + "-" + targetVertexId + "-" + this.partition + ")";
  }
}
