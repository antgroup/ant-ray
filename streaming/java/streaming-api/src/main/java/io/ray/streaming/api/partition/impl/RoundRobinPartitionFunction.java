package io.ray.streaming.api.partition.impl;

import io.ray.streaming.api.partition.Partition;

/**
 * Partition record to downstream tasks in a round-robin matter.
 *
 * @param <T> Type of the input record.
 */
public class RoundRobinPartitionFunction<T> implements Partition<T> {
  private int seq;
  private int[] partitions = new int[1];

  public RoundRobinPartitionFunction() {
    this.seq = 0;
  }

  @Override
  public int[] partition(T value, int currentIndex, int numPartition) {
    seq = (seq + 1) % numPartition;
    partitions[0] = seq;
    return partitions;
  }

  @Override
  public int[] partition(T record, int numPartition) {
    // TODO
    return new int[0];
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.forward;
  }
}
