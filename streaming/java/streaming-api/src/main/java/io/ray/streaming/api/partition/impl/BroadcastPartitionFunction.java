package io.ray.streaming.api.partition.impl;

import io.ray.streaming.api.partition.Partition;
import java.util.stream.IntStream;

/** Broadcast the record to all downstream partitions. */
public class BroadcastPartitionFunction<T> implements Partition<T> {
  private int[] partitions = new int[0];

  public BroadcastPartitionFunction() {}

  @Override
  public int[] partition(T value, int currentIndex, int numPartition) {
    if (partitions.length != numPartition) {
      partitions = IntStream.rangeClosed(0, numPartition - 1).toArray();
    }
    return partitions;
  }

  @Override
  public int[] partition(T record, int numPartition) {
    return new int[0];
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.broadcast;
  }
}
