package io.ray.streaming.api.partition.impl;

import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.util.DynamicRebalanceDetector;

/** Select the channel with the lowest back pressure ratio to send data. */
public class DynamicRebalanceParition<T> implements Partition<T> {
  private int[] partitions = new int[] {0};
  private DynamicRebalanceDetector dynamicRebalanceDetector;

  public DynamicRebalanceParition(DynamicRebalanceDetector dynamicRebalanceDetector) {
    this.dynamicRebalanceDetector = dynamicRebalanceDetector;
  }

  /**
   * Using the channel with the lowest back pressure rate.
   *
   * @param record The record.
   * @param numPartition num of partitions
   */
  @Override
  public int[] partition(T record, int numPartition) {
    partitions[0] = this.dynamicRebalanceDetector.getBestBackpressureRatio();
    return partitions;
  }
}
