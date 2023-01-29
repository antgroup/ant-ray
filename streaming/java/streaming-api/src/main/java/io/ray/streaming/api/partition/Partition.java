package io.ray.streaming.api.partition;

/**
 * Interface of the partitioning strategy.
 *
 * @param <T> Type of the input data.
 */
@FunctionalInterface
public interface Partition<T> {

  /**
   * Given a record and downstream partitions, determine which partition(s) should receive the
   * record.
   *
   * @param record The record.
   * @param currentIndex index of current actor in the group
   * @param numPartition num of partitions
   * @return IDs of the downstream partitions that should receive the record.
   */
  default int[] partition(T record, int currentIndex, int numPartition) {
    return partition(record, numPartition);
  }

  /**
   * Given a record and downstream partitions, determine which partition(s) should receive the
   * record.
   *
   * @param record The record.
   * @param numPartition num of partitions
   * @return IDs of the downstream partitions that should receive the record.
   */
  int[] partition(T record, int numPartition);

  /** @return partition type . */
  default PartitionType getPartitionType() {
    return PartitionType.forward;
  }

  enum PartitionType {
    forward,
    broadcast,
    key,
    custom,
    iterator,
  }
}
