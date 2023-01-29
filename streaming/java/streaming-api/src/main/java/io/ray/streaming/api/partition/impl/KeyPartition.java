package io.ray.streaming.api.partition.impl;

import io.ray.state.util.KeyGroupAssignment;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.message.KeyRecord;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition the record by the key.
 *
 * @param <K> Type of the partition key.
 * @param <T> Type of the input record.
 */
public class KeyPartition<K, T> implements Partition<KeyRecord<K, T>> {
  private static final Logger LOG = LoggerFactory.getLogger(KeyPartition.class);
  private int maxPara;
  private Map<Integer, List<Integer>> keyGroupToTask;

  @Override
  public int[] partition(KeyRecord<K, T> keyRecord, int numPartition) {
    if (keyGroupToTask == null) {
      keyGroupToTask =
          KeyGroupAssignment.computeKeyGroupToTask(
              maxPara, IntStream.range(0, numPartition).boxed().collect(Collectors.toList()));
      LOG.info("Init keyGroupToTask map: {}.", keyGroupToTask);
    }

    int keyGroup = KeyGroupAssignment.assignToKeyGroup(keyRecord.getKey(), maxPara);
    int[] res = {keyGroupToTask.get(keyGroup).get(0)};

    LOG.debug(
        "maxPara: {}, keyGroupToTask: {}, keyGroup: {}, target channel: {}, key: {}, "
            + "key hashcode: {}.",
        maxPara,
        keyGroupToTask,
        keyGroup,
        res[0],
        keyRecord.getKey(),
        keyRecord.getKey().hashCode());

    return res;
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.key;
  }

  public void resetKeyGroup() {
    keyGroupToTask = null;
  }

  public void setMaxPara(int maxPara) {
    this.maxPara = maxPara;
  }
}
