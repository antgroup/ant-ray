package io.ray.streaming.api.collector;

import io.ray.streaming.message.Record;
import java.util.List;

/**
 * Combination of multiple collectors.
 *
 * @param <T> The type of output data.
 */
public class CollectionCollector<T> implements Collector<T> {

  private List<Collector<Record<T>>> collectorList;

  public CollectionCollector(List<Collector<Record<T>>> collectorList) {
    this.collectorList = collectorList;
  }

  @Override
  public void collect(T value) {
    for (Collector<Record<T>> collector : collectorList) {
      collector.collect(new Record<>(value));
    }
  }

  @Override
  public void retract(T value) {
    for (Collector<Record<T>> collector : collectorList) {
      Record<T> record = new Record<>(value);
      record.setRetract(true);
      collector.collect(record);
    }
  }
}
