package io.ray.streaming.operator.chain;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;

@SuppressWarnings("unchecked")
class ForwardCollector<T> implements Collector<T> {

  private final OneInputOperator succeedingOperator;

  ForwardCollector(OneInputOperator succeedingOperator) {
    this.succeedingOperator = succeedingOperator;
  }

  @Override
  public void collect(T value) {
    try {
      Record record;
      if (value instanceof Record) {
        record = (Record) value;
      } else {
        record = new Record(value);
      }
      succeedingOperator.processElement(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void retract(T value) {
    Record record;
    if (value instanceof Record) {
      record = (Record) value;
    } else {
      record = new Record(value);
    }
    Record copy = record.copy();
    copy.setRetract(true);
    try {
      succeedingOperator.processElement(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
