package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.JoinFunction;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.TwoInputOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Join operator
 *
 * @param <L> Type of the data in the left stream.
 * @param <R> Type of the data in the right stream.
 * @param <K> Type of the data in the join key.
 * @param <O> Type of the data in the joined stream.
 */
public class JoinOperator<L, R, K, O> extends AbstractStreamOperator<JoinFunction<L, R, O>>
    implements TwoInputOperator<L, R> {

  private Map<K, List<L>> leftRecordListMap;
  private Map<K, List<R>> rightRecordListMap;

  public JoinOperator() {}

  public JoinOperator(JoinFunction<L, R, O> function) {
    super(function);
    setChainStrategy(ChainStrategy.HEAD);
    // TODO: use list/value state.
    leftRecordListMap = new HashMap<>();
    rightRecordListMap = new HashMap<>();
  }

  @Override
  public void processElement(Record<L> leftRecord, Record<R> rightRecord) {
    K key;
    if (leftRecord != null) {
      key = ((KeyRecord<K, L>) leftRecord).getKey();
    } else {
      key = ((KeyRecord<K, L>) rightRecord).getKey();
    }

    List<R> rightList = rightRecordListMap.get(key);
    List<L> leftList = leftRecordListMap.get(key);

    if (leftRecord != null) {
      L value1 = leftRecord.getValue();
      leftList.add(value1);
      collect((Record) this.function.join(value1, null));
      for (R value2 : rightList) {
        collect((Record) this.function.join(value1, value2));
      }
      // TODO: process left record for state.
      // leftList.update(this.function.processLeft(leftList.get()));
    } else {
      R value2 = rightRecord.getValue();
      rightList.add(value2);
      collect((Record) this.function.join(null, value2));
      for (L value1 : leftList) {
        collect((Record) this.function.join(value1, value2));
      }
      // TODO: process right record for state.
      // rightList.update(this.function.processRight(rightList.get()));
    }
  }

  @Override
  public OperatorInputType getOpType() {
    return OperatorInputType.TWO_INPUT;
  }

  @Override
  public TypeInfo getLeftInputTypeInfo() {
    // TODO(chaokunyang) extract join function typeinfo
    return new TypeInfo<>(Object.class);
  }

  @Override
  public TypeInfo getRightInputTypeInfo() {
    // TODO(chaokunyang) extract join function typeinfo
    return new TypeInfo<>(Object.class);
  }
}
