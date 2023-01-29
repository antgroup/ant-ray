package io.ray.streaming.operator.impl;

import io.ray.state.api.KeyMapState;
import io.ray.state.api.KeyMapStateDescriptor;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.JoinFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.TwoInputOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PkJoinOperator<K, L, R, P, Q, O> extends AbstractStreamOperator<JoinFunction<L, R, O>>
    implements TwoInputOperator {

  private static final Logger LOG = LoggerFactory.getLogger(PkJoinOperator.class);
  private final KeyFunction<L, P> leftPrimaryKeyExtractor;
  private final KeyFunction<R, Q> rightPrimaryKeyExtractor;
  protected Class<K> keyClass;
  protected Class<L> in1Class;
  protected Class<R> in2Class;
  protected Class<O> outClass;
  protected Class<P> leftPkClass;
  protected Class<Q> rightPkClass;
  private transient KeyMapState<K, P, L> leftMap;
  private transient KeyMapState<K, Q, R> rightMap;

  public PkJoinOperator(
      JoinFunction<L, R, O> function,
      KeyFunction<L, P> primaryKeyExtractor1,
      KeyFunction<R, Q> primaryKeyExtractor2,
      Class<?>[] typeArguments) {
    super(function);
    this.leftPrimaryKeyExtractor = primaryKeyExtractor1;
    this.rightPrimaryKeyExtractor = primaryKeyExtractor2;
    this.keyClass = (Class<K>) typeArguments[0];
    this.in1Class = (Class<L>) typeArguments[1];
    this.in2Class = (Class<R>) typeArguments[2];
    this.outClass = (Class<O>) typeArguments[3];
    this.leftPkClass = (Class<P>) typeArguments[4];
    this.rightPkClass = (Class<Q>) typeArguments[5];
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    super.open(collectors, runtimeContext);
    leftMap =
        this.runtimeContext.getKeyMapState(
            KeyMapStateDescriptor.build(
                getName() + "-left", this.keyClass, this.leftPkClass, this.in1Class));
    rightMap =
        this.runtimeContext.getKeyMapState(
            KeyMapStateDescriptor.build(
                getName() + "-right", this.keyClass, this.rightPkClass, this.in2Class));
  }

  @Override
  public void processElement(Record record1, Record record2) throws Exception {
    if (record1 != null) {
      nonBatchProcess(record1, null);
    } else {
      nonBatchProcess(null, record2);
    }
  }

  @Override
  public TypeInfo getLeftInputTypeInfo() {
    return new TypeInfo(in1Class);
  }

  @Override
  public TypeInfo getRightInputTypeInfo() {
    return new TypeInfo(in2Class);
  }

  @Override
  public ChainStrategy getChainStrategy() {
    return ChainStrategy.HEAD;
  }

  private boolean tryJoinAndEmit(L leftValue, R rightValue, boolean isRetract) throws Exception {
    O out = this.function.join(leftValue, rightValue);
    if (out != null) {
      if (isRetract == false) {
        collect(out);
      } else {
        retract(out);
      }
      return true;
    }
    return false;
  }

  private void joinEmitMessage(
      KeyRecord<K, L> leftRecord, KeyRecord<K, R> rightRecord, boolean isRetract) throws Exception {
    if (leftRecord != null) {
      Map<Q, R> rightOldData = rightMap.multiGet(leftRecord.getKey());
      if (rightOldData == null || rightOldData.size() == 0) {
        tryJoinAndEmit(leftRecord.getValue(), null, isRetract);
      } else {
        for (R in2 : rightOldData.values()) {
          tryJoinAndEmit(leftRecord.getValue(), in2, isRetract);
        }
      }
    } else {
      Map<P, L> leftOldData = leftMap.multiGet(rightRecord.getKey());
      if (leftOldData == null || leftOldData.size() == 0) {
        tryJoinAndEmit(null, rightRecord.getValue(), isRetract);
      } else {
        for (L in1 : leftOldData.values()) {
          tryJoinAndEmit(in1, rightRecord.getValue(), isRetract);
        }
      }
    }
  }

  private void nonBatchProcess(Record<L> record1, Record<R> record2) throws Exception {
    boolean isLeft = (record1 != null);

    if (isLeft) {
      KeyRecord<K, L> leftRecord = (KeyRecord<K, L>) record1;
      P leftPk = this.leftPrimaryKeyExtractor.keyBy(leftRecord.getValue());
      K joinKey = leftRecord.getKey();

      if (leftRecord.isRetract()) {
        joinEmitMessage(leftRecord, null, true);
        leftMap.remove(joinKey, leftPk);
      } else {
        Map<P, L> leftOldValue = null;
        if (leftOldValue == null || leftOldValue.size() == 0) {
          joinRetractNullRecord(joinKey, null);
        }
        joinEmitMessage(leftRecord, null, false);
        leftMap.put(joinKey, leftPk, leftRecord.getValue());
      }
    } else {
      KeyRecord<K, R> rightRecord = (KeyRecord<K, R>) record2;
      K joinKey = rightRecord.getKey();
      Q rightPk = this.rightPrimaryKeyExtractor.keyBy(rightRecord.getValue());

      if (rightRecord.isRetract()) {
        joinEmitMessage(null, rightRecord, true);
        rightMap.remove(joinKey, rightPk);
      } else {
        Map<Q, R> rightOldValue = null;
        if (rightOldValue == null || rightOldValue.size() == 0) {
          joinRetractNullRecord(null, joinKey);
        }
        joinEmitMessage(null, rightRecord, false);
        rightMap.put(joinKey, rightPk, rightRecord.getValue());
      }
    }
  }

  private void joinRetractNullRecord(K leftKey, K rightKey) throws Exception {
    if (leftKey != null) {
      Map<Q, R> rightOldData = rightMap.multiGet(leftKey);
      if (rightOldData != null && rightOldData.size() != 0) {
        Iterator<R> it = rightOldData.values().iterator();
        while (it.hasNext()) {
          O out = this.function.join(null, it.next());
          if (out != null) {
            retract(out);
          }
        }
      }
    } else {
      Map<P, L> leftOldData = leftMap.multiGet(rightKey);
      if (leftOldData != null && leftOldData.size() != 0) {
        Iterator<L> it = leftOldData.values().iterator();
        while (it.hasNext()) {
          O out = this.function.join(it.next(), null);
          if (out != null) {
            retract(out);
          }
        }
      }
    }
  }
}
