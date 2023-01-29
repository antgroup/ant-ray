package io.ray.streaming.operator.impl;

import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeResolver;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateOperator<K, I, A, O>
    extends AbstractStreamOperator<AggregateFunction<I, A, O>> implements OneInputOperator {
  private Class<A> accClass;
  private transient KeyValueState<Object, A> aggregatingState;
  private boolean isBatchEmit = true;
  private Map<K, O> tmpCollection;
  private Map<K, O> oldValues;

  public AggregateOperator(AggregateFunction<I, A, O> aggregateFunction) {
    super(aggregateFunction);
    setChainStrategy(ChainStrategy.HEAD);
    Type type =
        TypeUtils.getFunctionMethod(AggregateFunction.class, aggregateFunction, "createAccumulator")
            .getGenericReturnType();
    typeInfo = new TypeInfo(type);
    accClass =
        (Class<A>)
            TypeResolver.resolveRawArguments(AggregateFunction.class, aggregateFunction.getClass())[
                1];
    this.tmpCollection = new HashMap<>();
    this.oldValues = new HashMap<>();
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    super.open(collectors, runtimeContext);

    this.aggregatingState =
        runtimeContext.getKeyValueState(
            KeyValueStateDescriptor.build(this.name, Object.class, accClass));
  }

  @Override
  public void processElement(Record record) throws Exception {
    KeyRecord<K, I> keyRecord = (KeyRecord<K, I>) record;
    K key = keyRecord.getKey();
    final boolean isRetractMessage = keyRecord.isRetract();
    A acc = aggregatingState.get(key);

    // 1. 发送Old Value
    if (acc == null) {
      acc = this.function.createAccumulator();
    }
    if (!isRetractMessage) {
      this.function.add(keyRecord.getValue(), acc);
      aggregatingState.put(key, acc);
    } else {
      this.function.retract(acc, keyRecord.getValue());
      aggregatingState.put(key, acc);
    }
    O result = this.function.getResult(acc);

    if (result != null) {

      if (isBatchEmit) {
        this.tmpCollection.put(keyRecord.getKey(), result);
      } else {
        collect(result);
      }
    }
  }

  @Override
  public OperatorInputType getOpType() {
    return OperatorInputType.ONE_INPUT;
  }

  @Override
  public void saveCheckpoint(long checkpointId) throws Exception {
    super.saveCheckpoint(checkpointId);
    if (isBatchEmit) {
      oldValues.forEach(
          (k, v) -> {
            if (v != null) {
              collect(v);
            }
          });
      oldValues.clear();
      tmpCollection.forEach((k, v) -> collect(v));
      tmpCollection.clear();
    }
  }

  @Override
  public TypeInfo getInputTypeInfo() {
    return typeInfo;
  }
}
