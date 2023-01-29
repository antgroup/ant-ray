package io.ray.streaming.operator.impl;

import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;
import java.util.List;

public class ReduceOperator<K, T> extends AbstractStreamOperator<ReduceFunction<T>>
    implements OneInputOperator<T> {

  private Class<T> valueClass;
  private transient KeyValueState<Object, T> reduceState;

  public ReduceOperator(ReduceFunction<T> reduceFunction, Class<?>[] reduceTypeArguments) {
    super(reduceFunction);
    setChainStrategy(ChainStrategy.HEAD);
    Type type =
        TypeUtils.getFunctionMethod(ReduceFunction.class, reduceFunction, "reduce")
            .getGenericReturnType();
    this.valueClass = (Class<T>) reduceTypeArguments[0];
    this.typeInfo = new TypeInfo(type);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.reduceState =
        this.runtimeContext.getKeyValueState(
            KeyValueStateDescriptor.build(getName() + "-reduce", Object.class, this.valueClass));
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    KeyRecord<K, T> keyRecord = (KeyRecord<K, T>) record;
    K key = keyRecord.getKey();
    T value = keyRecord.getValue();
    T newValue;

    if (reduceState.contains(key)) {
      T oldValue = reduceState.get(key);
      newValue = function.reduce(oldValue, value);
    } else {
      newValue = value;
    }

    this.reduceState.put(key, newValue);
    if (newValue != null) {
      collect(new Record<>(newValue));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return typeInfo;
  }
}
