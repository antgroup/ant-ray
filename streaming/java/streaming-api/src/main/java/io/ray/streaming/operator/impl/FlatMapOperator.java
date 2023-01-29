package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.CollectionCollector;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

public class FlatMapOperator<T, R> extends AbstractStreamOperator<FlatMapFunction<T, R>>
    implements OneInputOperator<T> {
  private final TypeInfo<T> inputTypeInfo;
  private Collector<R> collectionCollector;

  public FlatMapOperator(FlatMapFunction<T, R> flatMapFunction) {
    super(flatMapFunction);
    Type inputType = TypeUtils.getParamTypes(FlatMapFunction.class, flatMapFunction, "flatMap")[0];
    this.inputTypeInfo = new TypeInfo<>(inputType);
    Method method = TypeUtils.getFunctionMethod(FlatMapFunction.class, flatMapFunction, "flatMap");
    Type type = TypeUtils.resolveCollectorValueType(method.getGenericParameterTypes()[1]);
    typeInfo = new TypeInfo<R>(type);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.collectionCollector = new CollectionCollector(collectorList);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    this.function.flatMap(record.getValue(), collectionCollector);
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputTypeInfo;
  }
}
