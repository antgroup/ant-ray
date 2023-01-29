package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.CollectionCollector;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.ProcessFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

public class ProcessOperator<T, R> extends AbstractStreamOperator<ProcessFunction<T, R>>
    implements OneInputOperator<T> {
  private final TypeInfo<T> inputTypeInfo;
  private Collector<R> collectionCollector;

  public ProcessOperator(ProcessFunction<T, R> processFunction) {
    super(processFunction);
    Type inputType = TypeUtils.getParamTypes(ProcessFunction.class, processFunction, "process")[0];
    inputTypeInfo = new TypeInfo<>(inputType);
    Method method = TypeUtils.getFunctionMethod(ProcessFunction.class, processFunction, "process");
    Type type = TypeUtils.resolveCollectorValueType(method.getGenericParameterTypes()[1]);
    typeInfo = new TypeInfo(type);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.collectionCollector = new CollectionCollector(collectorList);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    if (record.isRetract()) {
      this.function.retract(record.getValue(), collectionCollector);
    } else {
      this.function.process(record.getValue(), collectionCollector);
    }
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputTypeInfo;
  }
}
