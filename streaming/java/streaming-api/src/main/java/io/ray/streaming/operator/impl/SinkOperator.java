package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;

public class SinkOperator<T> extends AbstractStreamOperator<SinkFunction<T>>
    implements OneInputOperator<T> {

  public SinkOperator(SinkFunction<T> sinkFunction) {
    super(sinkFunction);
    Type paramType = TypeUtils.getParamTypes(SinkFunction.class, sinkFunction, "sink")[0];
    typeInfo = new TypeInfo(paramType);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    if (!record.isRetract()) {
      this.function.sink(record.getValue());
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return typeInfo;
  }
}
