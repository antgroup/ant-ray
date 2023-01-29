package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;

public class FilterOperator<T> extends AbstractStreamOperator<FilterFunction<T>>
    implements OneInputOperator<T> {

  public FilterOperator(FilterFunction<T> filterFunction) {
    super(filterFunction);
    Type paramType = TypeUtils.getParamTypes(FilterFunction.class, filterFunction, "filter")[0];
    typeInfo = new TypeInfo(paramType);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    if (this.function.filter(record.getValue())) {
      this.collect(record);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return typeInfo;
  }
}
