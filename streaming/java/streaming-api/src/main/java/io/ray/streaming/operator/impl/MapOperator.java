package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;

public class MapOperator<T, R> extends AbstractStreamOperator<MapFunction<T, R>>
    implements OneInputOperator<T> {
  private final TypeInfo<T> inputTypeInfo;

  public MapOperator(MapFunction<T, R> mapFunction) {
    super(mapFunction);
    Type inputType = TypeUtils.getParamTypes(MapFunction.class, mapFunction, "map")[0];
    Type type =
        TypeUtils.getFunctionMethod(MapFunction.class, mapFunction, "map").getGenericReturnType();
    typeInfo = new TypeInfo(type);
    inputTypeInfo = new TypeInfo<>(inputType);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    this.collect(new Record<R>(this.function.map(record.getValue())));
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputTypeInfo;
  }
}
