package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.internal.Functions;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;
import org.apache.arrow.vector.types.pojo.Schema;

public class UnionOperator<T> extends AbstractStreamOperator<Function>
    implements OneInputOperator<T> {
  private final StreamOperator inputOperator;

  public UnionOperator(StreamOperator inputOperator) {
    super(Functions.emptyFunction());
    // UnionOperator's typeinfo is the same as previous stream type info.
    this.inputOperator = inputOperator;
  }

  @Override
  public void processElement(Record<T> record) {
    collect(record);
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputOperator.getTypeInfo();
  }

  @Override
  public boolean hasTypeInfo() {
    return inputOperator.hasTypeInfo();
  }

  @Override
  public TypeInfo getTypeInfo() {
    return inputOperator.getTypeInfo();
  }

  @Override
  public boolean hasSchema() {
    return inputOperator.hasSchema();
  }

  @Override
  public Schema getSchema() {
    return inputOperator.getSchema();
  }
}
