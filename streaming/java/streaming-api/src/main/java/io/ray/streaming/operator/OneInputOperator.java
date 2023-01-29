package io.ray.streaming.operator;

import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.message.Record;
import io.ray.streaming.util.TypeInfo;

public interface OneInputOperator<T> extends StreamOperator {

  void processElement(Record<T> record) throws Exception;

  @Override
  default OperatorInputType getOpType() {
    return OperatorInputType.ONE_INPUT;
  }

  TypeInfo<T> getInputTypeInfo();
}
