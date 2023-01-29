package io.ray.streaming.operator;

import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.message.Record;
import io.ray.streaming.util.TypeInfo;

public interface TwoInputOperator<L, R> extends StreamOperator {

  void processElement(Record<L> record1, Record<R> record2) throws Exception;

  @Override
  default OperatorInputType getOpType() {
    return OperatorInputType.TWO_INPUT;
  }

  TypeInfo getLeftInputTypeInfo();

  TypeInfo getRightInputTypeInfo();
}
