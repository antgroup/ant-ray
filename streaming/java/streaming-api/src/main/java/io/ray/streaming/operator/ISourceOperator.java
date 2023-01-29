package io.ray.streaming.operator;

import io.ray.streaming.api.function.impl.SourceFunction.SourceContext;
import io.ray.streaming.common.enums.OperatorInputType;

public interface ISourceOperator<T> extends StreamOperator {

  void fetch(long checkpointId);

  SourceContext<T> getSourceContext();

  @Override
  default OperatorInputType getOpType() {
    return OperatorInputType.SOURCE;
  }
}
