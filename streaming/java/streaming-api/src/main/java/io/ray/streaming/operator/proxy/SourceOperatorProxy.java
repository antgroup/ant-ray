package io.ray.streaming.operator.proxy;

import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.operator.ISourceOperator;

public class SourceOperatorProxy extends OutputOperatorProxy implements ISourceOperator {

  public SourceOperatorProxy(ISourceOperator operator) {
    super(operator);
  }

  @Override
  public void fetch(long checkpointId) {
    ((ISourceOperator) operator).fetch(checkpointId);
  }

  @Override
  public SourceFunction.SourceContext getSourceContext() {
    // SourceContext use collectors passed in `Operator#open`, so we shouldn't proxy SourceContext
    // again.
    return ((ISourceOperator) operator).getSourceContext();
  }
}
