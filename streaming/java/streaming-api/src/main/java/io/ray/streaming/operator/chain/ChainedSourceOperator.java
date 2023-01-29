package io.ray.streaming.operator.chain;

import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.operator.ISourceOperator;
import io.ray.streaming.operator.StreamOperator;
import java.util.List;
import java.util.Map;

public class ChainedSourceOperator<T> extends ChainedOperator implements ISourceOperator<T> {
  private final ISourceOperator<T> sourceOperator;

  @SuppressWarnings("unchecked")
  public ChainedSourceOperator(
      List<StreamOperator> operators,
      List<Map<String, String>> configs,
      List<Map<String, Double>> resources) {
    super(operators, configs, resources);
    sourceOperator = (ISourceOperator<T>) headOperator;
  }

  @Override
  public void fetch(long checkpointId) {
    sourceOperator.fetch(checkpointId);
  }

  @Override
  public SourceFunction.SourceContext<T> getSourceContext() {
    return sourceOperator.getSourceContext();
  }
}
