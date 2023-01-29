package io.ray.streaming.operator.chain;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.List;
import java.util.Map;

public class ChainedOneInputOperator<T> extends ChainedOperator implements OneInputOperator<T> {
  private final OneInputOperator<T> inputOperator;

  @SuppressWarnings("unchecked")
  public ChainedOneInputOperator(
      List<StreamOperator> operators,
      List<Map<String, String>> configs,
      List<Map<String, Double>> resources) {
    super(operators, configs, resources);
    inputOperator = (OneInputOperator<T>) headOperator;
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    inputOperator.processElement(record);
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputOperator.getInputTypeInfo();
  }
}
