package io.ray.streaming.operator.chain;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.TwoInputOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.List;
import java.util.Map;

public class ChainedTwoInputOperator<L, R> extends ChainedOperator
    implements TwoInputOperator<L, R> {
  private final TwoInputOperator<L, R> inputOperator;

  @SuppressWarnings("unchecked")
  public ChainedTwoInputOperator(
      List<StreamOperator> operators,
      List<Map<String, String>> configs,
      List<Map<String, Double>> resources) {
    super(operators, configs, resources);
    inputOperator = (TwoInputOperator<L, R>) headOperator;
  }

  @Override
  public void processElement(Record<L> record1, Record<R> record2) throws Exception {
    inputOperator.processElement(record1, record2);
  }

  @Override
  public TypeInfo getLeftInputTypeInfo() {
    return inputOperator.getLeftInputTypeInfo();
  }

  @Override
  public TypeInfo getRightInputTypeInfo() {
    return inputOperator.getRightInputTypeInfo();
  }
}
