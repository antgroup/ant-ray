package io.ray.streaming.python;

import java.util.List;

public class PythonMergeOperator extends PythonOperator {
  private int leftInputOperatorId;
  private List<Integer> rightInputOperatorIds;

  public PythonMergeOperator(
      PythonFunction function, int leftInputOperatorId, List<Integer> rightInputOperatorIds) {
    super(function);
    this.leftInputOperatorId = leftInputOperatorId;
    this.rightInputOperatorIds = rightInputOperatorIds;
  }

  public int getLeftInputOperatorId() {
    return leftInputOperatorId;
  }

  public List<Integer> getRightInputOperatorIds() {
    return rightInputOperatorIds;
  }
}
