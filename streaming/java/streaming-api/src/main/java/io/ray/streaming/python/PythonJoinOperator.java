package io.ray.streaming.python;

public class PythonJoinOperator extends PythonOperator {
  private int leftInputOperatorId;
  private int rightInputOperatorId;

  public PythonJoinOperator(
      PythonFunction function, int leftInputOperatorId, int rightInputOperatorId) {
    super(function);
    this.leftInputOperatorId = leftInputOperatorId;
    this.rightInputOperatorId = rightInputOperatorId;
  }

  public int getLeftInputOperatorId() {
    return leftInputOperatorId;
  }

  public int getRightInputOperatorId() {
    return rightInputOperatorId;
  }
}
