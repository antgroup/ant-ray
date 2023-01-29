package io.ray.streaming.common.enums;

public enum OperatorType {

  /** operator type */
  SOURCE(0),
  TRANSFORM(1),
  SINK(2),
  SOURCE_AND_SINK(3);

  private int value;

  OperatorType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
