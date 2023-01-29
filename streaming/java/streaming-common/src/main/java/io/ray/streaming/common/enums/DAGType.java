package io.ray.streaming.common.enums;

public enum DAGType {

  /** Whole dag type. */
  DAG("DAG", 0),

  /** Sub dag type. */
  SUBDAG("Sub-DAG", 1);

  private String name;
  private int index;

  DAGType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }
}
