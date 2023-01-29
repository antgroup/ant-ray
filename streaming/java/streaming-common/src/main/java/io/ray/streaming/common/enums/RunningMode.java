package io.ray.streaming.common.enums;

/** Running mode for ray streaming. */
public enum RunningMode {

  /** Cluster mode. */
  CLUSTER("cluster"),

  /** Single process mode. */
  SINGLE_PROCESS("single-process");

  private String value;

  RunningMode(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
