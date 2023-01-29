package io.ray.streaming.common.enums;

public enum EnvironmentType {

  /** Dev type */
  DEV("dev", 0),

  /** Prod type */
  PROD("prod", 1);

  private String name;
  private int index;

  EnvironmentType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }
}
