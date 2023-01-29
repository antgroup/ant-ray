package io.ray.api;

public enum EventSeverity {
  INFO("INFO", 0),
  WARNING("WARNING", 1),
  ERROR("ERROR", 2),
  FATAL("FATAL", 3);

  private String name;
  private int index;

  private EventSeverity(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public int getIndex() {
    return index;
  }
}
