package io.ray.streaming.common.enums;

/** Define resource key for different type resources. */
public enum ResourceKey {

  /** Cpu resource. */
  CPU("CPU"),

  /** Gpu resource. */
  GPU("GPU"),

  /** Memory resource. */
  MEM("MEM"),

  /** Disk storage size resource. */
  DISK("DISK"),

  /** Disk io resource key. */
  DISK_IO("DISK_IO");

  private String value;

  ResourceKey(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
