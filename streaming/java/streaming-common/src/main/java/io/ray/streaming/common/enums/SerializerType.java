package io.ray.streaming.common.enums;

public enum SerializerType {

  /** Fury type */
  FURY("fury", 0),

  /** Fst type */
  FST("fst", 1),

  /** Kryo type */
  KRYO("kryo", 2);

  private String name;
  private int index;

  SerializerType(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
