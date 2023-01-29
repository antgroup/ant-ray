package io.ray.streaming.api.function.impl;

import java.util.Map;

public interface PSinkFunction<T> extends SinkFunction<T> {

  byte[] psink();

  default byte[] psink(Map<String, String> config) {
    return null;
  }

  default void sink(T t) {
    throw new UnsupportedOperationException("should not be here");
  }
}
