package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.function.Function;

/**
 * Interface of process functions.
 *
 * @param <T> Type of the input data.
 * @param <O> Type of the output data.
 */
public interface ProcessFunction<T, O> extends Function {

  void process(T value, Collector<O> collector) throws Exception;

  default void retract(T value, Collector<O> collector) throws Exception {
    process(value, collector);
  }

  @Deprecated
  default void finish(Collector<O> collector) throws Exception {}

  /**
   * rollback function is only for compiling.
   *
   * @throws Exception
   */
  @Deprecated
  default void rollback() throws Exception {}
}
