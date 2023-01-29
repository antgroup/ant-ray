package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of Source functions.
 *
 * @param <T> Type of the data output by the source.
 */
public interface SourceFunction<T> extends Function {

  void init(int parallel, int index);

  void fetch(long checkpointId, SourceContext<T> ctx) throws Exception;

  interface SourceContext<T> {

    void collect(T element) throws Exception;
  }
}
