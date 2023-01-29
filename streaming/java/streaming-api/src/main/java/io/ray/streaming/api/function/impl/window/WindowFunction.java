package io.ray.streaming.api.function.impl.window;

import io.ray.streaming.api.function.RichFunction;
import io.ray.streaming.api.window.Window;

/**
 * Basic window function abstraction.
 *
 * @param <K> key type
 * @param <T> input data type
 * @param <R> output data type
 * @param <W> window type
 */
public interface WindowFunction<K, T, R, W extends Window> extends RichFunction {

  /**
   * Invoke the input data and output them properly.
   *
   * @param key data key
   * @param values data in the window being evaluated
   * @param window used window
   * @throws Exception any exceptions during this method
   */
  R apply(K key, Iterable<T> values, W window) throws Exception;
}
