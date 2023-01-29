package io.ray.streaming.api.function.async;

import io.ray.streaming.api.function.RichFunction;

/**
 * Interface of async io functions which enable a async io operation.
 *
 * @param <T> Type of the input data.
 * @param <R> Type of the output data.
 */
public interface AsyncFunction<T, R> extends RichFunction {

  void runAsync(T input, ResultFuture<R> resultFuture) throws Exception;

  default void timeout(T input, ResultFuture<R> resultFuture) {
    resultFuture.completeExceptionally(new RuntimeException("Run a async function timed out!"));
  }
}
