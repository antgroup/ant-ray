package io.ray.streaming.api.function.async;

import io.ray.streaming.operator.async.AsyncElement;
import java.util.Collection;

/**
 * ResultFuture is used to collect data or exceptions in async io processing.
 *
 * @param <R> Type of the output data.
 */
public interface ResultFuture<R> {

  /**
   * Sets the results when {@link AsyncElement} is completed.
   *
   * @param results the results of {@link AsyncFunction}
   */
  void complete(Collection<R> results);

  /**
   * If not completed, handles the given exception.
   *
   * @param ex the exception
   */
  void completeExceptionally(Throwable ex);
}
