package io.ray.streaming.api.window;

import java.io.Serializable;

/** The basic abstraction for different types of 'Window'. */
public interface Window extends Serializable {

  /**
   * Gets the max timestamp that still belongs to this window.
   *
   * @return timestamp
   */
  long maxTime();

  /**
   * Gets the min timestamp that still belongs to this window.
   *
   * @return timestamp
   */
  long minTime();
}
