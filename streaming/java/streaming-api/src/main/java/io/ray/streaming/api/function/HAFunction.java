package io.ray.streaming.api.function;

/**
 * For HA-Structure, the same function might be running in different jobs and clusters. But only the
 * activated function can process at the same time.
 */
public interface HAFunction extends Function {

  /**
   * Is the function been activated.
   *
   * @return active result
   */
  boolean isActive();

  /** Activate the function. */
  void activate();

  /** Deactivate the function. */
  void deactivate();
}
