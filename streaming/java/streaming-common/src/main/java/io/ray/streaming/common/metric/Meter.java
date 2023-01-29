package io.ray.streaming.common.metric;

import java.io.Serializable;

public interface Meter extends Serializable {

  /**
   * update value in batch.
   *
   * @param obj metric value
   */
  void update(Number obj);

  /** update value one by one. */
  void update();
}
