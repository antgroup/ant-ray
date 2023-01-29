package io.ray.streaming.common.metric;

import java.io.Serializable;

public interface Gauge extends Serializable {

  /**
   * Update a value that can arbitrarily go up and down.
   *
   * @param obj metric value
   */
  void update(Number obj);
}
