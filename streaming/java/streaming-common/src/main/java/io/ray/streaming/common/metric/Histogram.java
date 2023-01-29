package io.ray.streaming.common.metric;

import java.io.Serializable;

public interface Histogram extends Serializable {

  void update(Number obj);
}
