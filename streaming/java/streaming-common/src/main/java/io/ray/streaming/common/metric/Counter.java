package io.ray.streaming.common.metric;

import java.io.Serializable;

public interface Counter extends Serializable {

  void inc();

  void update(Number val);
}
