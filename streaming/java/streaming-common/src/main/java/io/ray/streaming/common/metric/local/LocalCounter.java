package io.ray.streaming.common.metric.local;

import io.ray.streaming.common.metric.Counter;

public class LocalCounter implements Counter {

  @Override
  public void inc() {}

  @Override
  public void update(Number val) {}
}
