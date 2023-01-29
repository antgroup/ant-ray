package io.ray.streaming.util;

import java.util.Map;

public interface DynamicRebalanceDetector {
  // Key is output channel ID, value is backpressure ratio.
  Map<String, Double> getOutputBackpressureRatio();

  int getBestBackpressureRatio();

  int getBufferCapacity();

  int getBatchSize();
}
