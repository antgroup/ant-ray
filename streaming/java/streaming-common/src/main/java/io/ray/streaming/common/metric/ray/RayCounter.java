package io.ray.streaming.common.metric.ray;

import io.ray.runtime.metric.Sum;
import io.ray.streaming.common.metric.Counter;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayCounter implements Counter {

  private static final Logger LOG = LoggerFactory.getLogger(RayCounter.class);
  private static final int THRESHOLD = 20000;

  private final String name;
  private final Map<String, String> tags;
  private final ExecutorService rayMetricService;
  private AtomicLong count;
  private long lastUpdateTs = 0L;
  private final Sum rayMetric;

  public RayCounter(String name, Map<String, String> tags, ExecutorService rayMetricService) {
    this.name = name;
    this.tags = tags;
    this.count = new AtomicLong();
    this.rayMetricService = rayMetricService;
    rayMetric = new Sum(name, "", tags);
  }

  @Override
  public void inc() {
    update(1);
  }

  @Override
  public void update(Number val) {
    long now = System.currentTimeMillis();
    long countSnapshot = count.addAndGet(val.longValue());
    if (countSnapshot > THRESHOLD || now - lastUpdateTs > RayMetricGroup.REPORTING_GAP) {
      rayMetricService.execute(
          () -> {
            rayMetric.update(countSnapshot);
            rayMetric.record();
          });
      lastUpdateTs = now;
      count.set(0);
    }
  }
}
