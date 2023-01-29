package io.ray.streaming.common.metric.ray;

import io.ray.runtime.metric.Gauge;
import io.ray.streaming.common.metric.Meter;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayMeter implements Meter {

  private static final Logger LOG = LoggerFactory.getLogger(RayMeter.class);
  private static final int THRESHOLD = 20000;

  private final String name;
  private final Map<String, String> tags;
  private final ExecutorService rayMetricService;
  private long updateCount;
  private long lastUpdateTime = 0;
  private Gauge rayMetric;

  public RayMeter(String meterName, Map<String, String> tags, ExecutorService rayMetricService) {
    this.name = meterName;
    this.tags = tags;
    this.updateCount = 0L;
    this.rayMetricService = rayMetricService;
    rayMetric = new Gauge(meterName, "", tags);
  }

  @Override
  public synchronized void update(Number obj) {
    if (obj.intValue() <= 0) {
      return;
    }
    updateCount += obj.longValue();
    long now = System.currentTimeMillis();
    if (now - lastUpdateTime >= RayMetricGroup.REPORTING_GAP) {
      // NOTE(lingxuan.zlx): counting all updated metric and sliding them in to a
      // window for computing tps.
      double tpsRate = 1000.0 * updateCount / (now - lastUpdateTime);
      rayMetricService.execute(
          () -> {
            rayMetric.update(tpsRate);
            rayMetric.record();
          });
      lastUpdateTime = now;
      updateCount = 0L;
    }
  }

  @Override
  public void update() {
    update(1);
  }
}
