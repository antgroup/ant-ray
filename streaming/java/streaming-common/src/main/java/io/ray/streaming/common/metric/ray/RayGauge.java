package io.ray.streaming.common.metric.ray;

import io.ray.streaming.common.metric.Gauge;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayGauge implements Gauge {

  private static final Logger LOG = LoggerFactory.getLogger(RayGauge.class);

  private final String name;
  private final Map<String, String> tags;
  private final ExecutorService rayMetricService;
  private final io.ray.runtime.metric.Gauge rayMetric;

  public RayGauge(String meterName, Map<String, String> tags, ExecutorService rayMetricService) {
    this.name = meterName;
    this.tags = tags;
    this.rayMetricService = rayMetricService;
    this.rayMetric = new io.ray.runtime.metric.Gauge(name, "", tags);
  }

  @Override
  public void update(Number obj) {
    rayMetricService.execute(
        () -> {
          rayMetric.update(obj.doubleValue());
          rayMetric.record();
        });
  }
}
