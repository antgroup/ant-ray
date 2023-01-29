package io.ray.streaming.common.metric.ray;

import io.ray.runtime.metric.Gauge;
import io.ray.streaming.common.metric.Histogram;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayHistogram implements Histogram {

  private static final Logger LOG = LoggerFactory.getLogger(RayHistogram.class);
  private static final int THRESHOLD = 1000;

  private final String name;
  private final Map<String, String> tags;
  private final ExecutorService rayMetricService;
  private long lastUpdateTs = 0L;
  private List<Number> metricDataList = new ArrayList<>();
  private int uploadTimes = 0;
  private Gauge rayMetric;

  public RayHistogram(String name, Map<String, String> tags, ExecutorService rayMetricService) {
    this.name = name;
    this.tags = tags;
    this.rayMetricService = rayMetricService;
    this.rayMetric = new Gauge(name, "", tags);
  }

  @Override
  public synchronized void update(Number obj) {
    if (obj.intValue() <= 0) {
      return;
    }
    metricDataList.add(obj);
    long now = System.currentTimeMillis();
    if (metricDataList.size() > THRESHOLD || now - lastUpdateTs > RayMetricGroup.REPORTING_GAP) {
      double avg = metricDataList.stream().mapToInt(Number::intValue).average().orElse(0.0);
      metricDataList.clear();
      lastUpdateTs = now;
      rayMetricService.execute(
          () -> {
            rayMetric.update(avg);
            rayMetric.record();
          });
    }
  }
}
