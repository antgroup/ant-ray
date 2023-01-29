package io.ray.streaming.common.metric.local;

import io.ray.streaming.common.metric.Counter;
import io.ray.streaming.common.metric.Gauge;
import io.ray.streaming.common.metric.Histogram;
import io.ray.streaming.common.metric.Meter;
import io.ray.streaming.common.metric.MetricGroup;
import java.util.Map;

public class LocalMetricGroup implements MetricGroup {

  @Override
  public Meter getMeter(String meterName) {
    return new LocalMeter();
  }

  @Override
  public Counter getCounter(String counterName) {
    return new LocalCounter();
  }

  @Override
  public Histogram getHistogram(String histogramName) {
    return new LocalHistogram();
  }

  @Override
  public Gauge getGauge(String gaugeName) {
    return new LocalGauge();
  }

  @Override
  public Gauge getGauge(String gaugeName, Map<String, String> tags) {
    return getGauge(gaugeName);
  }

  @Override
  public Meter getMeter(String meterName, Map<String, String> tags) {
    return getMeter(meterName);
  }

  @Override
  public Counter getCounter(String counterName, Map<String, String> tags) {
    return getCounter(counterName);
  }

  @Override
  public Histogram getHistogram(String histogramName, Map<String, String> tags) {
    return getHistogram(histogramName);
  }
}
