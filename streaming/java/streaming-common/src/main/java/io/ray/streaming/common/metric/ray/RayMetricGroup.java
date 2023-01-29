package io.ray.streaming.common.metric.ray;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.ray.streaming.common.metric.Counter;
import io.ray.streaming.common.metric.Gauge;
import io.ray.streaming.common.metric.Histogram;
import io.ray.streaming.common.metric.Meter;
import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.common.metric.MetricPluginUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayMetricGroup implements MetricGroup {

  private static final Logger LOG = LoggerFactory.getLogger(RayMetricGroup.class);
  /** Reporting in interval time for reducing recoding race. */
  public static final int REPORTING_GAP = 5000;

  private static ExecutorService rayMetricService =
      new ThreadPoolExecutor(
          1,
          1,
          60,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(),
          new ThreadFactoryBuilder().setNameFormat("async-ray-metric-%d").build(),
          new AbortPolicy());

  private Map<String, String> globalTags;
  private Map<String, Meter> asmMeterMap;
  private Map<String, Counter> asmCounterMap;
  private Map<String, Histogram> asmHistogramMap;
  private Map<String, Gauge> asmGaugeMap;
  private String serviceName;

  public RayMetricGroup(String serviceName, Map<String, String> globalTags) {
    this.serviceName = serviceName;
    this.globalTags = globalTags;
  }

  @Override
  public void init(Map<String, String> jobConfig) {
    // we need the job name, operator index
    LOG.info("JobConfig {}", jobConfig);

    asmMeterMap = new ConcurrentHashMap<>();
    asmCounterMap = new ConcurrentHashMap<>();
    asmHistogramMap = new ConcurrentHashMap<>();
    asmGaugeMap = new ConcurrentHashMap<>();
  }

  private String buildFullMetricName(String metricName) {
    if (null != serviceName) {
      return serviceName + "." + metricName;
    }
    return metricName;
  }

  @Override
  public Gauge getGauge(String gaugeName) {
    return getGauge(gaugeName, new HashMap<>());
  }

  @Override
  public Meter getMeter(String meterName) {
    return getMeter(meterName, new HashMap<>());
  }

  @Override
  public Counter getCounter(String counterName) {
    return getCounter(counterName, new HashMap<>());
  }

  @Override
  public Histogram getHistogram(String histogramName) {
    return getHistogram(histogramName, new HashMap<>());
  }

  @Override
  public Gauge getGauge(String gaugeName, Map<String, String> tags) {
    String metricNameWithTags = MetricPluginUtils.getMetricNameWithTags(gaugeName, tags);
    if (asmGaugeMap.containsKey(metricNameWithTags)) {
      return asmGaugeMap.get(metricNameWithTags);
    } else {
      gaugeName = buildFullMetricName(gaugeName);
      LOG.info("Register gauge: {}.", metricNameWithTags);
      Map<String, String> mixedTags = MetricPluginUtils.buildMixedTags(tags, globalTags);
      Gauge asmMeter = new RayGauge(gaugeName, mixedTags, rayMetricService);
      asmGaugeMap.put(metricNameWithTags, asmMeter);
      return asmMeter;
    }
  }

  @Override
  public Meter getMeter(String meterName, Map<String, String> tags) {
    String metricNameWithTags = MetricPluginUtils.getMetricNameWithTags(meterName, tags);
    if (asmMeterMap.containsKey(metricNameWithTags)) {
      return asmMeterMap.get(metricNameWithTags);
    } else {
      meterName = buildFullMetricName(meterName);
      LOG.info("Register meter: {}.", metricNameWithTags);
      Map<String, String> mixedTags = MetricPluginUtils.buildMixedTags(tags, globalTags);
      Meter meter = new RayMeter(meterName, mixedTags, rayMetricService);
      asmMeterMap.put(metricNameWithTags, meter);
      return meter;
    }
  }

  @Override
  public Counter getCounter(String counterName, Map<String, String> tags) {
    String metricNameWithTags = MetricPluginUtils.getMetricNameWithTags(counterName, tags);
    if (asmCounterMap.containsKey(metricNameWithTags)) {
      return asmCounterMap.get(metricNameWithTags);
    } else {
      counterName = buildFullMetricName(counterName);
      LOG.info("Register counter: {}.", metricNameWithTags);
      Map<String, String> mixedTags = MetricPluginUtils.buildMixedTags(tags, globalTags);
      Counter counter = new RayCounter(counterName, mixedTags, rayMetricService);
      asmCounterMap.put(metricNameWithTags, counter);
      return counter;
    }
  }

  @Override
  public Histogram getHistogram(String histogramName, Map<String, String> tags) {
    String metricNameWithTags = MetricPluginUtils.getMetricNameWithTags(histogramName, tags);
    if (asmHistogramMap.containsKey(metricNameWithTags)) {
      return asmHistogramMap.get(metricNameWithTags);
    } else {
      histogramName = buildFullMetricName(histogramName);
      LOG.info("Register histogram: {}.", metricNameWithTags);
      Map<String, String> mixedTags = MetricPluginUtils.buildMixedTags(tags, globalTags);
      Histogram histogram = new RayHistogram(histogramName, mixedTags, rayMetricService);
      asmHistogramMap.put(metricNameWithTags, histogram);
      return histogram;
    }
  }
}
