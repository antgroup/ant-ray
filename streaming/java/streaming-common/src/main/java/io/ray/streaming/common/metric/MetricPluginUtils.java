package io.ray.streaming.common.metric;

import java.util.HashMap;
import java.util.Map;

public class MetricPluginUtils {

  /**
   * build full metric name with all tag entries.
   *
   * @param metricName
   * @param tags
   * @return
   */
  public static String getMetricNameWithTags(String metricName, Map<String, String> tags) {
    StringBuilder sb = new StringBuilder();
    sb.append(metricName);
    tags.entrySet().stream()
        .forEachOrdered(
            x -> {
              sb.append('\n' + x.getKey() + ":" + x.getValue());
            });
    return sb.toString();
  }

  /**
   * build mixed tags in local priority first.
   *
   * @param localTags
   * @param globalTags
   * @return
   */
  public static Map<String, String> buildMixedTags(
      final Map<String, String> localTags, final Map<String, String> globalTags) {

    Map<String, String> mixedTags = new HashMap<>();
    mixedTags.putAll(globalTags);
    mixedTags.putAll(localTags);
    return mixedTags;
  }
}
