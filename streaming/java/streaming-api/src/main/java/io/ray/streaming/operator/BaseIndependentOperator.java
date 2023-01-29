package io.ray.streaming.operator;

import java.util.Map;

/**
 * {@link BaseIndependentOperator} is a kind of operator not belonging to the DAG, but also need to
 * be described in job graph.
 *
 * <p>All the related extensions should extend {@link BaseIndependentOperator}, using {@link
 * BaseIndependentOperator#BaseIndependentOperator(Map)} as constructor.
 */
public abstract class BaseIndependentOperator {

  protected final Map<String, String> config;

  public BaseIndependentOperator(Map<String, String> config) {
    this.config = config;
  }

  public Map<String, String> getConfig() {
    return config;
  }
}
