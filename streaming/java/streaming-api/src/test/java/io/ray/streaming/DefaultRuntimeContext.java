package io.ray.streaming;

import io.ray.state.api.KeyMapState;
import io.ray.state.api.KeyMapStateDescriptor;
import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.state.api.ValueState;
import io.ray.state.api.ValueStateDescriptor;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.common.metric.local.LocalMetricGroup;
import java.util.HashMap;
import java.util.Map;

/** Default RuntimeContext for test only. */
public class DefaultRuntimeContext implements RuntimeContext {

  @Override
  public int getTaskId() {
    return 0;
  }

  @Override
  public int getTaskIndex() {
    return 0;
  }

  @Override
  public int getTaskParallelism() {
    return 0;
  }

  @Override
  public int getOperatorId() {
    return 0;
  }

  @Override
  public String getOperatorName() {
    return "";
  }

  @Override
  public Map<String, String> getJobConfig() {
    return new HashMap<>();
  }

  @Override
  public Map<String, String> getOpConfig() {
    return new HashMap<>();
  }

  @Override
  public Object getCurrentKey() {
    return null;
  }

  @Override
  public void setCurrentKey(Object currentKey) {}

  @Override
  public <V> ValueState<V> getValueState(ValueStateDescriptor<V> stateDescriptor) {
    return null;
  }

  @Override
  public <V> ValueState<V> getNonKeyedValueState(ValueStateDescriptor<V> stateDescriptor) {
    return null;
  }

  @Override
  public <K, V> KeyValueState<K, V> getKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    return null;
  }

  @Override
  public <K, V> KeyValueState<K, V> getNonKeyedKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    return null;
  }

  @Override
  public <K, T, Y> KeyMapState<K, T, Y> getKeyMapState(
      KeyMapStateDescriptor<K, T, Y> stateDescriptor) {
    return null;
  }

  @Override
  public long getCheckpointId() {
    return 0;
  }

  @Override
  public MetricGroup getMetric() {
    return new LocalMetricGroup();
  }
}
