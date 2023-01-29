package io.ray.streaming.api.window.trigger;

import com.google.common.base.MoreObjects;
import io.ray.state.api.KeyMapState;
import io.ray.state.api.KeyMapStateDescriptor;
import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.state.api.ValueState;
import io.ray.state.api.ValueStateDescriptor;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.timer.TimerService;
import io.ray.streaming.common.metric.MetricGroup;
import java.io.Serializable;

/** The runtime context of window trigger. */
public class TriggerContext<K, W extends Window> implements Serializable {

  private RuntimeContext runtimeContext;
  private TimerService<K, W> timerService;

  public TriggerContext(RuntimeContext ctx) {
    this.runtimeContext = ctx;
  }

  public String getOperatorId() {
    return String.valueOf(runtimeContext.getOperatorId());
  }

  public MetricGroup getMetricsGroup() {
    return runtimeContext.getMetric();
  }

  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    return this.runtimeContext.getValueState(stateDescriptor);
  }

  public <K, V> KeyValueState<K, V> getKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    return this.runtimeContext.getKeyValueState(stateDescriptor);
  }

  public <K, UK, UV> KeyMapState<K, UK, UV> getKeyMapState(
      KeyMapStateDescriptor<K, UK, UV> stateDescriptor) {
    return this.runtimeContext.getKeyMapState(stateDescriptor);
  }

  public Object getCurrentKey() {
    return runtimeContext.getCurrentKey();
  }

  public void setCurrentKey(Object currentKey) {
    runtimeContext.setCurrentKey(currentKey);
  }

  public TimerService<K, W> getTimerService() {
    return timerService;
  }

  public void setTimerService(TimerService<K, W> timerService) {
    this.timerService = timerService;
  }

  public long getCurrentProcessingTime() {
    if (timerService != null) {
      return timerService.getCurrentProcessingTime();
    }
    return System.currentTimeMillis();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("runtimeContext", runtimeContext).toString();
  }
}
