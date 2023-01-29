package io.ray.streaming.api.window.trigger;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.window.Window;

/**
 * Generic trigger implements.
 *
 * @param <T> record type
 * @param <W> window type
 */
public abstract class AbstractTrigger<K, T, W extends Window> implements Trigger<K, T, W> {

  protected transient TriggerContext<K, W> triggerContext;
  protected String name;
  protected boolean isMultiTrigger = false;

  public AbstractTrigger() {
    this.name = this.getClass().getSimpleName();
  }

  @Override
  public void init(TriggerContext<K, W> triggerContext) {
    this.triggerContext = triggerContext;
    this.setOperatorIdentity(triggerContext.getOperatorId());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public abstract TriggerResult onElement(T element, long timestamp, W window) throws Exception;

  @Override
  public abstract TriggerResult onProcessingTime(long currentTs, W window) throws Exception;

  @Override
  public abstract TriggerResult onWatermark(W window) throws Exception;

  @Override
  public void onMerge(K key, W mergedStateResult, W toMerged) throws Exception {}

  @Override
  public void clear(K key, W window) throws Exception {}

  protected void setOperatorIdentity(String operatorIdentity) {
    this.name = operatorIdentity + "-" + name;
  }

  public boolean hasMultiTrigger() {
    return this.isMultiTrigger;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("isMultiTrigger", isMultiTrigger)
        .toString();
  }
}
