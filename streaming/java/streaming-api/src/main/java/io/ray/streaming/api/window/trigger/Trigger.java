package io.ray.streaming.api.window.trigger;

import io.ray.streaming.api.window.Window;

/** Window's trigger, decide when and how to trigger a window. */
public interface Trigger<K, T, W extends Window> {

  void init(TriggerContext<K, W> triggerContext);

  String getName();

  void setName(String name);

  TriggerResult onElement(T record, long timestamp, W window) throws Exception;

  TriggerResult onProcessingTime(long timestamp, W window) throws Exception;

  TriggerResult onWatermark(W window) throws Exception;

  void onMerge(K key, W mergedStateResult, W toMerged) throws Exception;

  void clear(K key, W window) throws Exception;
}
