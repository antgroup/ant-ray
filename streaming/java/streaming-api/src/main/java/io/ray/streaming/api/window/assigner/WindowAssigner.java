package io.ray.streaming.api.window.assigner;

import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.trigger.Trigger;
import java.io.Serializable;
import java.util.Collection;

/**
 * To assign the specific type window.
 *
 * @param <K> key type
 * @param <T> value type
 * @param <W> window type
 */
public abstract class WindowAssigner<K, T, W extends Window> implements Serializable {

  public abstract Collection<W> assignWindows(T element, long timestamp);

  public boolean isEventTime() {
    return false;
  }

  public abstract Trigger<K, T, W> getDefaultTrigger();
}
