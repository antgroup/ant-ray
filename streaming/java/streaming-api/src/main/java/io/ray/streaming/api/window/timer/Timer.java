package io.ray.streaming.api.window.timer;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.common.utils.TimeUtil;
import java.io.Serializable;
import java.util.Objects;

/**
 * Used for window timing.
 *
 * @param <W> type of window
 */
public class Timer<K, W extends Window> implements Comparable<Timer<?, ?>>, Serializable {

  /** The key for which the timer is scoped. */
  private final K key;

  /** The window for which the timer is scoped. */
  private final W window;

  /** The specified expiration time. */
  private final long time;

  public Timer(K key, W window, long time) {
    this.key = key;
    this.window = window;
    this.time = time;
  }

  public K getKey() {
    return key;
  }

  public W getWindow() {
    return window;
  }

  public long getTime() {
    return time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Timer<?, ?> timer = (Timer<?, ?>) o;
    return time == timer.time && window.equals(timer.window) && key.equals(timer.key);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("window", window)
        .add("time", TimeUtil.getReadableTime(time))
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(window, time);
  }

  @Override
  public int compareTo(Timer<?, ?> otherTimer) {
    return Long.compare(time, otherTimer.time);
  }
}
