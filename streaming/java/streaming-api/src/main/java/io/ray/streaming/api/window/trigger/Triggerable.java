package io.ray.streaming.api.window.trigger;

import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.timer.Timer;
import io.ray.streaming.api.window.timer.TimerService;

/**
 * Interface for things that can be triggered by {@link TimerService}.
 *
 * @param <W> type of timer's key
 */
public interface Triggerable<K, W extends Window> {

  /** Invoked when a processing-time timer fires. */
  void onProcessingTime(Timer<K, W> timer) throws Exception;

  /** Invoked when an event-time timer fires. */
  void onEventTime(Timer<K, W> timer) throws Exception;
}
