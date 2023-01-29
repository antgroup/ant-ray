package io.ray.streaming.api.window.assigner;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.window.TimeWindow;
import io.ray.streaming.api.window.trigger.AfterProcessingTimeTrigger;
import io.ray.streaming.api.window.trigger.Trigger;
import io.ray.streaming.common.utils.Time;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.msgpack.core.Preconditions;

/**
 * Session window based on processing time.
 *
 * <p>Window start: when an element is received.
 *
 * <p>Window end: start time + the session timeout.
 */
public class ProcessingTimeSessionWindows<K, T> extends MergeableWindowAssigner<K, T, TimeWindow> {

  /** Time size of the current session, unit milliseconds. */
  private final long sessionTimeout;

  private ProcessingTimeSessionWindows(long sessionTimeout) {
    Preconditions.checkArgument(
        sessionTimeout > 0, "ProcessingTimeSessionWindows's sessionTimeout must > 0.");
    this.sessionTimeout = sessionTimeout;
  }

  /**
   * Create a new session window use processing time by a static session timeout.
   *
   * @param sessionTimeout the time gap between sessions
   * @return the assignment of {@link ProcessingTimeSessionWindows}
   */
  public static <K, T> ProcessingTimeSessionWindows<K, T> of(Time sessionTimeout) {
    return new ProcessingTimeSessionWindows<>(sessionTimeout.toMilliseconds());
  }

  @Override
  public Collection<TimeWindow> assignWindows(T element, long time) {
    return Collections.singletonList(new TimeWindow(time, time + sessionTimeout));
  }

  @Override
  public Trigger<K, T, TimeWindow> getDefaultTrigger() {
    return AfterProcessingTimeTrigger.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("sessionTimeout", sessionTimeout).toString();
  }

  @Override
  public Map<TimeWindow, Set<TimeWindow>> mergeWindows(Collection<TimeWindow> windows) {
    return TimeWindow.mergeWindows(windows);
  }
}
