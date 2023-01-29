package io.ray.streaming.api.window.assigner;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.streaming.api.window.TimeWindow;
import io.ray.streaming.api.window.WindowUtil;
import io.ray.streaming.api.window.trigger.AfterProcessingTimeTrigger;
import io.ray.streaming.api.window.trigger.Trigger;
import io.ray.streaming.common.utils.Time;
import java.util.Collection;
import java.util.Collections;

/**
 * Tumbling window based on processing time.
 *
 * <p>Window start: when an element is received
 *
 * <p>Window end: when there isn't any new element is received according to the timeout
 */
public class ProcessingTimeTumblingWindows<K, T> extends WindowAssigner<K, T, TimeWindow> {

  /** Window size, unit milliseconds. */
  private final long size;

  /** Start time, unit milliseconds. */
  private long startTime;

  /** Offset from now, unit milliseconds. */
  private final long offset;

  /** Should window start based on the input start time. */
  private boolean isBaseOnStart;

  private ProcessingTimeTumblingWindows(long size, long offset, boolean isBaseOnStart) {
    if (!isBaseOnStart) {
      Preconditions.checkArgument(
          offset >= 0 && offset < size, "TumblingProcessingTimeWindow with illegal arguments.");
    }

    this.size = size;
    this.startTime = offset;
    this.isBaseOnStart = isBaseOnStart;
    this.offset = offset;
  }

  /**
   * Create a new tumbling window use processing time, start right now.
   *
   * @param size window size
   * @return the assignment of {@link ProcessingTimeTumblingWindows}
   */
  public static <K, T> ProcessingTimeTumblingWindows<K, T> of(Time size) {
    return of(size, Time.milliseconds(0));
  }

  /**
   * Create a new tumbling window use processing time, start by the specified offset from now on.
   *
   * @param size window size
   * @return the assignment of {@link ProcessingTimeTumblingWindows}
   */
  public static <K, T> ProcessingTimeTumblingWindows<K, T> of(Time size, Time offset) {
    return new ProcessingTimeTumblingWindows<>(
        size.toMilliseconds(), offset.toMilliseconds() % size.toMilliseconds(), false);
  }

  /**
   * Create a new tumbling window use processing time, start by the specified start time.
   *
   * @param size window size
   * @param startTime specified start time
   * @return the assignment of {@link ProcessingTimeTumblingWindows}
   */
  public static <K, T> ProcessingTimeTumblingWindows<K, T> of(Time size, long startTime) {
    return new ProcessingTimeTumblingWindows<>(size.toMilliseconds(), startTime, true);
  }

  @Override
  public Collection<TimeWindow> assignWindows(T element, long time) {
    time = System.currentTimeMillis();
    long start =
        isBaseOnStart
            ? WindowUtil.getWindowStartTimeByStartTime(time, startTime, size)
            : WindowUtil.getWindowStartTimeByOffset(time, offset, size);
    return Collections.singletonList(new TimeWindow(start, start + size));
  }

  @Override
  public Trigger<K, T, TimeWindow> getDefaultTrigger() {
    return AfterProcessingTimeTrigger.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("startTime", startTime)
        .add("size", size)
        .add("offset", offset)
        .add("startTimeBased", isBaseOnStart)
        .toString();
  }
}
