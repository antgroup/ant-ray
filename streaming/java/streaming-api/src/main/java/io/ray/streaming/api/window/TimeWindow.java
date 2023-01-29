package io.ray.streaming.api.window;

import com.google.common.base.MoreObjects;
import io.ray.state.util.MathUtils;
import io.ray.streaming.api.window.assigner.MergeableWindowAssigner;
import io.ray.streaming.common.tuple.Tuple2;
import io.ray.streaming.common.utils.TimeUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The most common basic type of {@link Window}. These kinds of window represent a time interval,
 * and the data in the interval will be processed as user's intention.
 *
 * <p>{@link TimeWindow} including: processing time series and event time series.
 */
public class TimeWindow implements Window {

  private final long start;
  private final long end;

  public TimeWindow(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public static TimeWindow of(long start, long end) {
    return new TimeWindow(start, end);
  }

  /** Returns true if the current window intersect with the given window. */
  public boolean intersect(TimeWindow other) {
    return this.start <= other.end && this.end >= other.start;
  }

  /** Returns true if the current window is the subset of the given window. */
  public boolean isSubsetOf(TimeWindow other) {
    return this.start >= other.start && this.end <= other.end;
  }

  /** Returns the minimal window covers both this window and the given window. */
  public TimeWindow cover(TimeWindow other) {
    return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
  }

  /** For use by merging {@link MergeableWindowAssigner}. */
  public static Map<TimeWindow, Set<TimeWindow>> mergeWindows(Collection<TimeWindow> windows) {
    // sort the windows by the start time and then merge overlapping windows
    List<TimeWindow> sortedWindows = new ArrayList<>(windows);
    sortedWindows.sort(Comparator.comparingLong(TimeWindow::minTime));

    List<Tuple2<TimeWindow, Set<TimeWindow>>> mergedWindows = new ArrayList<>();
    Tuple2<TimeWindow, Set<TimeWindow>> currentMerged = null;

    for (TimeWindow candidate : sortedWindows) {
      if (currentMerged == null) {
        currentMerged = new Tuple2<>();
        currentMerged.f0 = candidate;
        currentMerged.f1 = new HashSet<>();
      } else if (currentMerged.f0.intersect(candidate)) {
        currentMerged.f0 = currentMerged.f0.cover(candidate);
      } else {
        mergedWindows.add(currentMerged);
        currentMerged = new Tuple2<>();
        currentMerged.f0 = candidate;
        currentMerged.f1 = new HashSet<>();
      }
      currentMerged.f1.add(candidate);
    }

    if (currentMerged != null) {
      mergedWindows.add(currentMerged);
    }

    Map<TimeWindow, Set<TimeWindow>> mergeResults = new HashMap<>();
    for (Tuple2<TimeWindow, Set<TimeWindow>> mergedWindow : mergedWindows) {
      if (mergedWindow.f1.size() > 1) {
        mergeResults.put(mergedWindow.f0, mergedWindow.f1);
      }
    }
    return mergeResults;
  }

  @Override
  public int hashCode() {
    return MathUtils.longToIntWithBitMixing(start + end);
  }

  @SuppressWarnings("EqualsHashCode")
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeWindow window = (TimeWindow) o;
    return end == window.end && start == window.start;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("start", TimeUtil.getReadableTime(start))
        .add("end", TimeUtil.getReadableTime(end))
        .add("obj", this.hashCode())
        .toString();
  }

  @Override
  public long maxTime() {
    return end;
  }

  @Override
  public long minTime() {
    return start;
  }
}
