package io.ray.streaming.api.window.assigner;

import io.ray.streaming.api.window.Window;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * For {@code WindowAssigner} that can b merged. e.g. session window and sliding window
 *
 * @param <T> value type
 * @param <W> window type
 */
public abstract class MergeableWindowAssigner<K, T, W extends Window>
    extends WindowAssigner<K, T, W> {

  /**
   * Determines which windows (if any) should be merged.
   *
   * @param windows windows need to be merged
   */
  public abstract Map<W, Set<W>> mergeWindows(Collection<W> windows);
}
