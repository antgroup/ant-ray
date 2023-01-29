package io.ray.streaming.api.window;

import com.google.common.base.MoreObjects;
import io.ray.state.api.KeyValueState;
import io.ray.streaming.api.window.assigner.MergeableWindowAssigner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To merge and keep the merged windows.
 *
 * @param <W> type of window
 */
public class WindowMerger<K, W extends Window> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowMerger.class);

  /**
   * Mapping for the merged window and it's state window.
   *
   * <p>Key: merged window
   *
   * <p>Value: the state window of the merged window
   */
  private final Map<W, W> lastStateWindowMapping;

  private final Map<W, W> currentStateWindowMapping;

  /** Actual state to persist mapping info for merged window, and it's state. */
  private final KeyValueState<W, W> state;

  private final MergeableWindowAssigner<K, ?, W> windowAssigner;

  /** Restores a {@link WindowMerger} from the given state. */
  public WindowMerger(MergeableWindowAssigner<K, ?, W> windowAssigner, KeyValueState<W, W> state)
      throws Exception {

    this.windowAssigner = windowAssigner;
    currentStateWindowMapping = new HashMap<>();

    if (state != null) {
      Iterator<Map.Entry<W, W>> stateIterator = state.iterator();
      while (stateIterator != null && stateIterator.hasNext()) {
        Map.Entry<W, W> window = stateIterator.next();
        currentStateWindowMapping.put(window.getKey(), window.getValue());
      }
    }
    this.state = state;

    lastStateWindowMapping = new HashMap<>();
    lastStateWindowMapping.putAll(currentStateWindowMapping);
  }

  /** Persist the updated mapping to the given state if the mapping changed since initialization. */
  public void persist() throws Exception {
    if (!currentStateWindowMapping.equals(lastStateWindowMapping)) {
      state.clear();
      for (Map.Entry<W, W> window : currentStateWindowMapping.entrySet()) {
        state.put(window.getKey(), window.getValue());
      }
    }
  }

  /**
   * Get the state window by the specified merged window.
   *
   * @param window The window for which to get the state window.
   */
  public W getActualStateWindow(W window) {
    return currentStateWindowMapping.get(window);
  }

  /**
   * Removes the given window from the merged windows.
   *
   * @param window The {@code Window} to remove.
   */
  public void discardWindow(W window) {
    this.currentStateWindowMapping.remove(window);
    LOG.debug("Discard window: {}, state window left: {}.", window, currentStateWindowMapping);
  }

  /**
   * Adds a new {@code Window} to the set of in-flight windows. It might happen that this triggers
   * merging of previously in-flight windows. In that case, the provided {@link MergeFunction} is
   * called.
   *
   * <p>Procedure: 1) get the new merged window by merging function in {@link
   * MergeableWindowAssigner}. 2) update the state window mapping according to the upper result
   *
   * @param newWindow new window to be merged
   * @param mergeFunction the callback function when merging is called
   * @return result window (not the actual state window)
   * @throws Exception exception
   */
  public W mergeNewWindow(W newWindow, MergeFunction<W> mergeFunction) throws Exception {
    // add the new window as a new starting window to the existing starting window
    List<W> windows = new ArrayList<>();
    windows.addAll(currentStateWindowMapping.keySet());
    windows.add(newWindow);

    // get the new merged window（time redefined）result (including the input new window)
    final Map<W, Set<W>> mergeResults = windowAssigner.mergeWindows(windows);
    LOG.debug("Merged result: {} for windows: {}.", mergeResults, windows);

    // define the result window for current merging
    W resultWindow = newWindow;
    Boolean mergedNewWindow = false;

    // update the state window mapping by the merged results
    for (Map.Entry<W, Set<W>> result : mergeResults.entrySet()) {
      W eachMergeResult = result.getKey();
      Set<W> mergedWindows = result.getValue();
      if (mergedWindows.remove(newWindow)) {
        mergedNewWindow = true;
        resultWindow = eachMergeResult;
      }

      // get the last state object for the last merged window
      // and update it as the current final merged window's state object
      //
      // Reason: for every merged window, it's state is a unique object
      // stored in the mapping(as value)
      W lastMergedStateWindow = currentStateWindowMapping.get(mergedWindows.iterator().next());
      if (lastMergedStateWindow != null) {
        currentStateWindowMapping.put(eachMergeResult, lastMergedStateWindow);
        LOG.debug(
            "Merge the state mapping with: {}-{}. Mapping(size: {}) now: {}, new window: {}, merged windows: {}.",
            eachMergeResult,
            lastMergedStateWindow,
            currentStateWindowMapping.size(),
            currentStateWindowMapping,
            newWindow,
            mergedWindows);
      }

      // TODO: We copy, put the state directly, and remove the history without state-merging for
      //  now. we should use actual state-merging in {@link AbstractWindowOperator} to improve the
      //  performance for state updating later.

      // remove history state
      for (W mergedWindow : mergedWindows) {
        if (!mergedWindow.equals(resultWindow)) {
          discardWindow(mergedWindow);
        }
      }

      // merge state:
      // Notice: don't merge the new window itself, it never had any state associated with it
      if (!(mergedWindows.contains(eachMergeResult) && mergedWindows.size() == 1)) {
        mergeFunction.merge(
            resultWindow,
            currentStateWindowMapping.get(resultWindow),
            eachMergeResult,
            mergedWindows);
      } else {
        LOG.debug("Skip merging because first state window is merged.");
      }
    }

    // create a new window without merging if there isn't any window need to be merged
    if (mergeResults.isEmpty() || (resultWindow.equals(newWindow) && !mergedNewWindow)) {
      LOG.debug("First time updating the state mapping use the first window: {}.", resultWindow);
      this.currentStateWindowMapping.put(resultWindow, resultWindow);
    }

    return resultWindow;
  }

  /**
   * Callback for {@link #mergeNewWindow(Window, MergeFunction)}.
   *
   * @param <W> window type
   */
  public interface MergeFunction<W> {

    /**
     * This gets called when a window merging occurs.
     *
     * @param mergedResult the final merged result
     * @param mergedStateResult the final merged result's state
     * @param eachMergedResult each merged result
     * @param mergedWindows collection of all the merged {@link Window}
     * @throws Exception exception
     */
    void merge(W mergedResult, W mergedStateResult, W eachMergedResult, Collection<W> mergedWindows)
        throws Exception;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("currentWindowMapping", currentStateWindowMapping)
        .add("windowAssigner", windowAssigner)
        .toString();
  }
}
