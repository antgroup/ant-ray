package io.ray.streaming.api.window.trigger;

import com.google.common.base.MoreObjects;
import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.streaming.api.window.TriggerMode;
import io.ray.streaming.api.window.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that fires when the value count meet the requirement.
 *
 * @param <T> record type
 * @param <W> window type
 */
public class AtCountTrigger<K, T, W extends Window> extends AbstractTrigger<K, T, W> {

  private static final Logger LOG = LoggerFactory.getLogger(AtCountTrigger.class);

  private int count;
  private KeyValueState<Window, Integer> state;
  private TriggerMode mode;

  private AtCountTrigger(int count) {
    super();
    this.count = count;
    this.mode = TriggerMode.DISCARD;
  }

  private AtCountTrigger(int count, TriggerMode mode) {
    super();
    this.count = count;
    this.mode = mode;
  }

  public static <K, T, W extends Window> AtCountTrigger<K, T, W> of(int count) {
    return new AtCountTrigger<>(count);
  }

  public static <K, T, W extends Window> AtCountTrigger<K, T, W> of(int count, TriggerMode mode) {
    return new AtCountTrigger<>(count, mode);
  }

  @Override
  public TriggerResult onElement(T element, long timestamp, W window) throws Exception {
    if (!state.contains(window)) {
      state.put(window, 0);
    }
    state.put(window, state.get(window) + 1);
    LOG.debug("AtCountTrigger update state for window: {}, value: {}.", window, state.get(window));
    if (state.get(window) == count) {
      LOG.info("AtCount trigger meet count: {}, so fire at window: {} ", count, window);
      if (mode == TriggerMode.DISCARD) {
        return TriggerResult.FIRE_AND_PURGE;
      }
      return TriggerResult.FIRE;
    }
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long currentTs, W window) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onWatermark(W window) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void onMerge(K key, W mergedStateResult, W toMerged) throws Exception {
    // no need to merge, cuz the new merged window already inherited the state
  }

  @Override
  public void init(TriggerContext<K, W> ctx) {
    super.init(ctx);
    state = ctx.getKeyValueState(KeyValueStateDescriptor.build(name, Window.class, Integer.class));
  }

  @Override
  public void clear(K key, W window) throws Exception {
    LOG.debug("AtCount trigger clear window state, window: {}.", window);
    state.remove(window);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("count", count)
        .add("mode", mode)
        .toString();
  }
}
