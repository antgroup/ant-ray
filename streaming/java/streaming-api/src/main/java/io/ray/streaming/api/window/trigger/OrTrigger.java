package io.ray.streaming.api.window.trigger;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.window.Window;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Will return result if any {@link Trigger} in the list fired.
 *
 * @param <T> record type
 * @param <W> window type
 */
public class OrTrigger<K, T, W extends Window> extends AbstractTrigger<K, T, W> {

  private static final Logger LOG = LoggerFactory.getLogger(OrTrigger.class);

  private List<Trigger<K, T, W>> triggerList;

  private OrTrigger(List<Trigger<K, T, W>> triggerList) {
    super();
    this.triggerList = triggerList;
  }

  public static <K, T, W extends Window> OrTrigger<K, T, W> of(Trigger<K, T, W>... args) {
    return new OrTrigger<>(Arrays.asList(args));
  }

  public static <K, T, W extends Window> OrTrigger<K, T, W> of(List<Trigger<K, T, W>> triggers) {
    return new OrTrigger<>(triggers);
  }

  @Override
  public void init(TriggerContext ctx) {
    super.init(ctx);
    int index = 0;
    for (Trigger<K, T, W> trigger : triggerList) {
      trigger.setName(String.format("%s-%s-%d", name, trigger.getName(), index++));
      trigger.init(ctx);
    }
  }

  @Override
  public TriggerResult onElement(T element, long timestamp, W window) throws Exception {
    TriggerResult triggerResult = TriggerResult.CONTINUE;
    for (Trigger<K, T, W> trigger : triggerList) {
      TriggerResult tmpResult = trigger.onElement(element, timestamp, window);
      if (tmpResult.isFire() && !triggerResult.isFire()) {
        LOG.info("OrTrigger: {} fire at window: {}.", trigger, window);
        triggerResult = tmpResult;
      }
    }
    return triggerResult;
  }

  @Override
  public TriggerResult onProcessingTime(long currentTs, W window) throws Exception {
    TriggerResult triggerResult = TriggerResult.CONTINUE;
    for (Trigger<K, T, W> trigger : triggerList) {
      TriggerResult tmp = trigger.onProcessingTime(currentTs, window);
      if (tmp.isFire() && !triggerResult.isFire()) {
        LOG.info("OrTrigger: {} fire at window: {}.", trigger, window);
        triggerResult = tmp;
      }
    }
    return triggerResult;
  }

  @Override
  public TriggerResult onWatermark(W window) throws Exception {
    // TODO
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(K key, W window) throws Exception {
    LOG.debug("Trigger clear state for window: {}.", window);
    for (Trigger<K, T, W> trigger : triggerList) {
      trigger.clear(key, window);
    }
  }

  @Override
  public void onMerge(K key, W mergedStateResult, W toMerged) throws Exception {
    for (Trigger<K, T, W> trigger : triggerList) {
      trigger.onMerge(key, mergedStateResult, toMerged);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("triggerList", triggerList)
        .toString();
  }
}
