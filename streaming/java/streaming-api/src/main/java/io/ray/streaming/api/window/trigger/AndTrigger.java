package io.ray.streaming.api.window.trigger;

import com.google.common.base.MoreObjects;
import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.streaming.api.window.Window;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Addition of {@link Trigger}.
 *
 * @param <T> record type
 * @param <W> window type
 */
public class AndTrigger<K, T, W extends Window> extends AbstractTrigger<K, T, W> {

  private static final Logger LOG = LoggerFactory.getLogger(AndTrigger.class);

  private List<Trigger<K, T, W>> triggerList;
  private KeyValueState<Window, TriggerResult[]> state;

  private AndTrigger(List<Trigger<K, T, W>> triggerList) {
    super();
    this.triggerList = triggerList;
  }

  public static <K, T, W extends Window> AndTrigger<K, T, W> of(Trigger<K, T, W>... triggers) {
    return new AndTrigger<>(Arrays.asList(triggers));
  }

  public static <K, T, W extends Window> AndTrigger<K, T, W> of(List<Trigger<K, T, W>> triggers) {
    return new AndTrigger<>(triggers);
  }

  @Override
  public void init(TriggerContext<K, W> triggerContext) {
    super.init(triggerContext);
    int index = 0;
    for (Trigger<K, T, W> trigger : triggerList) {
      trigger.setName(String.format("%s-%s-%d", name, trigger.getName(), index++));
      trigger.init(triggerContext);
    }
    state =
        triggerContext.getKeyValueState(
            KeyValueStateDescriptor.build(name, Window.class, TriggerResult[].class));
  }

  @Override
  public TriggerResult onElement(T element, long timestamp, W window) throws Exception {
    TriggerResult[] triggerResults = initOrGetState(window);
    for (int i = 0; i < triggerList.size(); i++) {
      Trigger<K, T, W> trigger = triggerList.get(i);
      TriggerResult tmpResult = trigger.onElement(element, timestamp, window);
      if (tmpResult.isFire()) {
        LOG.info("One of the trigger: {} fire at window: {}.", trigger, window);
        triggerResults[i] = tmpResult;
      }
    }
    state.put(window, triggerResults);

    TriggerResult finalResult = judeResult(triggerResults, window);
    if (finalResult.isFire()) {
      LOG.info(
          "AndTrigger: {}, meet: {}, so fire at window: {} for key: {}.",
          this,
          element,
          window,
          triggerContext.getCurrentKey());
    }
    return finalResult;
  }

  @Override
  public TriggerResult onProcessingTime(long currentTs, W window) throws Exception {
    TriggerResult[] triggerResults = initOrGetState(window);
    for (int i = 0; i < triggerList.size(); i++) {
      Trigger<K, T, W> trigger = triggerList.get(i);
      TriggerResult tmp = trigger.onProcessingTime(currentTs, window);
      if (tmp.isFire()) {
        triggerResults[i] = tmp;
      }
    }
    state.put(window, triggerResults);

    TriggerResult finalResult = judeResult(triggerResults, window);
    if (finalResult.isFire()) {
      LOG.info(
          "AndTrigger:{}, meet: {}, so fire at window: {} for key: {}. ",
          this,
          currentTs,
          window,
          triggerContext.getCurrentKey());
    }
    return finalResult;
  }

  @Override
  public TriggerResult onWatermark(W window) throws Exception {
    // TODO
    return TriggerResult.CONTINUE;
  }

  @Override
  public void onMerge(K key, W mergedStateResult, W toMerged) throws Exception {
    for (Trigger<K, T, W> trigger : triggerList) {
      trigger.onMerge(key, mergedStateResult, toMerged);
    }
  }

  @Override
  public void clear(K key, W window) throws Exception {
    state.remove(window);
    LOG.debug("Trigger clean state for window: {}.", window);

    for (Trigger<K, T, W> trigger : triggerList) {
      trigger.clear(key, window);
    }
  }

  private TriggerResult[] initOrGetState(W window) throws Exception {
    TriggerResult[] triggerResults = state.get(window);
    if (triggerResults == null) {
      triggerResults = new TriggerResult[triggerList.size()];
    }
    return triggerResults;
  }

  private TriggerResult judeResult(TriggerResult[] results, W window) throws Exception {
    TriggerResult triggerResult = TriggerResult.FIRE_AND_PURGE;
    for (TriggerResult result : results) {
      if (result == null) {
        return TriggerResult.CONTINUE;
      }
      triggerResult = triggerResult.merge(result);
    }
    if (triggerResult.isFire()) {
      state.put(window, null);
    }
    return triggerResult;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("triggerList", triggerList)
        .toString();
  }
}
