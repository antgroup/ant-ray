package io.ray.streaming.api.window.trigger;

import io.ray.streaming.api.window.Window;
import io.ray.streaming.common.utils.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that fires when the current system time passes the window's processing time.
 *
 * @param <T> record type
 * @param <W> window type
 */
public class AfterProcessingTimeTrigger<K, T, W extends Window> extends AbstractTrigger<K, T, W> {

  private static final Logger LOG = LoggerFactory.getLogger(AfterProcessingTimeTrigger.class);

  public static <K, T, W extends Window> AfterProcessingTimeTrigger<K, T, W> of() {
    return new AfterProcessingTimeTrigger<>();
  }

  @Override
  public TriggerResult onElement(T element, long timestamp, W window) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long currentTime, W window) throws Exception {
    LOG.debug(
        "AfterProcessingTimeTrigger on processing time, current: {}, window: {}.",
        TimeUtil.getReadableTime(currentTime),
        window);
    return TriggerResult.FIRE_AND_PURGE;
  }

  @Override
  public TriggerResult onWatermark(W window) throws RuntimeException {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void onMerge(K key, W mergedStateResult, W toMerged) throws Exception {
    if (toMerged.maxTime() > triggerContext.getCurrentProcessingTime()) {
      LOG.debug("AfterProcessingTimeTrigger on merge with new window: {}, key: {}.", toMerged, key);
      triggerContext
          .getTimerService()
          .registerProcessingTimeTimer(key, toMerged, toMerged.maxTime());
    }
  }

  @Override
  public void clear(K key, W window) throws Exception {
    triggerContext.getTimerService().unregisterProcessingTimeTimer(key, window);
  }
}
