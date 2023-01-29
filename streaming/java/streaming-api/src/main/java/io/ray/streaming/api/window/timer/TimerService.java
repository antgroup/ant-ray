package io.ray.streaming.api.window.timer;

import com.google.common.base.Preconditions;
import io.ray.state.api.ValueState;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.trigger.Triggerable;
import io.ray.streaming.common.utils.NoRepeatedPriorityBlockingQueue;
import io.ray.streaming.common.utils.TimeUtil;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that control timer in order to trigger window properly.
 *
 * @param <W> type of window
 */
public class TimerService<K, W extends Window> {

  private static final Logger LOG = LoggerFactory.getLogger(TimerService.class);

  /** Processing time timers that are currently in-flight. */
  private final ValueState<PriorityBlockingQueue<Timer<K, W>>> priorityQueueState;

  private PriorityBlockingQueue<Timer<K, W>> processingTimeTimersQueue;

  private final Triggerable<K, W> triggerTarget;
  private ScheduledFuture<?> nextTimer;

  private final ScheduledExecutorService timeTriggerService =
      new ScheduledThreadPoolExecutor(
          1,
          new BasicThreadFactory.Builder()
              .namingPattern("timeTrigger-schedule-pool-%d")
              .daemon(true)
              .build());

  public TimerService(
      Triggerable<K, W> triggerTarget,
      ValueState<PriorityBlockingQueue<Timer<K, W>>> priorityQueueState) {
    Preconditions.checkNotNull(triggerTarget, "Trigger target can not be null.");
    Preconditions.checkNotNull(priorityQueueState, "Queue state can not be null.");
    this.triggerTarget = triggerTarget;
    this.priorityQueueState = priorityQueueState;

    try {
      if (priorityQueueState.value() == null) {
        priorityQueueState.update(new NoRepeatedPriorityBlockingQueue<>());
      }
      this.processingTimeTimersQueue = priorityQueueState.value();
    } catch (Exception e) {
      LOG.error("Failed to get priority queue from state.", e);
      this.processingTimeTimersQueue = new NoRepeatedPriorityBlockingQueue<>();
    }
  }

  public void startTimeService() {
    LOG.debug("Start window time service, trigger target: {}.", triggerTarget);
    final Timer<K, W> headTimer = processingTimeTimersQueue.peek();
    if (headTimer != null) {
      nextTimer = initProcessingTimeTimer(headTimer.getTime(), this::onProcessingTime);
    }
  }

  public void stopTimeService() {
    LOG.debug("Stop window time service, trigger target: {}.", triggerTarget);
    processingTimeTimersQueue.clear();
    if (nextTimer != null) {
      nextTimer.cancel(true);
    }
  }

  public void registerProcessingTimeTimer(K key, W window) {
    registerProcessingTimeTimer(key, window, window.maxTime());
  }

  public synchronized void registerProcessingTimeTimer(K key, W window, long time) {
    Timer<K, W> newTimer = new Timer<>(key, window, time);

    LOG.debug(
        "Register processing-time timer: {}, timer queue(size:{}): {}.",
        newTimer,
        processingTimeTimersQueue.size(),
        processingTimeTimersQueue);
    Timer<K, W> originalHeadTimer = processingTimeTimersQueue.peek();
    if (processingTimeTimersQueue.add(newTimer)) {
      long nextTriggerTime =
          originalHeadTimer != null ? originalHeadTimer.getTime() : Long.MAX_VALUE;

      // pick up the earlier timer
      if (time < nextTriggerTime) {
        if (nextTimer != null) {
          nextTimer.cancel(false);
        }
        nextTimer = initProcessingTimeTimer(time, this::onProcessingTime);
        LOG.debug("Register timer: {} for window: {}.", TimeUtil.getReadableTime(time), window);
      } else {
        LOG.debug(
            "The new time < the next trigger time, new time: {}, next trigger time: {}.",
            TimeUtil.getReadableTime(time),
            TimeUtil.getReadableTime(nextTriggerTime));
      }
    }
  }

  public void unregisterProcessingTimeTimer(K key, W window) {
    unregisterProcessingTimeTimer(key, window, window.maxTime());
  }

  public synchronized void unregisterProcessingTimeTimer(K key, W window, long time) {
    Timer<K, W> target = new Timer<>(key, window, time);
    processingTimeTimersQueue.remove(target);
    LOG.debug("Timer queue after removing: {}.", processingTimeTimersQueue);
  }

  private void onProcessingTime(long time) throws Exception {
    nextTimer = null;
    Timer<K, W> timer;

    LOG.debug(
        "Time service on processing time, time: {}, timer: {}, all timers: {}.",
        TimeUtil.getReadableTime(time),
        processingTimeTimersQueue.peek(),
        processingTimeTimersQueue);
    while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTime() <= time) {
      processingTimeTimersQueue.poll();
      triggerTarget.onProcessingTime(timer);
    }

    if (timer != null && nextTimer == null) {
      LOG.debug("Active next timer: {}.", timer);
      nextTimer = initProcessingTimeTimer(timer.getTime(), this::onProcessingTime);
    }
  }

  private ScheduledFuture<?> initProcessingTimeTimer(long time, ProcessingTimeCallback callback) {
    long delay = Math.max(time - getCurrentProcessingTime(), 0) + 1;

    try {
      return timeTriggerService.schedule(
          new OnProcessingTimeTask(callback, time, 0), delay, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("Exception when initiating processing-time timer.", e);
      throw e;
    }
  }

  public long getCurrentProcessingTime() {
    return System.currentTimeMillis();
  }

  public void persist() {
    try {
      priorityQueueState.update(processingTimeTimersQueue);
    } catch (Exception e) {
      LOG.error("Failed to save queue into state.", e);
    }
  }

  private static final class OnProcessingTimeTask implements Runnable {

    private final ProcessingTimeCallback callback;

    private long nextTime;
    private final long interval;

    OnProcessingTimeTask(ProcessingTimeCallback callback, long time, long interval) {
      this.callback = callback;
      this.nextTime = time;
      this.interval = interval;
    }

    @Override
    public void run() {
      try {
        callback.onProcessingTime(nextTime);
      } catch (Exception e) {
        LOG.error("Error in on-processing-time-task.", e);
      }
      nextTime += interval;
    }
  }
}
