package io.ray.streaming.operator;

import com.google.common.base.Preconditions;
import io.ray.state.api.KeyMapState;
import io.ray.state.api.KeyMapStateDescriptor;
import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.state.api.ValueState;
import io.ray.state.api.ValueStateDescriptor;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.window.WindowFunction;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.WindowMerger;
import io.ray.streaming.api.window.assigner.MergeableWindowAssigner;
import io.ray.streaming.api.window.assigner.WindowAssigner;
import io.ray.streaming.api.window.timer.Timer;
import io.ray.streaming.api.window.timer.TimerService;
import io.ray.streaming.api.window.trigger.Trigger;
import io.ray.streaming.api.window.trigger.TriggerContext;
import io.ray.streaming.api.window.trigger.TriggerResult;
import io.ray.streaming.api.window.trigger.Triggerable;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic implement for window operator type.
 *
 * <p>For every element in this operator, it belongs to a {@link Window}. The {@link Window} is
 * created by the {@link WindowAssigner}, and for each window it is belongs to a data key. So, 1 key
 * -> N window -> M data.
 *
 * <p>For the whole context, there is a {@link Trigger}, by time or by data. When the {@link Window}
 * is triggered, all the values in the {@link Window} will be processed by the {@link
 * WindowFunction}.
 *
 * @param <K> key type
 * @param <T> input data type
 * @param <R> output data type
 * @param <W> window type
 * @param <F> function type
 */
public abstract class AbstractWindowOperator<
        K, T, R, W extends Window, F extends WindowFunction<K, T, R, W>>
    extends AbstractStreamOperator<F> implements OneInputOperator<T>, Triggerable<K, W> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWindowOperator.class);

  protected WindowAssigner<K, T, W> windowAssigner;
  protected Trigger<K, T, W> trigger;
  protected transient Context context;
  protected transient TimerService<K, W> timerService;

  /**
   * State for window operator.
   *
   * <p>windowValueStates: Store data key, state window and data value mapping. Key: data key,
   * SubKey: state window, Value: input data's value list.
   *
   * <p>mergingState: Store the merged window and state window mapping. Key: merged window, Value:
   * state window.
   *
   * <p>priorityQueueState: Store the timer in order.
   */
  protected transient KeyMapState<K, W, ArrayList> windowValueStates;

  protected transient KeyValueState<W, W> mergingState;
  protected transient ValueState<PriorityBlockingQueue> priorityQueueState;

  protected Class<W> windowClazz;
  protected Class<K> keyClazz;
  protected Class<T> valueClazz;

  public AbstractWindowOperator(
      F function, WindowAssigner<K, T, W> windowAssigner, Trigger<K, T, W> trigger) {
    super(function);
    this.windowAssigner =
        Preconditions.checkNotNull(windowAssigner, "windowAssigner must no be null");
    if (trigger == null) {
      trigger = windowAssigner.getDefaultTrigger();
    }
    this.trigger = Preconditions.checkNotNull(trigger, "trigger must no be null");
  }

  public void registerClazz(Class<W> windowClazz, Class<K> keyClazz, Class<T> valueClazz) {
    this.windowClazz = windowClazz;
    this.keyClazz = keyClazz;
    this.valueClazz = valueClazz;
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    super.open(collectors, runtimeContext);

    // init state
    windowValueStates =
        runtimeContext.getKeyMapState(
            KeyMapStateDescriptor.build(
                getName() + "_windowValueStates", keyClazz, windowClazz, ArrayList.class));
    mergingState =
        runtimeContext.getNonKeyedKeyValueState(
            KeyValueStateDescriptor.build(
                getName() + "_windowMergingStates", windowClazz, windowClazz));
    priorityQueueState =
        runtimeContext.getNonKeyedValueState(
            ValueStateDescriptor.build(
                getName() + "_windowPriorityQueueStates", PriorityBlockingQueue.class));
    priorityQueueState.setCurrentKey(runtimeContext.getTaskIndex());

    // init timer service
    timerService = new TimerService(this, priorityQueueState);
    timerService.startTimeService();

    // init context
    context = new Context(runtimeContext);
    context.setTimerService(timerService);
    trigger.init(context);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    KeyRecord<K, T> keyRecord = (KeyRecord<K, T>) record;
    K key = keyRecord.getKey();
    T recordValue = keyRecord.getValue();
    long recordTime = keyRecord.getTraceTimestamp();

    boolean isEventTime = windowAssigner.isEventTime();
    Collection<W> windows = getWindowsAssignment(recordValue, recordTime);

    if (windowAssigner instanceof MergeableWindowAssigner) {
      // for window need to merge
      WindowMerger<K, W> windowMerger = getWindowMerger();

      for (W window : windows) {
        // adding the new window might result in a merge, in that case the actualWindow
        // is the merged window and we work with that. If we don't merge then
        // actualWindow == window
        W actualWindow =
            windowMerger.mergeNewWindow(
                window,
                (mergedResult, mergedStateResult, eachMergedResult, mergedWindows) -> {
                  if (!isEventTime) {
                    if (eachMergedResult.maxTime() <= timerService.getCurrentProcessingTime()) {
                      return;
                    }
                  }

                  context.setKey(key);
                  context.setWindow(eachMergedResult);
                  context.setMergedWindows(mergedWindows);
                  trigger.onMerge(key, mergedResult, eachMergedResult);

                  for (W mergedWindow : mergedWindows) {
                    context.setWindow(mergedWindow);
                    if (!mergedWindow.equals(mergedStateResult)) {
                      trigger.clear(key, mergedWindow);
                    }
                    unregisterTimer(key, mergedWindow, isEventTime);
                  }
                });

        W stateWindow = windowMerger.getActualStateWindow(actualWindow);
        if (stateWindow == null) {
          throw new IllegalStateException("Can't get the state window by: " + actualWindow);
        }

        // need to make sure to update the merging state in state
        windowMerger.persist();

        processingElementForTimeWindow(stateWindow, key, recordValue, recordTime);

        // register timer for the window
        registerTimer(key, stateWindow, windowAssigner.isEventTime());
      }
    } else {
      // for window no need to merge
      for (W window : windows) {
        processingElementForTimeWindow(window, key, recordValue, recordTime);

        // register timer for the window
        registerTimer(key, window, windowAssigner.isEventTime());
      }
    }
  }

  @Override
  public void onProcessingTime(Timer<K, W> timer) throws Exception {
    LOG.debug("Window triggered on processing time: {}.", timer);
    TriggerResult triggerResult = trigger.onProcessingTime(timer.getTime(), timer.getWindow());

    LOG.debug("Window triggered result: {}.", triggerResult);
    handleWithTriggerResult(triggerResult, timer.getKey(), timer.getWindow());
  }

  @Override
  public void onEventTime(Timer<K, W> timer) throws Exception {
    // TODO
  }

  @Override
  public void close() {
    super.close();
    timerService.stopTimeService();
  }

  /**
   * Process element for time window.
   *
   * @param window window used
   * @param key record's key
   * @param recordValue record's value
   * @param recordTime record's time
   * @throws Exception exception
   */
  protected void processingElementForTimeWindow(W window, K key, T recordValue, long recordTime)
      throws Exception {
    addWindowAndKeyValue(key, window, recordValue);

    context.setKey(key);
    context.setWindow(window);

    LOG.debug("Trigger on element, key: {}, value: {}, window: {}.", key, recordValue, window);
    TriggerResult triggerResult = trigger.onElement(recordValue, recordTime, window);
    handleWithTriggerResult(triggerResult, key, window);
  }

  protected Collection<W> getWindowsAssignment(T element, long time) {
    return windowAssigner.assignWindows(element, time);
  }

  protected void addWindowAndKeyValue(K key, W window, T value) throws Exception {
    if (key == null || window == null) {
      LOG.warn("Invalid key: {} or window: {}.", key, window);
      return;
    }

    if (!windowValueStates.contains(key, window)) {
      addNewWindow(key, window);
    }
    addKeyValue(key, window, value);
  }

  protected synchronized void discardWindow(K key, W window) throws Exception {
    LOG.debug("Discard window: {}, key: {}.", window, key);
    windowValueStates.remove(key, window);
    mergingState.remove(window);
  }

  protected synchronized List<T> getWindowValues(K key, W window) throws Exception {
    return windowValueStates.get(key, window);
  }

  protected synchronized void addKeyValue(K key, W window, T value) throws Exception {
    LOG.debug("Add key({}), value({}) for window: {}.", key, value, window);
    ArrayList<T> valueList = windowValueStates.get(key, window);
    if (valueList == null) {
      valueList = new ArrayList<>();
    }
    valueList.add(value);
    windowValueStates.put(key, window, valueList);
  }

  private synchronized void addNewWindow(K key, W window) throws Exception {
    LOG.debug("Add a new window: {} for key: {}.", window, key);
    windowValueStates.put(key, window, new ArrayList<>());
  }

  protected WindowMerger<K, W> getWindowMerger() throws Exception {
    return new WindowMerger<>((MergeableWindowAssigner<K, T, W>) windowAssigner, mergingState);
  }

  /**
   * Handle with the data in window according to the trigger result.
   *
   * @param triggerResult result of window trigger
   * @param key data key
   * @param window the specified window
   * @throws Exception exception
   */
  protected void handleWithTriggerResult(TriggerResult triggerResult, K key, W window)
      throws Exception {
    if (triggerResult.isFire()) {
      handleFireTrigger(key, window);
    }
    if (triggerResult.isPurge()) {
      handlePurgeTrigger(key, window);
    }
  }

  /**
   * Handle with the data in window according to the purge action.
   *
   * @param key data key
   * @param window the specified window
   * @throws Exception exception
   */
  protected synchronized void handlePurgeTrigger(K key, W window) throws Exception {
    discardWindow(key, window);
    trigger.clear(key, window);
  }

  /**
   * Handle with the data in window according to the fire action.
   *
   * @param key data key
   * @param window the specified window
   * @throws Exception exception
   */
  protected void handleFireTrigger(K key, W window) throws Exception {
    List<T> values = getWindowValues(key, window);
    LOG.debug("Window value before fire: {}.", values);

    R result = function.apply(key, values, window);

    if (result != null) {
      synchronized (this) {
        collect(result);
      }
    }
  }

  /**
   * Register timer for window.
   *
   * @param key the specified key
   * @param window the specified window
   * @param isEventTime is event time type
   */
  protected void registerTimer(K key, W window, boolean isEventTime) {
    if (isEventTime) {
      // TODO
    } else {
      timerService.registerProcessingTimeTimer(key, window, Math.max(0, window.maxTime()));
    }
  }

  /**
   * Unregister timer for window.
   *
   * @param key the specified key
   * @param window the specified window
   * @param isEventTime is event time type
   */
  protected void unregisterTimer(K key, W window, boolean isEventTime) {
    if (isEventTime) {
      // TODO
    } else {
      timerService.unregisterProcessingTimeTimer(key, window, Math.max(0, window.maxTime()));
    }
  }

  @Override
  public void saveCheckpoint(long checkpointId) throws Exception {
    timerService.persist();
    super.saveCheckpoint(checkpointId);
  }

  public class Context extends TriggerContext<K, W> {
    protected K key;
    protected W window;

    protected Collection<W> mergedWindows;

    public Context(RuntimeContext runtimeContext) {
      this(null, null, runtimeContext);
    }

    public Context(K key, W window, RuntimeContext runtimeContext) {
      super(runtimeContext);
      this.key = key;
      this.window = window;
    }

    public K getKey() {
      return key;
    }

    public void setKey(K key) {
      this.key = key;
    }

    public W getWindow() {
      return window;
    }

    public void setWindow(W window) {
      this.window = window;
    }

    public Collection<W> getMergedWindows() {
      return mergedWindows;
    }

    public void setMergedWindows(Collection<W> mergedWindows) {
      this.mergedWindows = mergedWindows;
    }
  }
}
