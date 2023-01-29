package io.ray.streaming.api.window;

import io.ray.state.api.ValueState;
import io.ray.state.api.ValueStateDescriptor;
import io.ray.state.manager.StateManager;
import io.ray.streaming.api.window.timer.Timer;
import io.ray.streaming.api.window.timer.TimerService;
import io.ray.streaming.api.window.trigger.Triggerable;
import io.ray.streaming.common.metric.local.LocalMetricGroup;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimerServiceTest {

  @Test
  public void testBasicApi() throws Exception {
    String key = "test";
    StateManager stateManager =
        new StateManager("testBasicApi", "testBasicApi", new HashMap<>(), new LocalMetricGroup());
    ValueState<PriorityQueue> queueState =
        stateManager.getNonKeyedValueState(
            ValueStateDescriptor.build("testNoneKeyedValue", PriorityQueue.class));

    Map<Long, Integer> result = new HashMap<>();
    TimerService<String, TimeWindow> timerService =
        new TimerService(new TestOperator(result), queueState);
    Assert.assertEquals(timerService.getCurrentProcessingTime(), System.currentTimeMillis());

    long now = System.currentTimeMillis();
    TimeWindow timeWindow = new TimeWindow(now, now + 500);

    timerService.startTimeService();
    timerService.registerProcessingTimeTimer(key, timeWindow);
    Assert.assertEquals(result.size(), 0);

    TimeUnit.MILLISECONDS.sleep(600);
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.containsKey(now + 500));
    Assert.assertEquals((int) result.get(now + 500), 1);
    TimeUnit.MILLISECONDS.sleep(600);
    Assert.assertEquals(result.size(), 1);

    now = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      timeWindow = new TimeWindow(now + i * 50, now + i * 100);
      timerService.registerProcessingTimeTimer(key, timeWindow);
    }
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(result.size(), 4);
    Assert.assertTrue(result.containsKey(now));
    Assert.assertEquals((int) result.get(now), 2);
    Assert.assertTrue(result.containsKey(now + 100));
    Assert.assertEquals((int) result.get(now + 100), 3);
    Assert.assertTrue(result.containsKey(now + 2 * 100));
    Assert.assertEquals((int) result.get(now + 2 * 100), 4);

    now = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      timeWindow = new TimeWindow(now + i * 50, now + i * 100);
      timerService.registerProcessingTimeTimer(key, timeWindow);
      if (i == 1) {
        timerService.unregisterProcessingTimeTimer(key, timeWindow);
      }
    }
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(result.size(), 6);
    Assert.assertTrue(result.containsKey(now + 2 * 100));
    Assert.assertEquals((int) result.get(now + 2 * 100), 6);

    timerService.stopTimeService();
  }

  static class TestOperator implements Triggerable<String, TimeWindow> {

    private final Map<Long, Integer> result;
    private int count = 0;

    public TestOperator(Map<Long, Integer> result) {
      this.result = result;
    }

    @Override
    public void onProcessingTime(Timer<String, TimeWindow> timer) throws Exception {
      count++;
      result.put(timer.getTime(), count);
    }

    @Override
    public void onEventTime(Timer<String, TimeWindow> timer) throws Exception {}
  }
}
