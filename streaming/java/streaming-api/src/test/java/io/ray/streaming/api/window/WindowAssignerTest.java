package io.ray.streaming.api.window;

import io.ray.streaming.api.window.assigner.ProcessingTimeSessionWindows;
import io.ray.streaming.api.window.assigner.ProcessingTimeTumblingWindows;
import io.ray.streaming.common.utils.Time;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowAssignerTest {

  @Test
  public void testProcessingTimeTumblingWindows() {
    String elementValue = "test";
    long time = 1000;
    ProcessingTimeTumblingWindows<String, String> assigner =
        ProcessingTimeTumblingWindows.of(Time.of(1000, TimeUnit.MILLISECONDS));

    long now = System.currentTimeMillis();
    Collection<TimeWindow> windows = assigner.assignWindows(elementValue, time);
    Assert.assertEquals(windows.size(), 1);
    TimeWindow window = windows.iterator().next();
    Assert.assertEquals(window.maxTime() - window.minTime(), 1000);
    Assert.assertTrue(window.minTime() >= (now - 1000));

    assigner =
        ProcessingTimeTumblingWindows.of(
            Time.of(1000, TimeUnit.MILLISECONDS), Time.of(2000, TimeUnit.MILLISECONDS));
    now = System.currentTimeMillis();
    windows = assigner.assignWindows(elementValue, time);
    Assert.assertEquals(windows.size(), 1);
    window = windows.iterator().next();
    Assert.assertEquals(window.maxTime() - window.minTime(), 1000);
    Assert.assertTrue(window.minTime() >= (now - 1000));

    now = System.currentTimeMillis();
    assigner = ProcessingTimeTumblingWindows.of(Time.of(1000, TimeUnit.MILLISECONDS), now + 5000);
    windows = assigner.assignWindows(elementValue, time);
    Assert.assertEquals(windows.size(), 1);
    window = windows.iterator().next();
    Assert.assertEquals(window.maxTime() - window.minTime(), 1000);
    Assert.assertTrue(window.minTime() >= (now + 4000));

    now = System.currentTimeMillis();
    assigner = ProcessingTimeTumblingWindows.of(Time.of(1000, TimeUnit.MILLISECONDS), now - 4000);
    windows = assigner.assignWindows(elementValue, time);
    Assert.assertEquals(windows.size(), 1);
    window = windows.iterator().next();
    Assert.assertEquals(window.maxTime() - window.minTime(), 1000);
    Assert.assertTrue(window.minTime() >= now);
  }

  @Test
  public void testProcessingTimeSessionWindows() {
    String elementValue = "test";
    ProcessingTimeSessionWindows<String, String> assigner =
        ProcessingTimeSessionWindows.of(Time.of(1000, TimeUnit.MILLISECONDS));

    long now = System.currentTimeMillis();
    Collection<TimeWindow> windows = assigner.assignWindows(elementValue, now);
    Assert.assertEquals(windows.size(), 1);
    TimeWindow window = windows.iterator().next();
    Assert.assertEquals(window.minTime(), now);
    Assert.assertEquals(window.maxTime(), now + 1000);
  }
}
