package io.ray.streaming.api.window;

import io.ray.streaming.api.window.assigner.ProcessingTimeSessionWindows;
import io.ray.streaming.common.utils.Time;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.Test;

public class WindowMergerTest {

  @Test
  public void testMergeNewWindow() throws Exception {
    WindowMerger<String, TimeWindow> merger =
        new WindowMerger(
            ProcessingTimeSessionWindows.of(Time.of(1000, TimeUnit.MILLISECONDS)), null);

    long start = System.currentTimeMillis();
    long end = start + 1000;
    TimeWindow window0 = new TimeWindow(start, end);
    TimeWindow mergedWindow =
        merger.mergeNewWindow(
            window0, (mergedResult, mergedStateResult, eachMergedResult, mergedWindows) -> {});
    Assert.assertEquals(window0, mergedWindow);

    TimeWindow window1 = new TimeWindow(start + 100, end + 200);
    mergedWindow =
        merger.mergeNewWindow(
            window1, (mergedResult, mergedStateResult, eachMergedResult, mergedWindows) -> {});
    Assert.assertNotEquals(window0, mergedWindow);
    Assert.assertNotEquals(window1, mergedWindow);
    Assert.assertEquals(merger.getActualStateWindow(mergedWindow), window0);
    Assert.assertNotEquals(merger.getActualStateWindow(mergedWindow), window1);
    Assert.assertNotEquals(merger.getActualStateWindow(mergedWindow), mergedWindow);
    TimeWindow window01 = window0.cover(window1);
    Assert.assertEquals(window01.minTime(), mergedWindow.minTime());
    Assert.assertEquals(window01.maxTime(), mergedWindow.maxTime());

    TimeWindow window2 = new TimeWindow(start + 300, end + 300);
    mergedWindow =
        merger.mergeNewWindow(
            window2, (mergedResult, mergedStateResult, eachMergedResult, mergedWindows) -> {});
    Assert.assertNotEquals(window0, mergedWindow);
    Assert.assertNotEquals(window2, mergedWindow);
    Assert.assertEquals(merger.getActualStateWindow(mergedWindow), window0);
    Assert.assertNotEquals(merger.getActualStateWindow(mergedWindow), window2);
    Assert.assertNotEquals(merger.getActualStateWindow(mergedWindow), mergedWindow);
    TimeWindow window012 = window0.cover(window1).cover(window2);
    Assert.assertEquals(window012.minTime(), mergedWindow.minTime());
    Assert.assertEquals(window012.maxTime(), mergedWindow.maxTime());
  }
}
