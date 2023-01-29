package io.ray.streaming.api.window;

import com.google.common.collect.ImmutableList;
import io.ray.streaming.common.serializer.KryoUtils;
import io.ray.streaming.common.tuple.Tuple2;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.testng.annotations.Test;

public class TimeWindowTest {

  @Test
  public void basicMethodTest() {
    long start0 = System.currentTimeMillis();
    long end0 = start0 + 5000;
    TimeWindow window0 = new TimeWindow(start0, end0);
    long start = start0 - 10000;
    long end = end0 - 7000;
    TimeWindow window1 = new TimeWindow(start, end);
    start = start0 - 10000;
    end = end0 - 4000;
    TimeWindow window2 = new TimeWindow(start, end);
    start = start0 + 1000;
    end = end0 - 2000;
    TimeWindow window3 = new TimeWindow(start, end);
    start = start0 + 1000;
    end = end0 + 2000;
    TimeWindow window4 = new TimeWindow(start, end);
    start = start0 + 10000;
    end = end0 + 20000;
    TimeWindow window5 = new TimeWindow(start, end);

    Assert.assertFalse(window1.intersect(window0));
    Assert.assertTrue(window2.intersect(window0));
    Assert.assertTrue(window3.intersect(window0));
    Assert.assertTrue(window4.intersect(window0));
    Assert.assertFalse(window5.intersect(window0));

    Assert.assertFalse(window1.isSubsetOf(window0));
    Assert.assertFalse(window2.isSubsetOf(window0));
    Assert.assertTrue(window3.isSubsetOf(window0));
    Assert.assertFalse(window4.isSubsetOf(window0));
    Assert.assertFalse(window5.isSubsetOf(window0));

    Assert.assertEquals(window3.cover(window0).minTime(), start0);
    Assert.assertEquals(window3.cover(window0).maxTime(), end0);
    Assert.assertEquals(window5.cover(window0).minTime(), start0);
    Assert.assertEquals(window5.cover(window0).maxTime(), end);
  }

  @Test
  public void testMerging() {
    long start = System.currentTimeMillis();
    long end = start + 3000;
    TimeWindow window0 = new TimeWindow(start, end);
    TimeWindow window1 = new TimeWindow(start + 1000, end + 500);
    TimeWindow window2 = new TimeWindow(start + 2000, end - 100);
    TimeWindow window3 = new TimeWindow(start + 3000, end + 2000);

    Map<TimeWindow, Set<TimeWindow>> result =
        TimeWindow.mergeWindows(ImmutableList.of(window0, window1, window2, window3));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    result.forEach(
        (k, v) -> {
          long startMin = k.minTime();
          long endMax = k.maxTime();
          v.forEach(
              window -> {
                Assert.assertTrue(window.minTime() >= startMin);
                Assert.assertTrue(window.maxTime() <= endMax);
              });
        });
  }

  @Test
  public void testSerialization() {
    com.esotericsoftware.minlog.Log.TRACE();
    long start = System.currentTimeMillis();
    long end = start + 3000;
    TimeWindow window0 = new TimeWindow(start, end);

    byte[] windowBytes = KryoUtils.writeToByteArray(window0);
    TimeWindow result = KryoUtils.readFromByteArray(windowBytes);
    Assert.assertEquals(window0, result);

    Tuple2<?, ?> tuple2 = Tuple2.of("test", window0);
    byte[] tupleBytes = KryoUtils.writeToByteArray(tuple2);
    Tuple2<?, ?> resultTuple2 = KryoUtils.readFromByteArray(tupleBytes);
    Assert.assertEquals(tuple2, resultTuple2);
  }
}
