package io.ray.streaming.common.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NoRepeatedPriorityBlockingQueueTest {

  @Test
  public void testBasic() {
    NoRepeatedPriorityBlockingQueue<Integer> queue = new NoRepeatedPriorityBlockingQueue<>();
    Assert.assertTrue(queue.add(1));
    Assert.assertTrue(queue.add(1));
    Assert.assertTrue(queue.add(1));
    Assert.assertTrue(queue.add(2));
    Assert.assertTrue(queue.add(3));
    Assert.assertTrue(queue.add(3));

    Assert.assertEquals(queue.size(), 3);
    Assert.assertEquals((int) queue.peek(), 1);
    Assert.assertEquals((int) queue.poll(), 1);
    Assert.assertEquals((int) queue.poll(), 2);
    Assert.assertEquals((int) queue.poll(), 3);
    Assert.assertEquals(queue.size(), 0);
  }
}
