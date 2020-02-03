package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


@Test(groups = {"directCall"})
public class ActorConcurrentCallsTest extends BaseTest {

  @RayRemote
  public static class ConcurrentActor {

    private BlockingQueue<Integer> queue = new LinkedBlockingQueue<>(3);

    public ConcurrentActor() {

    }

    public List<Integer> append(int x) {
      try {
        queue.put(x);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (queue.size() == 3) {
        queue.notifyAll();
      } else {
        try {
          queue.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      List<Integer> li = new ArrayList<>(3);
      li.addAll(queue);
      li.sort(new Comparator<Integer>() {
        public int compare(Integer o1, Integer o2) {
          return o1.compareTo(o2);
        }
      });
      return li;
    }

  }

  public void testConcurrentCalls() {
    TestUtils.skipTestIfDirectActorCallDisabled();

    ActorCreationOptions op = new ActorCreationOptions.Builder().setMaxConcurrency(2).createActorCreationOptions();
    RayActor<ConcurrentActor> actor = Ray.createActor(ConcurrentActor::new, op);
    RayObject<List<Integer>> obj1 = Ray.call(ConcurrentActor::append, actor, 1);
    RayObject<List<Integer>> obj2 = Ray.call(ConcurrentActor::append, actor, 2);
    RayObject<List<Integer>> obj3 = Ray.call(ConcurrentActor::append, actor, 3);

    List<Integer> expectedResult = ImmutableList.of(1, 2, 3);
    Assert.assertEquals(obj1.get(), expectedResult);
    Assert.assertEquals(obj2.get(), expectedResult);
    Assert.assertEquals(obj3.get(), expectedResult);
  }
}
