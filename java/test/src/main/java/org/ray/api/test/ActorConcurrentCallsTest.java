package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.ray.runtime.generated.Common;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


@Test(groups = {"directCall"})
public class ActorConcurrentCallsTest extends BaseTest {

  @RayRemote
  public static class ConcurrentAdder {


    public ConcurrentAdder() {

    }

    public int write() {
      try {
        TimeUnit.SECONDS.sleep(30);
      } catch (InterruptedException e) {

      }
      return 0;
    }

    public int read() {
      return 100;
    }

  }

  public void testConcurrentCalls() {
    TestUtils.skipTestIfDirectActorCallDisabled();

    ActorCreationOptions op = new ActorCreationOptions.Builder().setMaxConcurrency(2).createActorCreationOptions();
    RayActor<ConcurrentAdder> actor = Ray.createActor(ConcurrentAdder::new, op);
    RayObject<Integer> writeObj = Ray.call(ConcurrentAdder::write, actor);
    RayObject<Integer> readObj = Ray.call(ConcurrentAdder::read, actor);
    System.out.println("---------" + readObj.get());
  }
}
