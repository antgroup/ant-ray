package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class IgnoreReturnTest extends BaseTest {

  public static String echo(ActorHandle<SignalActor> signal) {
    signal.task(SignalActor::sendSignal).remote().get();
    return null;
  }

  public static class Echo {
    public String echo(ActorHandle<SignalActor> signal) {
      signal.task(SignalActor::sendSignal).remote().get();
      return null;
    }
  }

  public void testNormalTask() {
    ActorHandle<SignalActor> signal = SignalActor.create();
    ObjectRef<String> obj = Ray.task(IgnoreReturnTest::echo, signal).setIgnoreReturn(true).remote();
    Assert.assertNull(obj);
    signal.task(SignalActor::waitSignal).remote().get();
  }

  public void testActorTask() {
    ActorHandle<SignalActor> signal = SignalActor.create();
    ActorHandle<Echo> actor = Ray.actor(Echo::new).remote();
    ObjectRef<String> obj = actor.task(Echo::echo, signal).setIgnoreReturn(true).remote();
    Assert.assertNull(obj);
    signal.task(SignalActor::waitSignal).remote().get();
  }

  public static class Counter {

    public int echo(int value) {
      return value;
    }

    public int getPid() {
      return SystemUtil.pid();
    }
  }

  private void invokeAndAssert(ActorHandle<Counter> counter) {
    List<ObjectRef<Integer>> values = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      ObjectRef<Integer> value =
          counter.task(Counter::echo, i).setIgnoreReturn(i % 2 == 0).remote();
      if (value != null) {
        values.add(value);
      }
    }
    Ray.wait(values);
    for (int i = 0; i < values.size(); i++) {
      Assert.assertEquals((int) values.get(i).get(), i * 2 + 1);
    }
  }

  public void testMixedWithFo() throws IOException, InterruptedException {
    ActorHandle<Counter> counter = Ray.actor(Counter::new).setMaxRestarts(3).remote();
    for (int i = 0; i < 3; i++) {
      int pid = counter.task(Counter::getPid).remote().get();
      invokeAndAssert(counter);

      Process p = Runtime.getRuntime().exec("kill -9 " + pid);
      // Wait for the actor to be killed.
      TimeUnit.SECONDS.sleep(1);
    }
    invokeAndAssert(counter);
  }
}
