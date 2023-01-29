package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class L1FaultToleranceTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(L1FaultToleranceTest.class);

  @BeforeClass
  public void setupJobConfig() {
    System.setProperty("ray.job.enable-l1-fault-tolerance", "true");
    System.setProperty("ray.job.num-java-workers-per-process", "1");
  }

  public static class Counter {

    protected int value = 0;

    private boolean wasCurrentActorRestarted = false;

    public Counter() {
      wasCurrentActorRestarted = Ray.getRuntimeContext().wasCurrentActorRestarted();
    }

    public boolean wasCurrentActorRestarted() {
      return wasCurrentActorRestarted;
    }

    public int increase() {
      value += 1;
      return value;
    }

    public int getPid() {
      return SystemUtil.pid();
    }
  }

  public void testActorRestart() throws InterruptedException, IOException {
    List<ActorHandle<Counter>> actors = new ArrayList<>();
    final int actorCount = 4;
    for (int index = 0; index < actorCount; ++index) {
      ActorHandle<Counter> actor = Ray.actor(Counter::new).setMaxRestarts(1).remote();
      actor.task(Counter::increase).remote().get();
      Assert.assertFalse(actor.task(Counter::wasCurrentActorRestarted).remote().get());
      actors.add(actor);
    }

    // Get pid of actor1.
    int pid = actors.get(0).task(Counter::getPid).remote().get();

    // Kill the actor1 process.
    LOGGER.info("Kill the actor1 process, pid is {}.", pid);
    Runtime.getRuntime().exec("kill -9 " + pid);

    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(2);
    Assert.assertFalse(SystemUtil.isProcessAlive(pid));

    for (ActorHandle<Counter> actor : actors) {
      Assert.assertTrue(actor.task(Counter::wasCurrentActorRestarted).remote().get());
    }
  }

  public void testKillActorButNoRestart() throws InterruptedException, IOException {
    List<ActorHandle<Counter>> actors = new ArrayList<>();
    final int actorCount = 4;
    for (int index = 0; index < actorCount; ++index) {
      ActorHandle<Counter> actor = Ray.actor(Counter::new).setMaxRestarts(1).remote();
      actor.task(Counter::increase).remote().get();
      Assert.assertFalse(actor.task(Counter::wasCurrentActorRestarted).remote().get());
      actors.add(actor);
    }

    // Get pid of actor1.
    int pid = actors.get(0).task(Counter::getPid).remote().get();

    // Kill the actor1, with noRestart=true
    LOGGER.info("Kill the actor1 process, pid is {}.", pid);
    Ray.internal().killActor(actors.get(0), true);

    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(2);
    Assert.assertFalse(SystemUtil.isProcessAlive(pid));

    // Remove the dead actor.
    actors.remove(0);

    // L1 FO shouldn't be triggered.
    for (ActorHandle<Counter> actor : actors) {
      Assert.assertFalse(actor.task(Counter::wasCurrentActorRestarted).remote().get());
    }
  }
}
