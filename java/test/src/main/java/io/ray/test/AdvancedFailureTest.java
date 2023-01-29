package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AdvancedFailureTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.task.return_task_exception", "false");
    System.setProperty("ray.job.jvm-options.0", "-Dray.task.return_task_exception=false");
    System.setProperty("ray.job.enable-l1-fault-tolerance", "true");
    System.setProperty("ray.job.num-java-workers-per-process", "3");
  }

  public static class BadActor {

    public int getPid() {
      return SystemUtil.pid();
    }

    public int badMethod() {
      throw new RuntimeException("This is a bad method.");
    }

    public boolean wasActorRestarted() {
      return Ray.getRuntimeContext().wasCurrentActorRestarted();
    }
  }

  @Test(groups = "cluster")
  public void testActorTaskExceptionWithReturnTaskExceptionDisabled() throws InterruptedException {
    ActorHandle<BadActor> actor = Ray.actor(BadActor::new).setMaxRestarts(-1).remote();
    int oldPid = actor.task(BadActor::getPid).remote().get();
    actor.task(BadActor::badMethod).setIgnoreReturn(true).remote();
    // we assume that get another pid.
    boolean foundAnotherPid =
        TestUtils.waitForCondition(
            () -> {
              try {
                int newPid = actor.task(BadActor::getPid).remote().get();
                return oldPid != newPid;
              } catch (Exception e) {
                return false;
              }
            },
            10 * 1000);
    Assert.assertTrue(foundAnotherPid);
    boolean wasStarted = actor.task(BadActor::wasActorRestarted).remote().get();
    Assert.assertTrue(wasStarted);
  }
}
