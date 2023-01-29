package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.ActorLifetime;
import io.ray.runtime.exception.RayActorException;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class AdvancedFailureTest2 extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.job.num-java-workers-per-process", "1");
  }

  public static class BadActor {

    public BadActor() {
      if (!Ray.getRuntimeContext().wasCurrentActorRestarted()) {
        // This is the first time to create this actor, so fail it.
        throw new RuntimeException("Oooooops, this actor was dead when creation.");
      }
    }

    public String ping() {
      return "pong";
    }
  }

  public void testActorShouldNotFailoverWhenCreationTaskFailed() {
    ActorHandle<BadActor> actor =
        Ray.actor(BadActor::new)
            .setName("bad_actor")
            .setLifetime(ActorLifetime.DETACHED)
            .setMaxRestarts(ActorCreationOptions.INFINITE_RESTART)
            .remote();
    Supplier<Boolean> actorWasRestarted =
        () -> {
          try {
            actor.task(BadActor::ping).remote().get();
            return true;
          } catch (RayActorException e) {
            return false;
          }
        };

    // TODO(qwang): This timeout should be reduced, but right now, we should workaround because it
    // takes too long time to create a new worker.
    Assert.assertFalse(TestUtils.waitForCondition(actorWasRestarted, 30 * 1000));
  }
}
