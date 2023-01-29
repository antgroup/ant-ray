package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorUnschedulableException;
import io.ray.api.exception.RayTaskUnschedulableException;
import io.ray.api.id.UniqueId;
import io.ray.api.options.NodeAffinitySchedulingStrategy;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class NodeAffinityResourceUnavailableTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("num-cpus", "2");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("num-cpus");
  }

  private static class NodeActor {

    // Constructor
    public NodeActor() {}

    // return value
    public int getValue() {
      return 0;
    }

    public UniqueId getCurrentNodeId() {
      return Ray.getRuntimeContext().getCurrentNodeId();
    }
  }

  private static UniqueId getCurrentNodeId() {
    return Ray.getRuntimeContext().getCurrentNodeId();
  }

  private static Boolean longTimeTask() {
    try {
      while (true) {
        Thread.sleep(1000);
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return true;
  }

  public void testResourceUnavailabe() {
    UniqueId currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();

    NodeAffinitySchedulingStrategy schedulingStrategy =
        new NodeAffinitySchedulingStrategy.Builder().addNode(currentNodeId.toString()).build();

    ObjectRef<Boolean> ref1 =
        Ray.task(NodeAffinityResourceUnavailableTest::longTimeTask)
            .setSchedulingStrategy(schedulingStrategy)
            .setResource("CPU", 1.0)
            .remote();

    ActorHandle<NodeActor> actor =
        Ray.actor(NodeActor::new)
            .setSchedulingStrategy(schedulingStrategy)
            .setResource("CPU", 1.0)
            .remote();
    Assert.assertEquals(actor.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);

    Assert.expectThrows(
        RayTaskUnschedulableException.class,
        () -> {
          ObjectRef<UniqueId> ref2 =
              Ray.task(NodeAffinityResourceUnavailableTest::getCurrentNodeId)
                  .setSchedulingStrategy(schedulingStrategy)
                  .setResource("CPU", 1.0)
                  .remote();
          Ray.get(ref2, 5000);
        });

    Assert.expectThrows(
        RayActorUnschedulableException.class,
        () -> {
          ActorHandle<NodeActor> actor2 =
              Ray.actor(NodeActor::new)
                  .setSchedulingStrategy(schedulingStrategy)
                  .setResource("CPU", 1.0)
                  .remote();
          Assert.assertEquals(
              actor2.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);
        });
    // test anti-affinity
    NodeAffinitySchedulingStrategy schedulingStrategy2 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(UniqueId.randomId().toString())
            .setAntiAffintiy(true)
            .build();
    Assert.expectThrows(
        RayTaskUnschedulableException.class,
        () -> {
          ObjectRef<UniqueId> ref3 =
              Ray.task(NodeAffinityResourceUnavailableTest::getCurrentNodeId)
                  .setSchedulingStrategy(schedulingStrategy2)
                  .setResource("CPU", 1.0)
                  .remote();
          Ray.get(ref3, 5000);
        });

    Assert.expectThrows(
        RayActorUnschedulableException.class,
        () -> {
          ActorHandle<NodeActor> actor3 =
              Ray.actor(NodeActor::new)
                  .setSchedulingStrategy(schedulingStrategy2)
                  .setResource("CPU", 1.0)
                  .remote();
          Assert.assertEquals(
              actor3.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);
        });

    ObjectRef<UniqueId> ref3 =
        Ray.task(NodeAffinityResourceUnavailableTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy)
            .remote();
    Assert.assertEquals(Ray.get(ref3, 5000), currentNodeId);

    ActorHandle<NodeActor> actor3 =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy).remote();
    Assert.assertEquals(
        actor3.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);
  }
}
