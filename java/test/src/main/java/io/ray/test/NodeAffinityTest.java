package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorUnschedulableException;
import io.ray.api.exception.RayTaskUnschedulableException;
import io.ray.api.id.ActorId;
import io.ray.api.id.UniqueId;
import io.ray.api.options.ActorAffinityMatchExpression;
import io.ray.api.options.ActorAffinitySchedulingStrategy;
import io.ray.api.options.NodeAffinitySchedulingStrategy;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

// Test node affinity feature
@Test(groups = "cluster")
public class NodeAffinityTest extends BaseTest {

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

  public void testActorNodeAffinityStrategy() {
    UniqueId currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();
    // 1. test affinity local node
    NodeAffinitySchedulingStrategy schedulingStrategy =
        new NodeAffinitySchedulingStrategy.Builder().addNode(currentNodeId.toString()).build();
    ActorHandle<NodeActor> actor =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    Assert.assertEquals(actor.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);

    // 2. test affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy2 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(UniqueId.randomId().toString())
            .build();
    ActorHandle<NodeActor> actor2 =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy2).remote();
    Assert.assertNotEquals(actor2.getId(), ActorId.NIL);
    Assert.expectThrows(
        RayActorUnschedulableException.class,
        () -> {
          actor2.task(NodeActor::getValue).remote().get(5000);
        });

    // 3. test anti-affinity local node
    NodeAffinitySchedulingStrategy schedulingStrategy3 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(currentNodeId.toString())
            .setAntiAffintiy(true)
            .build();
    ActorHandle<NodeActor> actor3 =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy3).remote();
    Assert.assertNotEquals(actor3.getId(), ActorId.NIL);
    Assert.expectThrows(
        RayActorUnschedulableException.class,
        () -> {
          actor3.task(NodeActor::getValue).remote().get(5000);
        });
    // 4. test anti-affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy4 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(UniqueId.randomId().toString())
            .setAntiAffintiy(true)
            .build();
    ActorHandle<NodeActor> actor4 =
        Ray.actor(NodeActor::new)
            .setMaxRestarts(-1)
            .setSchedulingStrategy(schedulingStrategy4)
            .remote();
    Assert.assertNotEquals(actor4.getId(), ActorId.NIL);
    Assert.assertEquals(
        actor4.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);

    // 5. test soft affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy5 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(UniqueId.randomId().toString())
            .setSoft(true)
            .setAntiAffintiy(false)
            .build();
    ActorHandle<NodeActor> actor5 =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy5).remote();
    Assert.assertNotEquals(actor5.getId(), ActorId.NIL);
    Assert.assertEquals(
        actor5.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);

    // 6. test soft anti-affinity local node
    NodeAffinitySchedulingStrategy schedulingStrategy6 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(currentNodeId.toString())
            .setSoft(true)
            .setAntiAffintiy(true)
            .build();
    ActorHandle<NodeActor> actor6 =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy6).remote();
    Assert.assertNotEquals(actor6.getId(), ActorId.NIL);
    Assert.assertEquals(
        actor6.task(NodeActor::getCurrentNodeId).remote().get(10000), currentNodeId);
  }

  public void testActorNodeAffinityStrategyWithMultiNodes() {
    UniqueId currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();

    // 1. test anti-affinity local node and multi unknow nodes
    NodeAffinitySchedulingStrategy schedulingStrategy2 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(currentNodeId.toString())
            .addNode(UniqueId.randomId().toString())
            .setAntiAffintiy(true)
            .build();
    ActorHandle<NodeActor> actor3 =
        Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy2).remote();
    Assert.assertNotEquals(actor3.getId(), ActorId.NIL);
    Assert.expectThrows(
        RayActorUnschedulableException.class,
        () -> {
          actor3.task(NodeActor::getCurrentNodeId).remote().get(5000);
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          // 2. Don't suppot multi nodes when anti-affinity is false.
          NodeAffinitySchedulingStrategy schedulingStrategy =
              new NodeAffinitySchedulingStrategy.Builder()
                  .addNode(currentNodeId.toString())
                  .addNode(currentNodeId.toString())
                  .addNode(UniqueId.randomId().toString())
                  .addNode(UniqueId.randomId().toString())
                  .build();
          Ray.actor(NodeActor::new).setSchedulingStrategy(schedulingStrategy).remote();
        });
  }

  public void testTaskNodeAffinityStrategy() {
    UniqueId currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();
    // 1. test affinity local node
    NodeAffinitySchedulingStrategy schedulingStrategy1 =
        new NodeAffinitySchedulingStrategy.Builder().addNode(currentNodeId.toString()).build();
    ObjectRef<UniqueId> ref1 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy1)
            .remote();
    Assert.assertEquals(ref1.get(10000), currentNodeId);

    // 2. test affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy2 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(UniqueId.randomId().toString())
            .build();
    ObjectRef<UniqueId> ref2 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy2)
            .remote();
    Assert.expectThrows(
        RayTaskUnschedulableException.class,
        () -> {
          Assert.assertEquals(ref2.get(10000), currentNodeId);
        });

    // 3. test anti-affinity local node
    NodeAffinitySchedulingStrategy schedulingStrategy3 =
        new NodeAffinitySchedulingStrategy.Builder()
            .setAntiAffintiy(true)
            .addNode(currentNodeId.toString())
            .build();
    ObjectRef<UniqueId> ref3 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy3)
            .remote();
    Assert.expectThrows(
        RayTaskUnschedulableException.class,
        () -> {
          Assert.assertEquals(ref3.get(10000), currentNodeId);
        });

    // 4. test anti-affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy4 =
        new NodeAffinitySchedulingStrategy.Builder()
            .setAntiAffintiy(true)
            .addNode(UniqueId.randomId().toString())
            .build();
    ObjectRef<UniqueId> ref4 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy4)
            .remote();
    Assert.assertEquals(ref4.get(10000), currentNodeId);

    // 5. test soft affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy5 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(UniqueId.randomId().toString())
            .setSoft(true)
            .build();
    ObjectRef<UniqueId> ref5 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy5)
            .remote();
    Assert.assertEquals(ref5.get(10000), currentNodeId);

    // 6. test soft anti-affinity local node
    NodeAffinitySchedulingStrategy schedulingStrategy6 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(currentNodeId.toString())
            .setAntiAffintiy(true)
            .setSoft(true)
            .build();
    ObjectRef<UniqueId> ref6 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy6)
            .remote();
    Assert.assertEquals(ref6.get(10000), currentNodeId);
  }

  public void testTaskNodeAffinityStrategyWithMultiNodes() {
    UniqueId currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();

    // 1. test anti-affinity  unknow nodes and local node
    NodeAffinitySchedulingStrategy schedulingStrategy3 =
        new NodeAffinitySchedulingStrategy.Builder()
            .addNode(currentNodeId.toString())
            .addNode(UniqueId.randomId().toString())
            .addNode(UniqueId.randomId().toString())
            .setAntiAffintiy(true)
            .build();
    ObjectRef<UniqueId> ref3 =
        Ray.task(NodeAffinityTest::getCurrentNodeId)
            .setSchedulingStrategy(schedulingStrategy3)
            .remote();
    Assert.expectThrows(
        RayTaskUnschedulableException.class,
        () -> {
          Assert.assertEquals(ref3.get(10000), currentNodeId);
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          // 2. Don't suppot multi nodes when anti-affinity is false.
          NodeAffinitySchedulingStrategy schedulingStrategy1 =
              new NodeAffinitySchedulingStrategy.Builder()
                  .addNode(currentNodeId.toString())
                  .addNode(currentNodeId.toString())
                  .addNode(UniqueId.randomId().toString())
                  .addNode(UniqueId.randomId().toString())
                  .build();
          Ray.task(NodeAffinityTest::getCurrentNodeId)
              .setSchedulingStrategy(schedulingStrategy1)
              .remote();
        });
  }

  public void testNodeAffinityStrategyException() {
    UniqueId currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();
    PlacementGroup placementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "placement_group");
    Assert.assertTrue(placementGroup.wait(60));
    // 1. test affinity unknow node
    NodeAffinitySchedulingStrategy schedulingStrategy =
        new NodeAffinitySchedulingStrategy.Builder().addNode(currentNodeId.toString()).build();

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          ActorHandle<NodeActor> actor =
              Ray.actor(NodeActor::new)
                  .setPlacementGroup(placementGroup, 0)
                  .setSchedulingStrategy(schedulingStrategy)
                  .remote();
          actor.task(NodeActor::getValue).remote().get(5000);
        });
    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          ActorHandle<NodeActor> actor =
              Ray.actor(NodeActor::new)
                  .setSchedulingStrategy(schedulingStrategy)
                  .setPlacementGroup(placementGroup, 0)
                  .remote();
          actor.task(NodeActor::getValue).remote().get(5000);
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          Ray.task(NodeAffinityTest::getCurrentNodeId)
              .setPlacementGroup(placementGroup, 0)
              .setSchedulingStrategy(schedulingStrategy)
              .remote();
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          Ray.task(NodeAffinityTest::getCurrentNodeId)
              .setSchedulingStrategy(schedulingStrategy)
              .setPlacementGroup(placementGroup, 0)
              .remote();
        });

    ActorAffinitySchedulingStrategy actorAffinityStrategy =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("location", false))
            .build();
    ActorHandle<NodeActor> actor =
        Ray.actor(NodeActor::new)
            .setSchedulingStrategy(actorAffinityStrategy)
            .setSchedulingStrategy(schedulingStrategy)
            .remote();
    actor.task(NodeActor::getValue).remote().get(5000);

    NodeAffinitySchedulingStrategy emptySchedulingStrategy =
        new NodeAffinitySchedulingStrategy.Builder().build();

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          Ray.actor(NodeActor::new).setSchedulingStrategy(emptySchedulingStrategy).remote();
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          Ray.task(NodeAffinityTest::longTimeTask)
              .setSchedulingStrategy(emptySchedulingStrategy)
              .remote();
        });
  }
}
