package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.id.ActorId;
import io.ray.api.options.ActorAffinityMatchExpression;
import io.ray.api.options.ActorAffinitySchedulingStrategy;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

// Test actor affinity feature
@Test(groups = "cluster")
public class ActorAffinityTest extends BaseTest {

  private static class Counter {

    private int value;

    // Constructor
    public Counter(int initValue) {
      this.value = initValue;
    }

    // return value
    public int getValue() {
      return value;
    }
  }

  // test basic api
  public void testActorWithActorAffinityStrategy() {
    Map<String, String> labels =
        new HashMap<String, String>() {
          {
            put("version", "1.0");
            put("region", "china");
          }
        };
    ActorHandle<Counter> actor =
        Ray.actor(Counter::new, 1)
            .setMemoryMb(50)
            .setResource("CPU", 1.0)
            .setLabel("location", "dc_1")
            .setLabels(labels)
            .remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));

    // scheduling into node of actor which the value of label key "location" is in {"dc-1", "dc-2"}
    List<String> locationValues = new ArrayList<>();
    locationValues.add("dc_1");
    locationValues.add("dc_2");
    ActorAffinitySchedulingStrategy schedulingStrategy =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("location", locationValues, false))
            .build();
    ActorHandle<Counter> actor2 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategy).remote();
    Assert.assertEquals(actor2.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));

    // scheduling into node of actor which the value of label key "region" is not in {"USA"}
    List<String> regionValues = new ArrayList<>();
    regionValues.add("USA");
    ActorAffinitySchedulingStrategy schedulingStrategyNotIn =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.notIn("region", regionValues, false))
            .build();
    ActorHandle<Counter> actor3 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategyNotIn).remote();
    Assert.assertEquals(actor3.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));

    // scheduling into node of actor which exists the label key "version"
    ActorAffinitySchedulingStrategy schedulingStrategyExists =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("version", false))
            .build();
    ActorHandle<Counter> actor4 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategyExists).remote();
    Assert.assertEquals(actor4.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));

    // scheduling into node of actor which does not exist the label key "other-key"
    ActorAffinitySchedulingStrategy schedulingStrategyDoesNotExist =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.doesNotExist("other-key", false))
            .build();
    ActorHandle<Counter> actor5 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategyDoesNotExist).remote();
    Assert.assertEquals(actor5.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));
  }

  // Test multi-expression scenarios
  public void testActorAffinityManyExpression() {
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This actor affinity feature don't support raylet schedule mode.");
    }
    ActorHandle<Counter> actor =
        Ray.actor(Counter::new, 1)
            .setMemoryMb(50)
            .setResource("CPU", 1.0)
            .setLabel("location", "dc_1")
            .remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));

    // scheduling into node of actor which the value of label key "location" is in {"dc-1", "dc-2"}
    List<String> locationValues = new ArrayList<>();
    locationValues.add("dc_1");
    locationValues.add("dc_2");
    ActorAffinitySchedulingStrategy schedulingStrategy =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("location", locationValues, false))
            .addExpression(ActorAffinityMatchExpression.exists("error_key", false))
            .build();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          ActorHandle<Counter> actor2 =
              Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategy).remote();
          actor2.task(Counter::getValue).remote().get(3000);
        });

    ActorAffinitySchedulingStrategy schedulingStrategySoft =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("location", locationValues, false))
            .addExpression(ActorAffinityMatchExpression.exists("error_key", true))
            .build();
    ActorHandle<Counter> actor3 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategySoft).remote();
    Assert.assertEquals(actor3.task(Counter::getValue).remote().get(2000), Integer.valueOf(1));
  }

  // Test scenarios of strict and soft affinity
  public void testActorAffinityStrictAndSoft() {
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This actor affinity feature don't support raylet schedule mode.");
    }
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1).setLabel("location", "dc_1").remote();
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(10000), Integer.valueOf(1));

    List<String> locationValues = new ArrayList<>();
    locationValues.add("dc_1");
    ActorAffinitySchedulingStrategy schedulingStrategy =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.notIn("location", locationValues, false))
            .build();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          ActorHandle<Counter> actor2 =
              Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategy).remote();
          actor2.task(Counter::getValue).remote().get(3000);
        });

    List<String> locationValues2 = new ArrayList<>();
    locationValues.add("dc_2");
    ActorAffinitySchedulingStrategy schedulingStrategy2 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("location", locationValues2, false))
            .build();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          ActorHandle<Counter> actor3 =
              Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategy2).remote();
          actor3.task(Counter::getValue).remote().get(3000);
        });

    ActorAffinitySchedulingStrategy schedulingStrategy3 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("other_label_key", false))
            .build();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          ActorHandle<Counter> actor3 =
              Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategy3).remote();
          actor3.task(Counter::getValue).remote().get(3000);
        });

    ActorAffinitySchedulingStrategy schedulingStrategy4 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.doesNotExist("location", false))
            .build();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          ActorHandle<Counter> actor4 =
              Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategy4).remote();
          actor4.task(Counter::getValue).remote().get(3000);
        });

    // test soft
    ActorAffinitySchedulingStrategy schedulingStrategySoft1 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.notIn("location", locationValues, true))
            .build();
    ActorHandle<Counter> softActor1 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategySoft1).remote();
    Assert.assertEquals(softActor1.task(Counter::getValue).remote().get(2000), Integer.valueOf(1));

    ActorAffinitySchedulingStrategy schedulingStrategySoft2 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("location", locationValues2, true))
            .build();
    ActorHandle<Counter> softActor2 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategySoft2).remote();
    Assert.assertEquals(softActor2.task(Counter::getValue).remote().get(2000), Integer.valueOf(1));

    ActorAffinitySchedulingStrategy schedulingStrategySoft3 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("other_label_key", true))
            .build();
    ActorHandle<Counter> softActor3 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategySoft3).remote();
    Assert.assertEquals(softActor3.task(Counter::getValue).remote().get(2000), Integer.valueOf(1));

    ActorAffinitySchedulingStrategy schedulingStrategySoft4 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.doesNotExist("location", true))
            .build();
    ActorHandle<Counter> softActor4 =
        Ray.actor(Counter::new, 1).setSchedulingStrategy(schedulingStrategySoft4).remote();
    Assert.assertEquals(softActor4.task(Counter::getValue).remote().get(2000), Integer.valueOf(1));
  }

  // Test exception scenarios
  public void testActorAffinityStrategyException() {
    PlacementGroup placementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "placement_group");
    Assert.assertTrue(placementGroup.wait(60));

    // scheduling into node of actor which exists the label key "version"
    ActorAffinitySchedulingStrategy schedulingStrategyExists =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.doesNotExist("version", false))
            .build();
    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          ActorHandle<Counter> actor1 =
              Ray.actor(Counter::new, 1)
                  .setPlacementGroup(placementGroup, 1)
                  .setSchedulingStrategy(schedulingStrategyExists)
                  .remote();
          actor1.task(Counter::getValue).remote().get(3000);
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          ActorHandle<Counter> actor2 =
              Ray.actor(Counter::new, 1)
                  .setSchedulingStrategy(schedulingStrategyExists)
                  .setPlacementGroup(placementGroup, 1)
                  .remote();
          actor2.task(Counter::getValue).remote().get(3000);
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          ActorHandle<Counter> actor3 =
              Ray.actor(Counter::new, 1).setLabel("actor_id", "1").remote();
          actor3.task(Counter::getValue).remote().get(3000);
        });

    Assert.expectThrows(
        IllegalArgumentException.class,
        () -> {
          Ray.actor(Counter::new, 1)
              .setSchedulingStrategy(new ActorAffinitySchedulingStrategy.Builder().build())
              .remote();
        });
  }
}
