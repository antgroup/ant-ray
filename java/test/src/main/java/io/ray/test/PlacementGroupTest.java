package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.exception.RayException;
import io.ray.api.id.ActorId;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.Bundle;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * TODO: Currently, Java doesn't support multi-node tests so we can't test all strategy temporarily.
 */
public class PlacementGroupTest extends BaseTest {

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }

    public static String ping() {
      return "pong";
    }
  }

  // This test just creates a placement group with one bundle.
  // It's not comprehensive to test all placement group test cases.
  @Test
  public void testCreateAndCallActor() {
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();
    Assert.assertTrue(placementGroup.wait(60));
    Assert.assertEquals(placementGroup.getName(), "unnamed_group");

    // Test creating an actor from a constructor.
    ActorHandle<Counter> actor =
        Ray.actor(Counter::new, 1)
            .setMemoryMb(50)
            .setResource("CPU", 1.0)
            .setPlacementGroup(placementGroup, 0)
            .remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);

    // Test calling an actor.
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(), Integer.valueOf(1));
  }

  @Test(groups = {"cluster"})
  public void testGetPlacementGroup() {
    PlacementGroup firstPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "first_placement_group");

    PlacementGroup secondPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "second_placement_group");
    Assert.assertTrue(firstPlacementGroup.wait(60));
    Assert.assertTrue(secondPlacementGroup.wait(60));

    PlacementGroup firstPlacementGroupRes =
        PlacementGroups.getPlacementGroup((firstPlacementGroup).getId());
    PlacementGroup secondPlacementGroupRes =
        PlacementGroups.getPlacementGroup((secondPlacementGroup).getId());

    Assert.assertNotNull(firstPlacementGroupRes);
    Assert.assertNotNull(secondPlacementGroupRes);

    Assert.assertEquals(firstPlacementGroup.getId(), firstPlacementGroupRes.getId());
    Assert.assertEquals(firstPlacementGroupRes.getBundles().size(), 1);
    Assert.assertEquals(firstPlacementGroupRes.getStrategy(), PlacementStrategy.PACK);

    List<PlacementGroup> allPlacementGroup = PlacementGroups.getAllPlacementGroups();
    Assert.assertEquals(allPlacementGroup.size(), 2);

    PlacementGroup placementGroupRes = allPlacementGroup.get(0);
    Assert.assertNotNull(placementGroupRes.getId());
    PlacementGroup expectPlacementGroup =
        placementGroupRes.getId().equals(firstPlacementGroup.getId())
            ? firstPlacementGroup
            : secondPlacementGroup;

    Assert.assertEquals(
        placementGroupRes.getBundles().size(), expectPlacementGroup.getBundles().size());
    Assert.assertEquals(placementGroupRes.getStrategy(), expectPlacementGroup.getStrategy());
  }

  @Test(groups = {"cluster"})
  public void testRemovePlacementGroup() {
    PlacementGroup firstPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "first_placement_group");

    PlacementGroup secondPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "second_placement_group");
    Assert.assertTrue(firstPlacementGroup.wait(60));
    Assert.assertTrue(secondPlacementGroup.wait(60));

    List<PlacementGroup> allPlacementGroup = PlacementGroups.getAllPlacementGroups();
    Assert.assertEquals(allPlacementGroup.size(), 2);

    PlacementGroups.removePlacementGroup(secondPlacementGroup.getId());

    PlacementGroup removedPlacementGroup =
        PlacementGroups.getPlacementGroup((secondPlacementGroup).getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);

    // Wait for placement group after it is removed.
    int exceptionCount = 0;
    try {
      removedPlacementGroup.wait(10);
    } catch (RayException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 1);
  }

  @Test(groups = {"cluster"})
  public void testCheckBundleIndexWhenCreatingActor() {
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();
    Assert.assertTrue(placementGroup.wait(60));

    int exceptionCount = 0;
    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, 1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 1);

    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, -1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 2);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testBundleSizeValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleSizeInvalidGroup();
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testBundleResourceValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleResourceInvalidGroup();
  }

  @Test(groups = {"cluster"})
  public void testNamedPlacementGroup() {
    // Test Non-Global placement group.
    String pgName = "named_placement_group";
    PlacementGroup firstPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, pgName);
    Assert.assertTrue(firstPlacementGroup.wait(60));
    // Make sure we can get it by name successfully.
    PlacementGroup placementGroup = PlacementGroups.getPlacementGroup(pgName);
    Assert.assertNotNull(placementGroup);
    Assert.assertEquals(placementGroup.getBundles().size(), 1);
  }

  @Test(groups = {"cluster"})
  public void testCreatePlacementGroupWithSameName() {
    String pgName = "named_placement_group";
    PlacementGroup firstPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, pgName);
    Assert.assertTrue(firstPlacementGroup.wait(60));
    int exceptionCount = 0;
    try {
      PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
          "CPU", 1, PlacementStrategy.PACK, 1.0, pgName);
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 1);
  }

  @Test(groups = {"cluster"})
  public void testPlacementGroupForNormalTask() {
    // Create a placement group with non-exist resources.
    String pgName = "named_placement_group";
    PlacementGroup nonExistPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "non-exist-resource", 1, PlacementStrategy.PACK, 1.0, pgName);

    // Make sure its creation will failed.
    Assert.assertFalse(nonExistPlacementGroup.wait(5));

    // Submit a normal task that required a non-exist placement group resources and make sure its
    // scheduling will timeout.
    ObjectRef<String> obj =
        Ray.task(Counter::ping)
            .setPlacementGroup(nonExistPlacementGroup, 0)
            .setMemoryMb(50)
            .setResource("CPU", 1.0)
            .remote();

    List<ObjectRef<String>> waitList = ImmutableList.of(obj);
    WaitResult<String> waitResult = Ray.wait(waitList, 1, 5 * 1000);
    Assert.assertEquals(1, waitResult.getUnready().size());

    // Create a placement group and make sure its creation will successful.
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();
    Assert.assertTrue(placementGroup.wait(60));

    // Submit a normal task that required a exist placement group resources and make sure its
    // scheduling will successful.
    Assert.assertEquals(
        Ray.task(Counter::ping)
            .setPlacementGroup(placementGroup, 0)
            .setMemoryMb(50)
            .setResource("CPU", 1.0)
            .remote()
            .get(),
        "pong");

    // Make sure it will not affect the previous normal task.
    Assert.assertEquals(Ray.task(Counter::ping).remote().get(), "pong");
  }

  @Test(groups = {"cluster"})
  public void testAddAndRemoveBundles() {
    // NOTE: We have already tested most cases in python API,
    // so, we just need to test the basic API here.
    // Create a infeasible placement group firstly.
    Bundle bundle =
        new Bundle.Builder().setResource("Non-existent-resource", 1.0).setMemoryMb(50).build();
    List<Bundle> bundles = ImmutableList.of(bundle);
    PlacementGroupCreationOptions options =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.SPREAD)
            .build();

    PlacementGroup infeasiblePlacementGroup = PlacementGroups.createPlacementGroup(options);
    Assert.assertFalse(infeasiblePlacementGroup.wait(4));

    // Remove this infeasible placement group.
    PlacementGroups.removePlacementGroup(infeasiblePlacementGroup.getId());

    PlacementGroup removedPlacementGroup =
        PlacementGroups.getPlacementGroup((infeasiblePlacementGroup).getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);

    // Create a feasible placement group that can be scheduled immediately.
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();
    Assert.assertTrue(placementGroup.wait(5));

    // Add a new bundle for the placement group.
    Bundle newBundle = new Bundle.Builder().setCpu(1.0).setMemoryMb(50).build();
    List<Bundle> newBundles = ImmutableList.of(newBundle);
    placementGroup.addBundles(newBundles);

    // Wait for the add bundles operation done.
    Assert.assertTrue(placementGroup.wait(4));

    // Schedule an actor with the second new bundle.
    ActorHandle<Counter> actor =
        Ray.actor(Counter::new, 1)
            .setMemoryMb(50)
            .setResource("CPU", 1.0)
            .setPlacementGroup(placementGroup, 1)
            .remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(), Integer.valueOf(1));

    // Remove the second bundle.
    placementGroup.removeBundles(ImmutableList.of(1));

    // Wait for the remove bundles operation done.
    Assert.assertTrue(placementGroup.wait(4));

    // Scheduler another actor with the removed bundles
    // and make sure it will throw a runtime exception.
    Assert.expectThrows(
        IllegalArgumentException.class,
        () ->
            Ray.actor(Counter::new, 1)
                .setMemoryMb(50)
                .setResource("CPU", 1.0)
                .setPlacementGroup(placementGroup, 1)
                .remote());
  }
}
