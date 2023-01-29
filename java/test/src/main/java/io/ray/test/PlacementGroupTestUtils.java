package io.ray.test;

import io.ray.api.PlacementGroups;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.Bundle;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.List;

/** A utils class for placement group test. */
public class PlacementGroupTestUtils {

  public static PlacementGroup createNameSpecifiedSimpleGroup(
      String resourceName,
      int bundleSize,
      PlacementStrategy strategy,
      Double resourceSize,
      String groupName) {
    List<Bundle> bundles = new ArrayList<>();

    for (int i = 0; i < bundleSize; i++) {
      Bundle bundle =
          new Bundle.Builder().setResource(resourceName, resourceSize).setMemoryMb(50).build();
      bundles.add(bundle);
    }
    PlacementGroupCreationOptions.Builder builder =
        new PlacementGroupCreationOptions.Builder().setBundles(bundles).setStrategy(strategy);
    builder.setName(groupName);

    return PlacementGroups.createPlacementGroup(builder.build());
  }

  public static PlacementGroup createSpecifiedSimpleGroup(
      String resourceName,
      int bundleSize,
      PlacementStrategy strategy,
      Double resourceSize,
      boolean isGlobal) {
    return createNameSpecifiedSimpleGroup(
        resourceName, bundleSize, strategy, resourceSize, "unnamed_group");
  }

  public static PlacementGroup createSimpleGroup() {
    return createSpecifiedSimpleGroup("CPU", 1, PlacementStrategy.PACK, 1.0, false);
  }

  public static void createBundleSizeInvalidGroup() {
    createSpecifiedSimpleGroup("CPU", 0, PlacementStrategy.PACK, 1.0, false);
  }

  public static void createBundleResourceInvalidGroup() {
    createSpecifiedSimpleGroup("CPU", 1, PlacementStrategy.PACK, 0.0, false);
  }
}
