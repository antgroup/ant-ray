package io.ray.runtime.placementgroup;

import io.ray.api.Ray;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.placementgroup.Bundle;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.List;
import java.util.Map;

/** The default implementation of `PlacementGroup` interface. */
public class PlacementGroupImpl implements PlacementGroup {

  private final PlacementGroupId id;
  private final String name;
  private final List<Map<String, Double>> bundles;
  private final PlacementStrategy strategy;
  private final PlacementGroupState state;

  private PlacementGroupImpl(
      PlacementGroupId id,
      String name,
      List<Map<String, Double>> bundles,
      PlacementStrategy strategy,
      PlacementGroupState state) {
    this.id = id;
    this.name = name;
    this.bundles = bundles;
    this.strategy = strategy;
    this.state = state;
  }

  @Override
  public PlacementGroupId getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public List<Map<String, Double>> getBundles() {
    return bundles;
  }

  @Override
  public PlacementStrategy getStrategy() {
    return strategy;
  }

  @Override
  public PlacementGroupState getState() {
    return state;
  }

  @Override
  public boolean wait(int timeoutSeconds) {
    return Ray.internal().waitPlacementGroupReady(id, timeoutSeconds);
  }

  @Override
  public void addBundles(List<Bundle> bundles) {
    if (this.id == null) {
      throw new IllegalStateException(
          "The placement group must be created successfully before adding bundles.");
    }
    if (bundles == null || bundles.isEmpty()) {
      throw new IllegalArgumentException(
          "Argument `bundles` can't be null or empty "
              + "when adding bundles to a created placement group.");
    }
    boolean bundleResourceValid = bundles.stream().allMatch(Bundle::validate);

    if (!bundleResourceValid) {
      throw new IllegalArgumentException(
          "Each bundle's resources must "
              + "be positive when adding bundles to a placement group.");
    }

    Ray.internal().addBundlesForPlacementGroup(id, bundles);
  }

  @Override
  public void removeBundles(List<Integer> bundleIndexes) {
    if (this.id == null) {
      throw new IllegalStateException(
          "The placement group must has been created successfully before removing bundles.");
    }
    if (bundleIndexes == null || bundleIndexes.isEmpty()) {
      throw new IllegalArgumentException(
          "Argument `bundleIndexes` can't be null or empty "
              + "when removing bundles from a created placement group.");
    }
    Ray.internal().removeBundlesForPlacementGroup(id, bundleIndexes);
  }

  /** A help class for create the placement group. */
  public static class Builder {
    private PlacementGroupId id;
    private String name;
    private List<Map<String, Double>> bundles;
    private PlacementStrategy strategy;
    private PlacementGroupState state;

    /**
     * Set the Id of the placement group.
     *
     * @param id Id of the placement group.
     * @return self.
     */
    public Builder setId(PlacementGroupId id) {
      this.id = id;
      return this;
    }

    /**
     * Set the name of the placement group.
     *
     * @param name Name of the placement group.
     * @return self.
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the bundles of the placement group.
     *
     * @param bundles the bundles of the placement group.
     * @return self.
     */
    public Builder setBundles(List<Map<String, Double>> bundles) {
      this.bundles = bundles;
      return this;
    }

    /**
     * Set the placement strategy of the placement group.
     *
     * @param strategy the placement strategy of the placement group.
     * @return self.
     */
    public Builder setStrategy(PlacementStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    /**
     * Set the placement state of the placement group.
     *
     * @param state the state of the placement group.
     * @return self.
     */
    public Builder setState(PlacementGroupState state) {
      this.state = state;
      return this;
    }

    public PlacementGroupImpl build() {
      return new PlacementGroupImpl(id, name, bundles, strategy, state);
    }
  }
}
