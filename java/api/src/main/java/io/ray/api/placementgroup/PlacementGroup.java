package io.ray.api.placementgroup;

import io.ray.api.id.PlacementGroupId;
import java.util.List;
import java.util.Map;

/**
 * A placement group is used to place interdependent actors according to a specific strategy {@link
 * PlacementStrategy}. When a placement group is created, the corresponding actor slots and
 * resources are preallocated. A placement group consists of one or more bundles plus a specific
 * placement strategy.
 */
public interface PlacementGroup {

  /**
   * Get the id of current placement group.
   *
   * @return Id of current placement group.
   */
  PlacementGroupId getId();

  /**
   * Get the name of current placement group.
   *
   * @return Name of current placement group.
   */
  String getName();

  /**
   * Get all bundles which key is resource name and value is resource value.
   *
   * @return All bundles of current placement group.
   */
  List<Map<String, Double>> getBundles();

  /**
   * Get the strategy of current placement group.
   *
   * @return Strategy of current placement group.
   */
  PlacementStrategy getStrategy();

  /**
   * Get the state of current placement group.
   *
   * @return Creation state of current placement group.
   */
  PlacementGroupState getState();

  /**
   * Wait for the placement group to be ready within the specified time.
   *
   * @param timeoutSeconds Timeout in seconds.
   * @return True if the placement group is created. False otherwise.
   */
  boolean wait(int timeoutSeconds);

  /**
   * Add new bundles to this placement group. Note that the new bundles will append to the end of
   * the existing bundles.
   *
   * @param bundles New bundles that will be added to this placement group.
   */
  void addBundles(List<Bundle> bundles);

  /**
   * Remove bundles from this placement group. Existing actors and tasks in these bundles will be
   * killed. And future attempts to use these bundles will cause exceptions.
   *
   * @param bundleIndexes Indexes of the bundles that will be removed from this placement group.
   */
  void removeBundles(List<Integer> bundleIndexes);
}
