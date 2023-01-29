package io.ray.api.call;

import io.ray.api.Ray;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.ActorLifetime;
import io.ray.api.options.SchedulingStrategy;
import io.ray.api.placementgroup.PlacementGroup;
import java.util.Map;

/**
 * Base helper to create actor.
 *
 * @param <T> The type of the concrete actor creator
 */
public class BaseActorCreator<T extends BaseActorCreator> {
  protected ActorCreationOptions.Builder builder = new ActorCreationOptions.Builder();
  private ActorCreationOptions creationOptions;

  /**
   * Set the actor name of a named actor. This named actor is only accessible from this job by this
   * name via {@link Ray#getActor(java.lang.String)}. If you want create a named actor that is
   * accessible from all jobs, use {@link BaseActorCreator#setGlobalName(java.lang.String)} instead.
   *
   * @param name The name of the named actor.
   * @return self
   * @see io.ray.api.options.ActorCreationOptions.Builder#setName(String)
   */
  public T setName(String name) {
    builder.setName(name);
    return self();
  }

  public T setLifetime(ActorLifetime lifetime) {
    builder.setLifetime(lifetime);
    return self();
  }

  /**
   * Set the memory resource requirement to reserve for the lifetime of this actor. It will assign a
   * sole worker process for this actor if this method is called. This method can be called multiple
   * times. If the same resource is set multiple times, the latest quantity will be used.
   *
   * @param memoryMb memory size in mb.
   * @return self
   * @see ActorCreationOptions.Builder#setMemoryMb(long)
   */
  public T setMemoryMb(long memoryMb) {
    builder.setMemoryMb(memoryMb);
    return self();
  }

  /**
   * Set a custom resource requirement to reserve for the lifetime of this actor. This method can be
   * called multiple times. If the same resource is set multiple times, the latest quantity will be
   * used.
   *
   * @param resourceName resource name
   * @param resourceQuantity resource quantity
   * @return self
   * @see ActorCreationOptions.Builder#setResource(java.lang.String, java.lang.Double)
   */
  public T setResource(String resourceName, Double resourceQuantity) {
    builder.setResource(resourceName, resourceQuantity);
    return self();
  }

  /**
   * Set custom resource requirements to reserve for the lifetime of this actor. This method can be
   * called multiple times. If the same resource is set multiple times, the latest quantity will be
   * used.
   *
   * @param resources requirements for multiple resources.
   * @return self
   * @see BaseActorCreator#setResources(java.util.Map)
   */
  public T setResources(Map<String, Double> resources) {
    builder.setResources(resources);
    return self();
  }

  /**
   * This specifies the maximum number of times that the actor should be restarted when it dies
   * unexpectedly. The minimum valid value is 0 (default), which indicates that the actor doesn't
   * need to be restarted. A value of -1 indicates that an actor should be restarted indefinitely.
   *
   * @param maxRestarts max number of actor restarts
   * @return self
   * @see ActorCreationOptions.Builder#setMaxRestarts(int)
   */
  public T setMaxRestarts(int maxRestarts) {
    builder.setMaxRestarts(maxRestarts);
    return self();
  }

  /**
   * Set the max number of concurrent calls to allow for this actor.
   *
   * <p>The max concurrency defaults to 1 for threaded execution. Note that the execution order is
   * not guaranteed when {@code max_concurrency > 1}.
   *
   * @param maxConcurrency The max number of concurrent calls to allow for this actor.
   * @return self
   * @see ActorCreationOptions.Builder#setMaxConcurrency(int)
   */
  public T setMaxConcurrency(int maxConcurrency) {
    builder.setMaxConcurrency(maxConcurrency);
    return self();
  }

  /**
   * Set the placement group to place this actor in.
   *
   * @param group The placement group of the actor.
   * @param bundleIndex The index of the bundle to place this actor in.
   * @return self
   * @see ActorCreationOptions.Builder#setPlacementGroup(PlacementGroup, int)
   */
  public T setPlacementGroup(PlacementGroup group, int bundleIndex) {
    builder.setPlacementGroup(group, bundleIndex);
    return self();
  }

  /**
   * If enabled, tasks of this actor will fail immediately when the actor is temporarily
   * unavailable. E.g., when there is a network issue, or when the actor is restarting.
   *
   * @param enabled Whether to enable this option.
   * @return self.
   * @see ActorCreationOptions.Builder#setEnableTaskFastFail(boolean)
   */
  public T setEnableTaskFastFail(boolean enabled) {
    builder.setEnableTaskFastFail(enabled);
    return self();
  }

  /**
   * Set several extended key-value information to this actor, and these fields will be stored into
   * GCS eventually.
   *
   * @param extendedProperties customer extended information.
   * @return self
   */
  public T setExtendedProperties(Map<String, String> extendedProperties) {
    builder.setExtendedProperties(extendedProperties);
    return self();
  }

  public T setExtendedProperties(String key, String value) {
    builder.setExtendedProperties(key, value);
    return self();
  }

  /**
   * Set a key-value label.
   *
   * <p>This interface can be called multiple times. The value of the same key will be overwritten
   * by the latest one.
   *
   * @param key the key of label.
   * @param value the value of label.
   * @return self
   */
  public T setLabel(String key, String value) {
    builder.setLabel(key, value);
    return self();
  }

  /**
   * Set batch key-value labels.
   *
   * <p>This interface can be called multiple times. The value of the same key will be overwritten
   * by the latest one.
   *
   * @param labels A map that collects multiple labels.
   * @return self
   */
  public T setLabels(Map<String, String> labels) {
    builder.setLabels(labels);
    return self();
  }

  /**
   * Set scheduling strategy.
   *
   * <p>The placement group and scheduling strategy can't be set at the same time.
   *
   * @param schedulingStrategy a specific scheduling strategy.
   * @return self
   */
  public T setSchedulingStrategy(SchedulingStrategy schedulingStrategy) {
    builder.setSchedulingStrategy(schedulingStrategy);
    return self();
  }

  /**
   * Set actor creation options.
   *
   * <p>Other setter options won't take effect when is method is called.
   */
  @Deprecated
  public T setOptions(ActorCreationOptions options) {
    creationOptions = options;
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected ActorCreationOptions buildOptions() {
    return creationOptions != null ? creationOptions : builder.build();
  }
}
