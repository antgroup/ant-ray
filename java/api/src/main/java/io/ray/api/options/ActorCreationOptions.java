package io.ray.api.options;

import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.placementgroup.PlacementGroup;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The options for creating actor. */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RESTART = 0;
  public static final int INFINITE_RESTART = (int) Math.pow(2, 30);

  public final String name;
  public ActorLifetime lifetime;
  public final int maxRestarts;
  public final List<String> jvmOptions;
  public final int maxConcurrency;
  public final String serializedRuntimeEnv;
  public final PlacementGroup group;
  public final int bundleIndex;
  public final long memoryMb;
  public final boolean enableTaskFastFail;
  public final List<ConcurrencyGroup> concurrencyGroups;
  public final Map<String, String> extendedProperties;
  public final Map<String, String> labels;
  public final SchedulingStrategy schedulingStrategy;
  public final boolean isAsync;

  private ActorCreationOptions(
      String name,
      ActorLifetime lifetime,
      Map<String, Double> resources,
      int maxRestarts,
      List<String> jvmOptions,
      int maxConcurrency,
      String serializedRuntimeEnv,
      PlacementGroup group,
      int bundleIndex,
      long memoryMb,
      List<ConcurrencyGroup> concurrencyGroups,
      boolean enableTaskFastFail,
      Map<String, String> extendedProperties,
      Map<String, String> labels,
      SchedulingStrategy schedulingStrategy,
      boolean isAsync) {
    super(resources);
    this.name = name;
    this.lifetime = lifetime;
    this.maxRestarts = maxRestarts;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
    this.serializedRuntimeEnv = serializedRuntimeEnv;
    this.group = group;
    this.bundleIndex = bundleIndex;
    this.memoryMb = memoryMb;
    this.concurrencyGroups = concurrencyGroups;
    this.enableTaskFastFail = enableTaskFastFail;
    this.extendedProperties = extendedProperties;
    this.labels = labels;
    this.schedulingStrategy = schedulingStrategy;
    this.isAsync = isAsync;
  }

  /** The inner class for building ActorCreationOptions. */
  public static class Builder {
    private String name;
    private ActorLifetime lifetime = null;
    private Map<String, Double> resources = new HashMap<>();
    private int maxRestarts = 0;
    private List<String> jvmOptions = new ArrayList<>();
    private int maxConcurrency = 1;
    private String serializedRuntimeEnv = "{}";
    private PlacementGroup group;
    private int bundleIndex;
    private long memoryMb = 0;
    private boolean enableTaskFastFail = false;
    private boolean isAsync = false;
    private List<ConcurrencyGroup> concurrencyGroups = new ArrayList<>();
    private Map<String, String> extendedProperties = new HashMap<>();
    private Map<String, String> labels = new HashMap<>();
    private SchedulingStrategy schedulingStrategy;

    /**
     * Set the actor name of a named actor. This named actor is accessible in this namespace by this
     * name via {@link Ray#getActor(java.lang.String)} and in other namespaces via {@link
     * Ray#getActor(java.lang.String, java.lang.String)}.
     *
     * @param name The name of the named actor.
     * @return self
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /** Declare the lifetime of this actor. */
    public Builder setLifetime(ActorLifetime lifetime) {
      this.lifetime = lifetime;
      return this;
    }

    /**
     * Set the memory resource requirement to reserve for the lifetime of this actor. It will assign
     * a sole worker process for this actor if this method is called. This method can be called
     * multiple times. If the same resource is set multiple times, the latest quantity will be used.
     *
     * @param memoryMb memory size in mb
     * @return self
     */
    public Builder setMemoryMb(long memoryMb) {
      if (memoryMb <= 0 || memoryMb % 50 != 0) {
        throw new IllegalArgumentException(
            "The value of memoryMb (" + memoryMb + ") must be multiple of 50M.");
      }
      this.memoryMb = memoryMb;
      return this;
    }

    /**
     * Set a custom resource requirement to reserve for the lifetime of this actor. This method can
     * be called multiple times. If the same resource is set multiple times, the latest quantity
     * will be used.
     *
     * @param resourceName resource name
     * @param resourceQuantity resource quantity
     * @return self
     */
    public Builder setResource(String resourceName, Double resourceQuantity) {
      this.resources.put(resourceName, resourceQuantity);
      return this;
    }

    /**
     * Set custom resource requirements to reserve for the lifetime of this actor. This method can
     * be called multiple times. If the same resource is set multiple times, the latest quantity
     * will be used.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    /**
     * This specifies the maximum number of times that the actor should be restarted when it dies
     * unexpectedly. The minimum valid value is 0 (default), which indicates that the actor doesn't
     * need to be restarted. A value of -1 indicates that an actor should be restarted indefinitely.
     *
     * @param maxRestarts max number of actor restarts
     * @return self
     */
    public Builder setMaxRestarts(int maxRestarts) {
      this.maxRestarts = maxRestarts;
      return this;
    }

    /**
     * Set the JVM options for the Java worker that this actor is running in.
     *
     * <p>Note, if this is set, this actor won't share Java worker with other actors or tasks.
     *
     * @param jvmOptions JVM options for the Java worker that this actor is running in.
     * @return self
     */
    public Builder setJvmOptions(List<String> jvmOptions) {
      this.jvmOptions.addAll(jvmOptions);
      return this;
    }

    /**
     * Set the max number of concurrent calls to allow for this actor.
     *
     * <p>The max concurrency defaults to 1 for threaded execution. Note that the execution order is
     * not guaranteed when {@code max_concurrency > 1}.
     *
     * @param maxConcurrency The max number of concurrent calls to allow for this actor.
     * @return self
     */
    public Builder setMaxConcurrency(int maxConcurrency) {
      if (maxConcurrency <= 0) {
        throw new IllegalArgumentException("maxConcurrency must be greater than 0.");
      }
      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public Builder setSerializedRuntimeEnv(String serializedRuntimeEnv) {
      this.serializedRuntimeEnv = serializedRuntimeEnv;
      return this;
    }

    /**
     * Set the placement group to place this actor in.
     *
     * @param group The placement group of the actor.
     * @param bundleIndex The index of the bundle to place this actor in.
     * @return self
     */
    public Builder setPlacementGroup(PlacementGroup group, int bundleIndex) {
      if (this.schedulingStrategy != null) {
        throw new IllegalArgumentException(
            "The placement group and actor affinity strategy can't be both set.");
      }
      this.group = group;
      this.bundleIndex = bundleIndex;
      return this;
    }

    /**
     * If enabled, tasks of this actor will fail immediately when the actor is temporarily
     * unavailable. E.g., when there is a network issue, or when the actor is restarting.
     *
     * @param enabled Whether to enable this option.
     * @return self
     */
    public Builder setEnableTaskFastFail(boolean enabled) {
      this.enableTaskFastFail = enabled;
      return this;
    }

    /**
     * Mark the creating actor as async.
     *
     * @return self
     */
    public Builder setAsync(boolean enabled) {
      this.isAsync = enabled;
      return this;
    }

    /**
     * Set several extended key-value information to this actor, and these fields will be stored
     * into GCS eventually.
     *
     * @param extendedProperties customer extended information.
     * @return self
     */
    public Builder setExtendedProperties(Map<String, String> extendedProperties) {
      this.extendedProperties = extendedProperties;
      return this;
    }

    public Builder setExtendedProperties(String key, String value) {
      this.extendedProperties.put(key, value);
      return this;
    }

    public ActorCreationOptions build() {
      return new ActorCreationOptions(
          name,
          lifetime,
          resources,
          maxRestarts,
          jvmOptions,
          maxConcurrency,
          serializedRuntimeEnv,
          group,
          bundleIndex,
          memoryMb,
          concurrencyGroups,
          enableTaskFastFail,
          extendedProperties,
          labels,
          schedulingStrategy,
          isAsync);
    }

    /** Set the concurrency groups for this actor. */
    public Builder setConcurrencyGroups(List<ConcurrencyGroup> concurrencyGroups) {
      this.concurrencyGroups = concurrencyGroups;
      return this;
    }

    /** Set the labels for this actor. */
    public Builder setLabel(String key, String value) {
      this.labels.put(key, value);
      return this;
    }

    /** Set the labels for this actor. */
    public Builder setLabels(Map<String, String> labels) {
      if (labels == null) {
        return this;
      }
      this.labels.putAll(labels);
      return this;
    }

    /** Add actor affinity match expression for this actor. */
    public Builder setSchedulingStrategy(SchedulingStrategy schedulingStrategy) {
      if (this.group != null) {
        throw new IllegalArgumentException(
            "The placement group and scheduling strategy can't be set at the same time.");
      }
      this.schedulingStrategy = schedulingStrategy;
      return this;
    }
  }
}
