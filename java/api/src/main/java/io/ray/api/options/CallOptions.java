package io.ray.api.options;

import io.ray.api.placementgroup.PlacementGroup;
import java.util.HashMap;
import java.util.Map;

/** The options for RayCall. */
public class CallOptions extends BaseTaskOptions {

  public final String name;
  public final boolean ignoreReturn;
  public final PlacementGroup group;
  public final int bundleIndex;
  private final SchedulingStrategy schedulingStrategy;

  private CallOptions(
      String name,
      Map<String, Double> resources,
      boolean ignoreReturn,
      PlacementGroup group,
      int bundleIndex,
      SchedulingStrategy schedulingStrategy) {
    super(resources);
    this.name = name;
    this.ignoreReturn = ignoreReturn;
    this.group = group;
    this.bundleIndex = bundleIndex;
    this.schedulingStrategy = schedulingStrategy;
  }

  /** This inner class for building CallOptions. */
  public static class Builder {

    private String name;
    private Map<String, Double> resources = new HashMap<>();
    private PlacementGroup group;
    private int bundleIndex;

    private boolean ignoreReturn;
    private SchedulingStrategy schedulingStrategy;

    /**
     * Set the memory resource requirement for resource. It will assign a sole worker process for
     * this task if this method is called. This method can be called multiple times. If the same
     * resource is set multiple times, the latest quantity will be used.
     *
     * @param value memory size in mb
     * @return self
     */
    public Builder setMemoryMb(long value) {
      this.resources.put("memory", (double) value * 1024 * 1024);
      return this;
    }

    /**
     * Set a name for this task.
     *
     * @param name task name
     * @return self
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set a custom resource requirement for resource {@code name}. This method can be called
     * multiple times. If the same resource is set multiple times, the latest quantity will be used.
     *
     * @param name resource name
     * @param value resource capacity
     * @return self
     */
    public Builder setResource(String name, Double value) {
      this.resources.put(name, value);
      return this;
    }

    /**
     * Set custom requirements for multiple resources. This method can be called multiple times. If
     * the same resource is set multiple times, the latest quantity will be used.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    public Builder setIgnoreReturn(boolean returnVoid) {
      this.ignoreReturn = returnVoid;
      return this;
    }

    /**
     * Set the placement group to place this actor in.
     *
     * @param group The placement group of the actor.
     * @param bundleIndex The index of the bundle to place this task in.
     * @return self
     */
    public Builder setPlacementGroup(PlacementGroup group, int bundleIndex) {
      if (this.schedulingStrategy != null) {
        throw new IllegalArgumentException(
            "The placement group and scheduling strategy can't be set at the same time.");
      }
      this.group = group;
      this.bundleIndex = bundleIndex;
      return this;
    }

    /** Add node affinity match expression for this task. */
    public Builder setSchedulingStrategy(SchedulingStrategy schedulingStrategy) {
      if (this.group != null) {
        throw new IllegalArgumentException(
            "The placement group and scheduling strategy can't be set at the same time.");
      }
      if (schedulingStrategy instanceof ActorAffinitySchedulingStrategy) {
        throw new IllegalArgumentException(
            "The actor affinity scheduling strategy can not be used to normal tasks.");
      }
      this.schedulingStrategy = schedulingStrategy;
      return this;
    }

    public CallOptions build() {
      return new CallOptions(name, resources, ignoreReturn, group, bundleIndex, schedulingStrategy);
    }
  }
}
