package io.ray.api.placementgroup;

import java.util.HashMap;
import java.util.Map;

/* The bundle is a resource set that will be preallocated in a node. */
public final class Bundle {

  public final Map<String, Double> resources;

  public Bundle(Map<String, Double> resources) {
    this.resources = resources;
  }

  public boolean validate() {
    return this.resources.values().stream().allMatch(resource -> resource > 0);
  }

  /** This inner class for building Bundle. */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();

    /**
     * Set the memory resource requirement for resource.
     *
     * @param value memory size in mb.
     * @return self.
     */
    public Builder setMemoryMb(long value) {
      return setResource("memory", (double) value * 1024 * 1024);
    }

    /**
     * Set the CPU resource requirement for resource.
     *
     * @param value CPU resource size which could be a fraction.
     * @return self.
     */
    public Builder setCpu(Double value) {
      return setResource("CPU", value);
    }

    /**
     * Set the GPU resource requirement for resource.
     *
     * @param value GPU resource size which could be a fraction.
     * @return self.
     */
    public Builder setGpu(Double value) {
      return setResource("GPU", value);
    }

    /**
     * Set a custom resource requirement for resource.
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
     * Set custom requirements for multiple resources.
     *
     * @param resources requirements for multiple resources.
     * @return self.
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    public Bundle build() {
      return new Bundle(resources);
    }
  }
}
