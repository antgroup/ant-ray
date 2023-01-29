package io.ray.api.options;

import java.util.ArrayList;
import java.util.List;

/*
 * Node affinity scheduling strategy
 */
public class NodeAffinitySchedulingStrategy implements SchedulingStrategy {
  private List<String> nodes;
  private boolean soft;
  private boolean antiAffinity;

  /**
   * Node affinity scheduling strategy.
   *
   * @param nodes The list of node id hex string.
   * @param soft soft match
   * @param antiAffinity anti-affinity
   */
  private NodeAffinitySchedulingStrategy(List<String> nodes, boolean soft, boolean antiAffinity) {
    this.nodes = nodes;
    this.soft = soft;
    this.antiAffinity = antiAffinity;
  }

  public List<String> getNodes() {
    return nodes;
  }

  public boolean isSoft() {
    return soft;
  }

  public boolean isAntiAffinity() {
    return antiAffinity;
  }

  /** The inner class for building NodeAffinitySchedulingStrategy. */
  public static class Builder {
    private List<String> nodes = new ArrayList<>();
    private boolean soft = false;
    private boolean antiAffinity = false;

    /**
     * add the hex string of node id.
     *
     * @param nodeIdHexStr the hex string of node id
     * @return Builder
     */
    public Builder addNode(String nodeIdHexStr) {
      if (!this.nodes.contains(nodeIdHexStr)) {
        this.nodes.add(nodeIdHexStr);
      }
      return this;
    }

    /**
     * set soft of node affinity strategy.
     *
     * @param soft is soft
     * @return Builder
     */
    public Builder setSoft(boolean soft) {
      this.soft = soft;
      return this;
    }

    /**
     * set antiAffinity of node affinity strategy.
     *
     * @param antiAffinity is anti-affinity
     * @return Builder
     */
    public Builder setAntiAffintiy(boolean antiAffinity) {
      this.antiAffinity = antiAffinity;
      return this;
    }

    // build NodeAffinitySchedulingStrategy
    public NodeAffinitySchedulingStrategy build() {
      return new NodeAffinitySchedulingStrategy(nodes, soft, antiAffinity);
    }
  }
}
