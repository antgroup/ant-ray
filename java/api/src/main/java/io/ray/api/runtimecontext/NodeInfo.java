package io.ray.api.runtimecontext;

import io.ray.api.id.UniqueId;
import java.util.Map;

/** A class that represents the information of a node. */
public class NodeInfo {

  public final UniqueId nodeId;

  public final String nodeName;

  public final String nodeAddress;

  public final String nodeHostname;

  public final int nodeManagerPort;

  public final String objectStoreSocketName;

  public final String rayletSocketName;

  public final boolean isAlive;

  public final Map<String, Double> availableResources;

  public final Map<String, Double> totalResources;

  public NodeInfo(
      UniqueId nodeId,
      String nodeName,
      String nodeAddress,
      String nodeHostname,
      int nodeManagerPort,
      String objectStoreSocketName,
      String rayletSocketName,
      boolean isAlive,
      Map<String, Double> availableResources,
      Map<String, Double> totalResources) {
    this.nodeId = nodeId;
    this.nodeName = nodeName;
    this.nodeAddress = nodeAddress;
    this.nodeHostname = nodeHostname;
    this.nodeManagerPort = nodeManagerPort;
    this.objectStoreSocketName = objectStoreSocketName;
    this.rayletSocketName = rayletSocketName;
    this.isAlive = isAlive;
    this.availableResources = availableResources;
    this.totalResources = totalResources;
  }

  public String toString() {
    return "NodeInfo{"
        + "nodeId='"
        + nodeId
        + '\''
        + ", nodeName='"
        + nodeName
        + "\'"
        + ", nodeAddress='"
        + nodeAddress
        + "\'"
        + ", nodeHostname'"
        + nodeHostname
        + "\'"
        + ", isAlive="
        + isAlive
        + ", availableResources="
        + availableResources
        + ", resources="
        + totalResources
        + "}";
  }
}
