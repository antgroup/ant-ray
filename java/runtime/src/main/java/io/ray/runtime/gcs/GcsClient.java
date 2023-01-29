package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.events.ActorState;
import io.ray.api.exception.RayException;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.runtimecontext.ActorInfo;
import io.ray.api.runtimecontext.Address;
import io.ray.api.runtimecontext.JobInfo;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.generated.Gcs;
import io.ray.runtime.generated.Gcs.BasicGcsNodeInfo;
import io.ray.runtime.generated.Gcs.GcsNodeInfo;
import io.ray.runtime.placementgroup.PlacementGroupUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of GcsClient. */
public class GcsClient {
  private static Logger LOGGER = LoggerFactory.getLogger(GcsClient.class);

  private String redisAddress;
  private String redisPassword;
  private RedisClient primary = null;

  private GlobalStateAccessor globalStateAccessor;

  public GcsClient(String redisAddress, String redisPassword) {
    this.redisAddress = redisAddress;
    this.redisPassword = redisPassword;
    globalStateAccessor = GlobalStateAccessor.getInstance(redisAddress, redisPassword);
  }

  /**
   * ANT-INTERNAL: In Java lang worker we SHOULD NOT use redis client directly, and that's why we
   * make it created in lazy style. This is now only used by `fault_tolerance` module, we would to
   * remove this in future.
   */
  public RedisClient getPrimaryRedisClient() {
    if (primary == null) {
      primary = new RedisClient(redisAddress, redisPassword);
    }
    return primary;
  }

  /**
   * Get placement group by {@link PlacementGroupId}.
   *
   * @param placementGroupId Id of placement group.
   * @return The placement group.
   */
  public PlacementGroup getPlacementGroupInfo(PlacementGroupId placementGroupId) {
    byte[] result = globalStateAccessor.getPlacementGroupInfo(placementGroupId);
    return PlacementGroupUtils.generatePlacementGroupFromByteArray(result);
  }

  /**
   * Get a placement group by name.
   *
   * @param name Name of the placement group.
   * @param namespace The namespace of the placement group.
   * @return The placement group.
   */
  public PlacementGroup getPlacementGroupInfo(String name, String namespace) {
    byte[] result = globalStateAccessor.getPlacementGroupInfo(name, namespace);
    return result == null ? null : PlacementGroupUtils.generatePlacementGroupFromByteArray(result);
  }

  /**
   * Get all placement groups in this cluster.
   *
   * @return All placement groups.
   */
  public List<PlacementGroup> getAllPlacementGroupInfo() {
    List<byte[]> results = globalStateAccessor.getAllPlacementGroupInfo();

    List<PlacementGroup> placementGroups = new ArrayList<>();
    for (byte[] result : results) {
      placementGroups.add(PlacementGroupUtils.generatePlacementGroupFromByteArray(result));
    }
    return placementGroups;
  }

  /**
   * Get the address of the api server.
   *
   * @return The address of the api server, format is `${ip}:${port}`.
   */
  public String getApiServerAddress() {
    byte[] result = globalStateAccessor.getApiServerAddress();
    Gcs.ApiServerAddress address;
    try {
      address = Gcs.ApiServerAddress.parseFrom(result);
    } catch (InvalidProtocolBufferException e) {
      throw new RayException("Failed to parse api server address info.", e);
    }
    return String.format("%s:%s", address.getIp(), address.getPort());
  }

  public List<NodeInfo> getAllNodeInfo() {
    return getAllNodeInfoByNodegroup(/*nodegroupId = */ "");
  }

  public List<JobInfo> getAllJobInfo() {
    List<JobInfo> info = new ArrayList<>();
    List<byte[]> results = globalStateAccessor.getAllJobInfo();
    results.forEach(
        result -> {
          try {
            Gcs.JobTableData t = Gcs.JobTableData.parseFrom(result);
            info.add(
                new JobInfo(
                    JobId.fromBytes(t.getJobId().toByteArray()), t.getIsDead(), t.getStateValue()));
          } catch (InvalidProtocolBufferException e) {
            throw new RayException("Failed to parse job info.", e);
          }
        });
    return info;
  }

  public List<ActorInfo> getAllActorInfo() {
    List<ActorInfo> actorInfos = new ArrayList<>();
    List<byte[]> results = globalStateAccessor.getAllActorInfo();
    results.forEach(
        result -> {
          try {
            Gcs.ActorTableData info = Gcs.ActorTableData.parseFrom(result);
            info.getExtendedPropertiesMap();
            actorInfos.add(
                new ActorInfo(
                    ActorId.fromBytes(info.getActorId().toByteArray()),
                    ActorState.fromValue(info.getState().getNumber()),
                    info.getNumRestarts(),
                    new Address(
                        UniqueId.fromByteBuffer(
                            ByteBuffer.wrap(info.getAddress().getRayletId().toByteArray())),
                        info.getAddress().getIpAddress(),
                        info.getAddress().getPort()),
                    info.getName(),
                    info.getExtendedPropertiesMap()));
          } catch (InvalidProtocolBufferException e) {
            throw new RayException("Failed to parse actor info.", e);
          }
        });

    return actorInfos;
  }

  /*
   * Get nodes info by nodegroup id.
   * Return all nodes info of all nodegroups if the specified nodegroupId is empty.
   */
  public List<NodeInfo> getAllNodeInfoByNodegroup(String nodegroupId) {
    List<byte[]> results = globalStateAccessor.getAllNodeInfoByNodegroup(nodegroupId);

    // This map is used for deduplication of node entries.
    Map<UniqueId, NodeInfo> nodes = new HashMap<>();
    for (byte[] result : results) {
      Preconditions.checkNotNull(result);
      GcsNodeInfo data = null;
      try {
        data = GcsNodeInfo.parseFrom(result);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Received invalid protobuf data from GCS.");
      }
      final UniqueId nodeId =
          UniqueId.fromByteBuffer(data.getBasicGcsNodeInfo().getNodeId().asReadOnlyByteBuffer());

      // NOTE(lingxuan.zlx): we assume no duplicated node id in fetched node list
      // and it's only one final state for each node in recorded table.
      NodeInfo nodeInfo =
          new NodeInfo(
              nodeId,
              data.getBasicGcsNodeInfo().getNodeName(),
              data.getBasicGcsNodeInfo().getNodeManagerAddress(),
              data.getBasicGcsNodeInfo().getNodeManagerHostname(),
              data.getBasicGcsNodeInfo().getNodeManagerPort(),
              data.getObjectStoreSocketName(),
              data.getRayletSocketName(),
              data.getBasicGcsNodeInfo().getState() == BasicGcsNodeInfo.GcsNodeState.ALIVE,
              new HashMap<>(),
              new HashMap<>());
      nodes.put(nodeId, nodeInfo);
    }

    // Fill total resources.
    for (Map.Entry<UniqueId, NodeInfo> node : nodes.entrySet()) {
      if (node.getValue().isAlive) {
        node.getValue().totalResources.putAll(getResourcesForClient(node.getKey()));
      }
    }

    // Fill available resources.
    for (Map.Entry<UniqueId, NodeInfo> node : nodes.entrySet()) {
      if (node.getValue().isAlive) {
        node.getValue().availableResources.putAll(getAvailableResource(node.getKey()));
      }
    }

    return new ArrayList<>(nodes.values());
  }

  private Map<String, Double> getResourcesForClient(UniqueId clientId) {
    byte[] resourceMapBytes = globalStateAccessor.getNodeResourceInfo(clientId);
    Gcs.ResourceMap resourceMap;
    try {
      resourceMap = Gcs.ResourceMap.parseFrom(resourceMapBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid protobuf data from GCS.");
    }
    HashMap<String, Double> resources = new HashMap<>();
    for (Map.Entry<String, Gcs.ResourceTableData> entry : resourceMap.getItemsMap().entrySet()) {
      resources.put(entry.getKey(), entry.getValue().getResourceCapacity());
    }
    return resources;
  }

  /** If the actor exists in GCS. */
  public boolean actorExists(ActorId actorId) {
    byte[] result = globalStateAccessor.getActorInfo(actorId);
    return result != null;
  }

  public boolean wasCurrentActorRestarted(ActorId actorId) {
    // TODO(ZhuSenlin): Get the actor table data from CoreWorker later.
    byte[] value = globalStateAccessor.getActorInfo(actorId);
    if (value == null) {
      return false;
    }
    Gcs.ActorTableData actorTableData = null;
    try {
      actorTableData = Gcs.ActorTableData.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid protobuf data from GCS.");
    }
    return actorTableData.getNumRestarts() != 0;
  }

  public JobId nextJobId() {
    return JobId.fromBytes(globalStateAccessor.getNextJobID());
  }

  public GcsNodeInfo getNodeToConnectForDriver(String nodeIpAddress) {
    byte[] value = globalStateAccessor.getNodeToConnectForDriver(nodeIpAddress);
    Preconditions.checkNotNull(value);
    GcsNodeInfo nodeInfo = null;
    try {
      nodeInfo = GcsNodeInfo.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid protobuf data from GCS.");
    }
    return nodeInfo;
  }

  /** Destroy global state accessor when ray native runtime will be shutdown. */
  public void destroy() {
    // Only ray shutdown should call gcs client destroy.
    LOGGER.debug("Destroying global state accessor.");
    GlobalStateAccessor.destroyInstance();
  }

  /** Get available resources of the specific node id. */
  private Map<String, Double> getAvailableResource(UniqueId nodeId) {
    byte[] result = globalStateAccessor.getResourceUsage(nodeId);
    Preconditions.checkNotNull(result, "Failed to get available resource of node: " + nodeId);
    Gcs.ResourcesData data;
    try {
      data = Gcs.ResourcesData.parseFrom(result);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid protobuf data from GCS.");
    }

    return data.getResourcesAvailableMap();
  }

  public boolean updateJobResourceRequirements(
      JobId jobId,
      Map<String, Double> minResourceRequirements,
      Map<String, Double> maxResourceRequirements) {
    boolean output = false;
    try {
      output =
          globalStateAccessor.updateJobResourceRequirements(
              jobId, minResourceRequirements, maxResourceRequirements);
    } catch (Exception e) {
      throw e;
    }
    return output;
  }

  public void putJobData(JobId jobId, byte[] key, byte[] value) {
    globalStateAccessor.putJobData(jobId, key, value);
  }

  public byte[] getJobData(JobId jobId, byte[] key) {
    return globalStateAccessor.getJobData(jobId, key);
  }
}
