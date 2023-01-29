package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** `GlobalStateAccessor` is used for accessing information from GCS. */
public class GlobalStateAccessor {
  // NOTE(lingxuan.zlx): this is a singleton, it can not be changed during a Ray session.
  // Native pointer to the C++ GcsStateAccessor.
  private Long globalStateAccessorNativePointer = 0L;
  private static GlobalStateAccessor globalStateAccessor;

  public static synchronized GlobalStateAccessor getInstance(
      String redisAddress, String redisPassword) {
    if (null == globalStateAccessor) {
      globalStateAccessor = new GlobalStateAccessor(redisAddress, redisPassword);
    }
    return globalStateAccessor;
  }

  public static synchronized void destroyInstance() {
    if (null != globalStateAccessor) {
      globalStateAccessor.destroyGlobalStateAccessor();
      globalStateAccessor = null;
    }
  }

  private GlobalStateAccessor(String redisAddress, String redisPassword) {
    globalStateAccessorNativePointer = nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
    validateGlobalStateAccessorPointer();
    connect();
  }

  private boolean connect() {
    return this.nativeConnect(globalStateAccessorNativePointer);
  }

  private void validateGlobalStateAccessorPointer() {
    Preconditions.checkState(
        globalStateAccessorNativePointer != 0,
        "Global state accessor native pointer must not be 0.");
  }

  /** Returns A list of job info with JobInfo protobuf schema. */
  public List<byte[]> getAllJobInfo() {
    // Fetch a job list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllJobInfo(globalStateAccessorNativePointer);
    }
  }

  /** Returns next job id. */
  public byte[] getNextJobID() {
    // Get next job id from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetNextJobID(globalStateAccessorNativePointer);
    }
  }

  /** Returns A list of node info with GcsNodeInfo protobuf schema. */
  public List<byte[]> getAllNodeInfo() {
    // Fetch a node list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllNodeInfo(globalStateAccessorNativePointer);
    }
  }

  /** Return A list of node info with GcsNodeInfo protobuf schema in the specified nodegroup. */
  public List<byte[]> getAllNodeInfoByNodegroup(String nodegroupId) {
    // Fetch a node list with protobuf bytes format from GCS by nodegroup.
    synchronized (GlobalStateAccessor.class) {
      Preconditions.checkState(
          globalStateAccessorNativePointer != 0,
          "Get all node info when global state accessor have been destroyed.");
      return this.nativeGetAllNodeInfoByNodegroup(globalStateAccessorNativePointer, nodegroupId);
    }
  }

  /**
   * Get node resource info.
   *
   * @param nodeId node unique id.
   * @return A map of node resource info in protobuf schema.
   */
  public byte[] getNodeResourceInfo(UniqueId nodeId) {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return nativeGetNodeResourceInfo(globalStateAccessorNativePointer, nodeId.getBytes());
    }
  }

  public byte[] getPlacementGroupInfo(PlacementGroupId placementGroupId) {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return nativeGetPlacementGroupInfo(
          globalStateAccessorNativePointer, placementGroupId.getBytes());
    }
  }

  public byte[] getPlacementGroupInfo(String name, String namespace) {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return nativeGetPlacementGroupInfoByName(globalStateAccessorNativePointer, name, namespace);
    }
  }

  public List<byte[]> getAllPlacementGroupInfo() {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllPlacementGroupInfo(globalStateAccessorNativePointer);
    }
  }

  public byte[] getApiServerAddress() {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetApiServerAddress(globalStateAccessorNativePointer);
    }
  }

  /** Returns A list of actor info with ActorInfo protobuf schema. */
  public List<byte[]> getAllActorInfo() {
    // Fetch a actor list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllActorInfo(globalStateAccessorNativePointer);
    }
  }

  /** Returns An actor info with ActorInfo protobuf schema. */
  public byte[] getActorInfo(ActorId actorId) {
    // Fetch an actor with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetActorInfo(globalStateAccessorNativePointer, actorId.getBytes());
    }
  }

  /** Return An actor info with ActorInfo protobuf schema. */
  public byte[] getResourceUsage(UniqueId nodeId) {
    // Fetch a heartbeat of node with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      Preconditions.checkState(globalStateAccessorNativePointer != 0);
      return this.nativeGetResourceUsage(globalStateAccessorNativePointer, nodeId.getBytes());
    }
  }

  public boolean updateJobResourceRequirements(
      JobId jobId,
      Map<String, Double> minResourceRequirements,
      Map<String, Double> maxResourceRequirements) {
    if (minResourceRequirements.isEmpty()) {
      return false;
    }
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      List<String> minResourceNames = new ArrayList<>(minResourceRequirements.keySet());
      double[] minResourceValues = new double[minResourceRequirements.size()];
      int index = 0;
      for (Double value : minResourceRequirements.values()) {
        minResourceValues[index] = value;
        ++index;
      }

      List<String> maxResourceNames = new ArrayList<>(maxResourceRequirements.keySet());
      double[] maxResourceValues = new double[maxResourceRequirements.size()];
      index = 0;
      for (Double value : maxResourceRequirements.values()) {
        maxResourceValues[index] = value;
        ++index;
      }

      boolean output = false;
      try {
        output =
            this.nativeUpdateJobResourceRequirements(
                globalStateAccessorNativePointer,
                jobId.getBytes(),
                minResourceNames,
                minResourceValues,
                maxResourceNames,
                maxResourceValues);
      } catch (Exception e) {
        throw e;
      }
      return output;
    }
  }

  public void putJobData(JobId jobId, byte[] key, byte[] value) {
    synchronized (GlobalStateAccessor.class) {
      this.nativePutJobData(globalStateAccessorNativePointer, jobId.getBytes(), key, value);
    }
  }

  public byte[] getJobData(JobId jobId, byte[] key) {
    synchronized (GlobalStateAccessor.class) {
      return this.nativeGetJobData(globalStateAccessorNativePointer, jobId.getBytes(), key);
    }
  }

  /** Get the node to connect for a Ray driver. */
  public byte[] getNodeToConnectForDriver(String nodeIpAddress) {
    // Fetch a node with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetNodeToConnectForDriver(globalStateAccessorNativePointer, nodeIpAddress);
    }
  }

  private void destroyGlobalStateAccessor() {
    synchronized (GlobalStateAccessor.class) {
      if (0 == globalStateAccessorNativePointer) {
        return;
      }
      this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePointer);
      globalStateAccessorNativePointer = 0L;
    }
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native byte[] nativeGetNextJobID(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfoByNodegroup(long nativePtr, String nodegroupId);

  private native byte[] nativeGetNodeResourceInfo(long nativePtr, byte[] nodeId);

  private native List<byte[]> nativeGetAllActorInfo(long nativePtr);

  private native byte[] nativeGetActorInfo(long nativePtr, byte[] actorId);

  private native byte[] nativeGetResourceUsage(long nativePtr, byte[] nodeId);

  private native byte[] nativeGetPlacementGroupInfo(long nativePtr, byte[] placementGroupId);

  private native byte[] nativeGetPlacementGroupInfoByName(
      long nativePtr, String name, String namespace);

  private native List<byte[]> nativeGetAllPlacementGroupInfo(long nativePtr);

  private native boolean nativeUpdateJobResourceRequirements(
      long nativePtr,
      byte[] jobId,
      List<String> minResourceNames,
      double[] minResourceValues,
      List<String> maxResourceNames,
      double[] maxResourceValues);

  private native void nativePutJobData(long nativePtr, byte[] jobId, byte[] key, byte[] data);

  private native byte[] nativeGetJobData(long nativePtr, byte[] jobId, byte[] key);

  private native byte[] nativeGetApiServerAddress(long nativePtr);

  private native byte[] nativeGetNodeToConnectForDriver(long nativePtr, String nodeIpAddress);
}
