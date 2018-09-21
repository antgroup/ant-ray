package org.ray.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.PlasmaClient;
import org.ray.api.WorkerMode;
import org.ray.runtime.functionmanager.NativeRemoteFunctionManager;
import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.gcs.AddressInfo;
import org.ray.runtime.gcs.KeyValueStoreLink;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.gcs.StateStoreProxy;
import org.ray.runtime.gcs.StateStoreProxyImpl;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.util.RayLog;

/**
 * native runtime for local box and cluster run.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  static {
    System.err.println("Current working directory is " + System.getProperty("user.dir"));
    System.loadLibrary("local_scheduler_library_java");
    System.loadLibrary("plasma_java");
  }

  private StateStoreProxy stateStoreProxy;
  private KeyValueStoreLink kvStore = null;
  private RunManager manager = null;

  public RayNativeRuntime() {

  }

  @Override
  public void start() throws Exception {
    final boolean isWorker = (rayConfig.workerMode == WorkerMode.WORKER);

    if (rayConfig.redisAddress.length() == 0) {
      if (isWorker) {
        throw new Error("Redis address must be configured under Worker mode.");
      }
      startOnebox();
      initStateStore(rayConfig.redisAddress);
    } else {
      initStateStore(rayConfig.redisAddress);
      if (!isWorker) {
        List<AddressInfo> nodes = stateStoreProxy.getAddressInfo(
                            rayConfig.nodeIp, rayConfig.redisAddress, 5);
        rayConfig.objectStoreName = nodes.get(0).storeName;
        rayConfig.rayletSocketName = nodes.get(0).rayletSocketName;
      }
    }

    // initialize remote function manager
    RemoteFunctionManager funcMgr = rayConfig.runMode.isDevPathManager()
        ? new NopRemoteFunctionManager(rayConfig.driverId) : new NativeRemoteFunctionManager(kvStore);

    // initialize worker context
    if (rayConfig.workerMode == WorkerMode.DRIVER) {
      // TODO: The relationship between workerID, driver_id and dummy_task.driver_id should be
      // recheck carefully
      workerContext.setWorkerId(rayConfig.driverId);
    }

    // initialize the links
    final int releaseDelay = 0;
    ObjectStoreLink plink = new PlasmaClient(rayConfig.objectStoreName, "", releaseDelay);

    RayletClient rayletClient = new RayletClientImpl(
        rayConfig.rayletSocketName,
        workerContext.getCurrentWorkerId(),
        isWorker,
        workerContext.getCurrentTask().taskId
    );

    initMembers(rayletClient, plink, funcMgr);

    // register
    registerWorker(isWorker);

    RayLog.core.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreName, rayConfig.rayletSocketName);

  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup(true);
    }
  }

  private void startOnebox() throws Exception {
    rayConfig.cleanup = true;
    manager = new RunManager(rayConfig);
    manager.startRayHead();

    rayConfig.redisAddress = manager.info().redisAddress;
    rayConfig.objectStoreName = manager.info().localStores.get(0).storeName;
    rayConfig.rayletSocketName = manager.info().localStores.get(0).rayletSocketName;
  }

  private void initStateStore(String redisAddress) throws Exception {
    kvStore = new RedisClient();
    kvStore.setAddr(redisAddress);
    stateStoreProxy = new StateStoreProxyImpl(kvStore);
    stateStoreProxy.initializeGlobalState();
  }

  private void registerWorker(boolean isWorker) {
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(workerContext.getCurrentWorkerId().getBytes());
    if (!isWorker) {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      kvStore.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      kvStore.hmset("Workers:" + workerId, workerInfo);
    }
  }

}
