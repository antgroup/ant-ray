package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.api.RayInitConfig;
import org.ray.api.RunMode;
import org.ray.api.WorkerMode;
import org.ray.runtime.AbstractRayRuntime;

/**
 * default worker implementation.
 */
public class DefaultWorker {

  //
  // String workerCmd = "java" + " -jarls " + workerPath + " --node-ip-address=" + ip
  // + " --object-store-name=" + storeName
  // + " --object-store-manager-name=" + storeManagerName
  // + " --local-scheduler-name=" + name + " --redis-address=" + redisAddress
  //
  public static void main(String[] args) {
    try {
      RayInitConfig initConfig = new RayInitConfig(args);
      initConfig.setRunMode(RunMode.SINGLE_BOX);
      initConfig.setWorkerMode(WorkerMode.WORKER);

      Ray.init(initConfig);
      ((AbstractRayRuntime)Ray.internal()).loop();
      throw new RuntimeException("Control flow should never reach here");

    } catch (Throwable e) {
      e.printStackTrace();
      System.err
          .println("--config=ray.config.ini --overwrite=ray.java.start.worker_mode=WORKER;...");
      System.exit(-1);
    }
  }
}
