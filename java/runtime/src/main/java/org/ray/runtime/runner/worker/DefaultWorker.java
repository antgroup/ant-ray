package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.api.config.RayConfig;
import org.ray.api.config.RunMode;
import org.ray.api.config.WorkerMode;
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
      String rayConfigFile = null;
      String redisAddress = null;
      String nodeIpAddress = null;
      String overwrite = null;

      for (String arg : args) {
        if (arg.startsWith("--config=")) {
          rayConfigFile = arg.substring("--config=".length());
        } else if (arg.startsWith("--overwrite=")) {
          overwrite = arg.substring("--overwrite=".length());
        } else if (arg.startsWith("--redis_address=")) {
          redisAddress = arg.substring("--redis_address=".length());
        } else if (arg.startsWith("--node_ip_address=")) {
          nodeIpAddress = arg.substring("--node_ip_address=".length());
        }
      }

      RayConfig config = new RayConfig(rayConfigFile, overwrite);

      config.setRedisAddr(redisAddress)
          .setNodeIpAddr(nodeIpAddress)
          .setRunMode(RunMode.SINGLE_BOX)
          .setWorkerMode(WorkerMode.WORKER)
          .build();

      Ray.init(config);

      ((AbstractRayRuntime) Ray.internal()).loop();
      throw new RuntimeException("Control flow should never reach here");
    } catch (Throwable e) {
      e.printStackTrace();
      System.err
          .println("--config=ray.config.ini --overwrite=ray.java.start.worker_mode=WORKER;...");
      System.exit(-1);
    }
  }
}
