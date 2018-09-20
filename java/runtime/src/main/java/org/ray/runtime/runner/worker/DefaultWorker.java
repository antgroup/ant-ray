package org.ray.runtime.runner.worker;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.ray.api.Ray;
import org.ray.api.RayInitConfig;
import org.ray.api.RunMode;
import org.ray.api.WorkerMode;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RayConfig;

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
      System.setProperty("ray.worker.mode", "WORKER");
      final String DEFAULT_CONFIG_FILE = "ray.default.conf";
      final String CUSTOM_CONFIG_FILE = "ray.conf";
      Config config = ConfigFactory.load(DEFAULT_CONFIG_FILE)
                          .withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
      RayConfig rayConfig = new RayConfig(config);

      System.out.println("----------aaaaaa-----------");
      System.out.println(rayConfig.workerMode);
      for (String elem: args) {
        System.out.println(elem);
      }
      System.out.println("----------aaaaaa-----------");

      RayInitConfig initConfig = new RayInitConfig(args);
      // TODO(qwang): We should specify RunMode in code.
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
