package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.api.config.RayConfig;
import org.ray.api.config.RunMode;
import org.ray.api.config.WorkerMode;

/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  //
  // " --node-ip-address=" + ip
  // + " --redis-address=" + redisAddress
  // + " --driver-class" + className
  // + " --class-args" + classArgs
  //
  public static void main(String[] args) {
    try {
      String rayConfigFile = null;
      String redisAddress = null;
      String nodeIpAddress = null;
      String driverClass = null;
      String driverArgs = null;
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
        } else if (arg.startsWith("--driver_class=")) {
          driverClass = arg.substring("--driver_class=".length());
        } else if (arg.startsWith("--driver_args=")) {
          // be careful, wrap it with "--driver_args=a1,a2,a3"
          driverArgs = arg.substring("--driver_args=".length());
        }
      }

      RayConfig config = new RayConfig(rayConfigFile, overwrite);

      config.setRedisAddr(redisAddress)
              .setNodeIpAddr(nodeIpAddress)
              .setRunMode(RunMode.SINGLE_BOX)
              .setWorkerMode(WorkerMode.DRIVER)
              .build();

      Ray.init(config);

      Class<?> cls = Class.forName(driverClass);
      String[] argsArray = (driverArgs != null) ? driverArgs.split(",")
              : (new String[] {});
      cls.getMethod("main", String[].class).invoke(null, (Object) argsArray);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
