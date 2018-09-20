package org.ray.runtime.runner.worker;

import org.ray.api.Ray;

/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  //
  // " --node-ip-address=" + ip
  // + " --redis-address=" + redisAddress
  // + " --driver-class" + className
  //
  public static void main(String[] args) {
    try {
      System.setProperty("ray.worker.mode", "DRIVER");
      Ray.init();

      //TODO(qwang): We should get dirver class and driver_args from args.
      String driverClass = null;
      String driverArgs = null;
      //String driverClass = AbstractRayRuntime.configReader
      //    .getStringValue("ray.java.start", "driver_class", "",
      //        "java class which main is served as the driver in a java worker");
      //String driverArgs = AbstractRayRuntime.configReader
      //    .getStringValue("ray.java.start", "driver_args", "",
      //        "arguments for the java class main function which is served at the driver");
      Class<?> cls = Class.forName(driverClass);
      String[] argsArray = (driverArgs != null) ? driverArgs.split(",") : (new String[] {});
      cls.getMethod("main", String[].class).invoke(null, (Object) argsArray);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
