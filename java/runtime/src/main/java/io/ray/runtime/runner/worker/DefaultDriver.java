package io.ray.runtime.runner.worker;

import io.ray.api.EventSeverity;
import io.ray.api.Ray;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class DefaultDriver {

  public static void main(String[] args) throws Throwable {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "Failed to start DefaultDriver: driver class name was not specified.");
    }
    String driverClassName = args[0];
    int driverArgsLength = args.length - 1;
    String[] driverArgs = null;
    if (driverArgsLength > 0) {
      driverArgs = new String[driverArgsLength];
      System.arraycopy(args, 1, driverArgs, 0, driverArgsLength);
    } else {
      driverArgs = new String[0];
    }

    System.setProperty("ray.worker.mode", "DRIVER");

    try {
      Class<?> clazz = Class.forName(driverClassName);
      // We do not call Ray.init() here. It's still users' responsibility to call it.
      clazz.getMethod("main", String[].class).invoke(null, (Object) driverArgs);
    } catch (Throwable e) {
      if (e instanceof InvocationTargetException && e.getCause() != null) {
        e = e.getCause();
      }
      if (Ray.isInitialized()) {
        Ray.reportEvent(
            EventSeverity.ERROR,
            "DriverFailure",
            "Driver of Job "
                + Ray.getRuntimeContext().getCurrentJobId()
                + " failed. Message: \n"
                + ExceptionUtils.getStackTrace(e));
      }
      e.printStackTrace();
      // Throw the exception again to let the driver fail.
      throw e;
    }

    // If the driver exits normally, we invoke Ray.shutdown() again here, in case the
    // user code forgot to invoke it.
    Ray.shutdown();
  }
}
