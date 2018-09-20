package org.ray.api.runtime;

import org.ray.api.RayInitConfig;
import java.lang.reflect.Method;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  @Override
  public RayRuntime createRayRuntime(RayInitConfig initConfig) {
    try {
      Class clz;
      if (initConfig.getRunMode().isNativeRuntime()) {
        clz = Class.forName("org.ray.runtime.RayNativeRuntime");
      } else {
        clz = Class.forName("org.ray.runtime.RayDevRuntime");
      }

      RayRuntime runtime = (RayRuntime) clz.newInstance();
      Method init = clz.getMethod("init");
      init.setAccessible(true);
      init.invoke(runtime);
      init.setAccessible(false);

      return runtime;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
