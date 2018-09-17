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
      Method m = Class.forName("org.ray.runtime.AbstractRayRuntime").getDeclaredMethod("init", RayInitConfig.class);
      m.setAccessible(true);
      RayRuntime runtime = (RayRuntime) m.invoke(null, initConfig);
      m.setAccessible(false);
      return runtime;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
