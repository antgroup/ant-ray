package org.ray.runtime;

import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.config.RayConfig;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {
  @Override
  public RayRuntime createRayRuntime() {

    // Create a rayConfig object.
    RayConfig rayConfig = new RayConfig();

    try {
      AbstractRayRuntime runtime;
      if (rayConfig.runMode.isNativeRuntime()) {
        runtime = new RayNativeRuntime();
      } else {
        runtime = new RayDevRuntime();
      }

      runtime.init(rayConfig);
      return runtime;

    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }

  }
}
