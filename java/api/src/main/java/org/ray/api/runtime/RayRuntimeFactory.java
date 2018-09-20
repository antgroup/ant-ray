package org.ray.api.runtime;

import org.ray.api.RayInitConfig;

/**
 * A factory that produces a RayRuntime instance.
 */
public interface RayRuntimeFactory {

  RayRuntime createRayRuntime(RayInitConfig initConfig);
}
