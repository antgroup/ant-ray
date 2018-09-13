package org.ray.api.runtime;

import org.ray.api.config.RayConfig;

/**
 * A factory that produces a RayRuntime instance.
 */
public interface RayRuntimeFactory {

  RayRuntime createRayRuntime(RayConfig config);
}
