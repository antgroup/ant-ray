package org.ray.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.util.RayConfig;
import java.lang.reflect.Method;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  @Override
  public RayRuntime createRayRuntime() {

    // Create a rayConfig object.
    final String DEFAULT_CONFIG_FILE = "ray.default.conf";
    final String CUSTOM_CONFIG_FILE = "ray.conf";
    Config config = ConfigFactory.load(DEFAULT_CONFIG_FILE)
                        .withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
    RayConfig rayConfig = new RayConfig(config);

    try {
      Class clz;
      if (rayConfig.runMode.isNativeRuntime()) {
        clz = Class.forName("org.ray.runtime.RayNativeRuntime");
      } else {
        clz = Class.forName("org.ray.runtime.RayDevRuntime");
      }

      RayRuntime runtime = (RayRuntime) clz.newInstance();
      Method init = clz.getMethod("init", RayConfig.class);
      init.invoke(runtime, rayConfig);

      return runtime;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }

}
