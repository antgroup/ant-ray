package org.ray.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.Method;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.util.RayConfig;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  @Override
  public RayRuntime createRayRuntime() {

    // Create a rayConfig object.
    final String defaultConfigFile = "ray.default.conf";
    final String customConfigFile = "ray.conf";
    Config config = ConfigFactory.load(defaultConfigFile)
                        .withFallback(ConfigFactory.load(customConfigFile));
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
