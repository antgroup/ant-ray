package org.ray.api.test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.config.RayConfig;

@RunWith(MyRunner.class)
public class RayConfigTest {
  @Test
  public void testDefaultConfFile() {
    final String DEFAULT_CONFIG_FILE = "ray.default.conf";
    final String CUSTOM_CONFIG_FILE = "ray.conf";
    Config config = ConfigFactory.load(CUSTOM_CONFIG_FILE);
    config.withFallback(ConfigFactory.load(DEFAULT_CONFIG_FILE));

    RayConfig rayConfig = new RayConfig(config);
  }

  @Test
  public void testCustomConfFile() {

  }
}
