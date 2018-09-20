package org.ray.api.test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.RunMode;
import org.ray.api.WorkerMode;
import org.ray.runtime.config.RayConfig;

@RunWith(MyRunner.class)
public class RayConfigTest {
  @Test
  public void testDefaultConfFile() {
    final String DEFAULT_CONFIG_FILE = "ray.default.conf";
    final String CUSTOM_CONFIG_FILE = "ray.conf";
    Config config = ConfigFactory.load(DEFAULT_CONFIG_FILE)
                        .withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
    RayConfig rayConfig = new RayConfig(config);

    Assert.assertEquals(rayConfig.workerMode, WorkerMode.DRIVER);
    Assert.assertEquals(rayConfig.runMode, RunMode.SINGLE_BOX);
  }

  @Test
  public void testCustomConfFile() {

  }
}
