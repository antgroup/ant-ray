package org.ray.api.test;

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
    RayConfig rayConfig = new RayConfig();

    Assert.assertEquals(rayConfig.workerMode, WorkerMode.DRIVER);
    Assert.assertEquals(rayConfig.runMode, RunMode.SINGLE_BOX);
  }

  @Test
  public void testCustomConfFile() {

  }
}
