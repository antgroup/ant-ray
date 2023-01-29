package io.ray.test;

import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataDirTest extends BaseTest {
  @Test
  public void testDataDir() {
    String dataDir = Ray.getRuntimeContext().getJobDataDir();
    File file = new File(dataDir);
    Assert.assertTrue(file.exists());
    Assert.assertTrue(dataDir != null && dataDir.contains(String.valueOf(SystemUtil.pid())));
    file.delete();
  }
}
