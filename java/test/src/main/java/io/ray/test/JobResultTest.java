package io.ray.test;

import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class JobResultTest extends BaseTest {

  public void testJobResult() {
    Ray.internal().putJobResult("abcd");
    Assert.assertEquals(Ray.internal().getJobResult(), "abcd");
    Ray.internal().putJobResult("efgh");
    Assert.assertEquals(Ray.internal().getJobResult(), "efgh");
  }
}
