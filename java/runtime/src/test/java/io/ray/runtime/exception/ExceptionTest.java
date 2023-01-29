package io.ray.runtime.functionmanager;

import io.ray.runtime.exception.RayTaskException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ExceptionTest {

  public void testCatchApiException() {
    try {
      throw new RayTaskException("foo");
    } catch (io.ray.api.exception.RayTaskException e) {
      Assert.assertTrue(e.getMessage() != null && e.getMessage().contains("foo"));
    }
  }

  public void testInstanceOfApiException() {
    try {
      throw new RayTaskException("foo");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof io.ray.api.exception.RayTaskException);
    }
  }
}
