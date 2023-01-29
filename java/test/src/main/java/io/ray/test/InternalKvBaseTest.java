package io.ray.test;

import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class InternalKvBaseTest extends BaseTest {
  public void testGetPutApi() {
    byte[] key = "TEST_KEY".getBytes();
    byte[] value = "TEST_VAL".getBytes();
    byte[] newValue = "NEW_TEST_VAL".getBytes();
    Assert.assertTrue(Ray.internal().kv().put(key, value, false));
    Assert.assertEquals(Ray.internal().kv().get(key), value);
    Assert.assertTrue(Ray.internal().kv().exists(key));
    Assert.assertTrue(!Ray.internal().kv().put(key, newValue, false));
    Assert.assertEquals(Ray.internal().kv().get(key), value);
    Assert.assertTrue(Ray.internal().kv().put(key, newValue, true));
    Assert.assertEquals(Ray.internal().kv().get(key), newValue);
    Assert.assertTrue(Ray.internal().kv().delete(key));
    Assert.assertTrue(!Ray.internal().kv().delete(key));
    Assert.assertEquals(Ray.internal().kv().get(key), null);
    Assert.assertTrue(!Ray.internal().kv().exists(key));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPutWithTooLongArg() {
    Ray.internal().kv().put("key".getBytes(), new byte[5 * 1024 * 1024 + 1], true);
  }
}
