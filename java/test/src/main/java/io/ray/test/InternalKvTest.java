package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.runner.RunManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class InternalKvTest extends BaseTest {

  static byte[] key = "TEST_KEY".getBytes();
  static byte[] value = "TEST_VAL".getBytes();
  static byte[] valueCrossJob = "TEST_VAL_CROSS_JOB".getBytes();
  static byte[] newValue = "NEW_TEST_VAL".getBytes();

  RayConfig rayConfig;

  @BeforeClass
  public void setUp() {
    rayConfig = RayConfig.create();
    RunManager.startRayHead(rayConfig);
    System.setProperty("ray.address", rayConfig.getRedisAddress());
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.address");
    RunManager.stopRay();
  }

  public static boolean taskKv() {
    Assert.assertTrue(Ray.internal().kv().exists(key));
    Assert.assertTrue(Ray.internal().kv().exists(key, true));
    Assert.assertEquals(Ray.internal().kv().get(key), value);
    Assert.assertEquals(Ray.internal().kv().get(key, true), valueCrossJob);
    return true;
  }

  public static class KvActor {
    public boolean taskCheckKv() {
      Assert.assertTrue(Ray.internal().kv().exists(key));
      Assert.assertTrue(Ray.internal().kv().exists(key, true));
      Assert.assertEquals(Ray.internal().kv().get(key), value);
      Assert.assertEquals(Ray.internal().kv().get(key, true), valueCrossJob);
      return true;
    }
  }

  @Test
  public void testKvBaseFeature() {
    Assert.assertTrue(!Ray.internal().kv().exists(key));
    Assert.assertTrue(!Ray.internal().kv().exists(key, true));
    Assert.assertTrue(Ray.internal().kv().put(key, value, false));
    Assert.assertTrue(Ray.internal().kv().put(key, valueCrossJob, false, true));
    Assert.assertEquals(Ray.internal().kv().get(key), value);
    Assert.assertEquals(Ray.internal().kv().get(key, true), valueCrossJob);
    Assert.assertTrue(Ray.internal().kv().exists(key));
    Assert.assertTrue(Ray.internal().kv().exists(key, true));

    // test actor&task call kv api
    Assert.assertTrue(Ray.get(Ray.task(InternalKvTest::taskKv).remote()));
    ActorHandle<KvActor> actor = Ray.actor(KvActor::new).remote();
    Assert.assertTrue(Ray.get(actor.task(KvActor::taskCheckKv).remote()));

    Assert.assertTrue(!Ray.internal().kv().put(key, newValue, false));
    Assert.assertTrue(!Ray.internal().kv().put(key, newValue, false, true));
    Assert.assertEquals(Ray.internal().kv().get(key), value);
    Assert.assertEquals(Ray.internal().kv().get(key, true), valueCrossJob);
    Assert.assertTrue(Ray.internal().kv().put(key, newValue, true));
    Assert.assertTrue(Ray.internal().kv().put(key, newValue, true, true));
    Assert.assertEquals(Ray.internal().kv().get(key), newValue);
    Assert.assertEquals(Ray.internal().kv().get(key, true), newValue);

    // delete key
    Assert.assertTrue(Ray.internal().kv().delete(key));
    Assert.assertTrue(!Ray.internal().kv().delete(key));
    Assert.assertEquals(Ray.internal().kv().get(key), null);
    Assert.assertTrue(!Ray.internal().kv().exists(key));

    Assert.assertTrue(Ray.internal().kv().exists(key, true));
    Assert.assertTrue(Ray.internal().kv().delete(key, true));
    Assert.assertTrue(!Ray.internal().kv().delete(key, true));
    Assert.assertEquals(Ray.internal().kv().get(key, true), null);
    Assert.assertTrue(!Ray.internal().kv().exists(key, true));
  }

  @Test
  public void testMultiJobStep1() {
    Assert.assertTrue(Ray.internal().kv().put(key, value, false));
    Assert.assertTrue(Ray.internal().kv().put(key, valueCrossJob, false, true));
    Assert.assertEquals(Ray.internal().kv().get(key), value);
    Assert.assertEquals(Ray.internal().kv().get(key, true), valueCrossJob);
  }

  @Test
  public void testMultiJobStep2() {
    if (rayConfig.runMode != RunMode.SINGLE_PROCESS) {
      Assert.assertTrue(!Ray.internal().kv().exists(key));
      Assert.assertTrue(Ray.internal().kv().exists(key, true));
      Assert.assertEquals(Ray.internal().kv().get(key), null);
      Assert.assertEquals(Ray.internal().kv().get(key, true), valueCrossJob);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPutWithTooLongValue() {
    Ray.internal().kv().put("key".getBytes(), new byte[5 * 1024 * 1024 + 1], true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPutWithTooLongValue2() {
    Ray.internal().kv().put("key".getBytes(), new byte[5 * 1024 * 1024 + 1], true, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPutWithTooLongKey() {
    Ray.internal().kv().put(new byte[5 * 1024 * 1024 + 1], "value".getBytes(), true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPutWithTooLongKey2() {
    Ray.internal().kv().put(new byte[5 * 1024 * 1024 + 1], "value".getBytes(), true, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetWithTooLongKey() {
    Ray.internal().kv().get(new byte[5 * 1024 * 1024 + 1]);
  }
}
