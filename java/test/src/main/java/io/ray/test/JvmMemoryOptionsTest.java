package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class JvmMemoryOptionsTest {

  @AfterMethod
  public void shutdown() {
    Ray.shutdown();
  }

  public void testSetXmnWithoutXmx() {
    System.setProperty("ray.job.jvm-options.0", "-xmn500m");

    try {
      Ray.init();
      Assert.fail();
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      Assert.assertEquals(cause.getClass(), IllegalStateException.class);
    } finally {
      Ray.shutdown();
    }
  }

  public static class MyActor {
    public int getPid() {
      return SystemUtil.pid();
    }

    public long getMaxMemory() {
      return Runtime.getRuntime().maxMemory();
    }
  }

  public void testBasicFullOptions() {
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "1000");
    System.setProperty("ray.job.num-initial-java-worker-processes", "0");
    System.setProperty("ray.job.total-memory-mb", "4096");
    System.setProperty("ray.job.max-total-memory-mb", "4096");

    System.setProperty("ray.job.jvm-options.0", "-Xmx500m");
    System.setProperty("ray.job.jvm-options.1", "-Xms300m");
    System.setProperty("ray.job.jvm-options.2", "-Xmn200m");
    System.setProperty("ray.job.jvm-options.3", "-Xss100m");
    System.setProperty("ray.job.jvm-options.4", "-XX:MaxDirectMemorySize=100m");

    Ray.init();
    ActorHandle<MyActor> actor1 = Ray.actor(MyActor::new).remote();
    ActorHandle<MyActor> actor2 = Ray.actor(MyActor::new).remote();

    int pid1 = actor1.task(MyActor::getPid).remote().get();
    int pid2 = actor2.task(MyActor::getPid).remote().get();
    Assert.assertEquals(pid1, pid2);
  }

  public void testBothSetPerJobOptionsAndActorOptions() {
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "1000");
    System.setProperty("ray.job.num-initial-java-worker-processes", "0");
    System.setProperty("ray.job.total-memory-mb", "4096");
    System.setProperty("ray.job.max-total-memory-mb", "4096");

    System.setProperty("ray.job.jvm-options.0", "-Xmx500m");
    System.setProperty("ray.job.jvm-options.1", "-Xms300m");
    System.setProperty("ray.job.jvm-options.2", "-Xmn200m");
    System.setProperty("ray.job.jvm-options.3", "-Xss100m");
    System.setProperty("ray.job.jvm-options.4", "-XX:MaxDirectMemorySize=100m");

    Ray.init();
    ActorHandle<MyActor> actor1 = Ray.actor(MyActor::new).remote();
    ActorHandle<MyActor> actor2 =
        Ray.actor(MyActor::new)
            .setMemoryMb(1500)
            .setJvmOptions(ImmutableList.of("-Xmx1200m", "-Xms1000m", "-Xmn800m"))
            .remote();

    int pid1 = actor1.task(MyActor::getPid).remote().get();
    int pid2 = actor2.task(MyActor::getPid).remote().get();
    Assert.assertNotEquals(pid1, pid2);

    long maxMemory1 = actor1.task(MyActor::getMaxMemory).remote().get();
    long maxMemory2 = actor2.task(MyActor::getMaxMemory).remote().get();
    Assert.assertTrue(maxMemory1 < 500 * 1024 * 1024);
    Assert.assertTrue(maxMemory2 > 500 * 1024 * 1024);
  }

  /// This tests the following case:
  /// 1. The per job xmx is set to 2000M
  /// 2. The memory mb of actor is set to 1500M
  /// 3. The xmx of the actor is set to 1200M
  ///
  /// This case should be pass because it's reasonable.
  public void testComplexCase1() {
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "2200");
    System.setProperty("ray.job.num-initial-java-worker-processes", "0");
    System.setProperty("ray.job.total-memory-mb", "4096");

    System.setProperty("ray.job.jvm-options.0", "-Xmx2000m");
    System.setProperty("ray.job.jvm-options.1", "-Xms300m");
    System.setProperty("ray.job.jvm-options.2", "-Xmn200m");
    System.setProperty("ray.job.jvm-options.3", "-Xss100m");
    System.setProperty("ray.job.jvm-options.4", "-XX:MaxDirectMemorySize=100m");

    Ray.init();
    ActorHandle<MyActor> actor2 =
        Ray.actor(MyActor::new)
            .setMemoryMb(1500)
            .setJvmOptions(ImmutableList.of("-Xmx1200m", "-Xms1000m", "-Xmn800m"))
            .remote();

    long maxMemory2 = actor2.task(MyActor::getMaxMemory).remote().get();
    Assert.assertTrue(maxMemory2 > 500 * 1024 * 1024);
  }

  public void testSetActorOptionsOnly() {
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "1000");
    System.setProperty("ray.job.num-initial-java-worker-processes", "0");
    System.setProperty("ray.job.total-memory-mb", "4096");
    System.setProperty("ray.job.max-total-memory-mb", "4096");

    Ray.init();

    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This case is only supported in gcs scheduling.");
    }

    ActorHandle<MyActor> actor =
        Ray.actor(MyActor::new)
            .setJvmOptions(ImmutableList.of("-Xmx2G", "-Xms2G", "-Xmn800m"))
            .setMemoryMb(2100)
            .remote();

    long maxMemory = actor.task(MyActor::getMaxMemory).remote().get();
    // Note that: 1680m = 2100m * 0.8
    Assert.assertTrue(
        maxMemory > 1680 * 1024 * 1024, String.format("The maxMemory is %d", maxMemory));
  }

  /** This case tests xmx > java-worker-process-default-memory-mb in JobConfig. */
  public void testInvalidJobConfigMemoryOptions() {
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "800");
    System.setProperty("ray.job.num-initial-java-worker-processes", "0");
    System.setProperty("ray.job.total-memory-mb", "4096");
    System.setProperty("ray.job.max-total-memory-mb", "4096");
    System.setProperty("ray.job.jvm-options.0", "-Xmx1000m");
    System.setProperty("ray.job.jvm-options.1", "-Xms300m");
    System.setProperty("ray.job.jvm-options.2", "-Xmn200m");
    System.setProperty("ray.job.jvm-options.3", "-Xss100m");
    System.setProperty("ray.job.jvm-options.4", "-XX:MaxDirectMemorySize=100m");

    try {
      Ray.init();
      ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();
      long maxMemory = actor.task(MyActor::getMaxMemory).remote().get();
      Assert.assertTrue(maxMemory > 500 * 1024 * 1024);
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
    }
  }

  /** This case tests xmx > memorymb set in creating actor. */
  public void testConfilictBetweenJobConfigOptionsAndActorCreatingOptions() {
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "1200");
    System.setProperty("ray.job.num-initial-java-worker-processes", "0");
    System.setProperty("ray.job.total-memory-mb", "4096");
    System.setProperty("ray.job.max-total-memory-mb", "4096");

    System.setProperty("ray.job.jvm-options.0", "-Xmx1000m");
    System.setProperty("ray.job.jvm-options.1", "-Xms300m");
    System.setProperty("ray.job.jvm-options.2", "-Xmn200m");
    System.setProperty("ray.job.jvm-options.3", "-Xss100m");
    System.setProperty("ray.job.jvm-options.4", "-XX:MaxDirectMemorySize=100m");

    Ray.init();
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This case does not work under new gcs task scheduling mode.");
    }

    try {
      ActorHandle<MyActor> actor = Ray.actor(MyActor::new).setMemoryMb(800).remote();
      long maxMemory = actor.task(MyActor::getMaxMemory).remote().get();
      Assert.assertTrue(maxMemory > 500 * 1024 * 1024);
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
    } finally {
      Ray.shutdown();
    }
  }

  /** This case tests xmx > memorymb set in creating actor. */
  public void testDynamic() {
    System.setProperty("ray.job.java-worker-process-default-memory-mb", "500");
    System.setProperty("ray.job.total-memory-mb", "4096");
    System.setProperty("ray.job.max-total-memory-mb", "4096");
    System.setProperty("ray.job.jvm-options.0", "-Xmx500m");
    System.setProperty("ray.job.jvm-options.1", "-Xms300m");
    System.setProperty("ray.job.jvm-options.2", "-Xmn200m");
    System.setProperty("ray.job.jvm-options.3", "-Xss100m");
    System.setProperty("ray.job.jvm-options.4", "-XX:MaxDirectMemorySize=100m");

    Ray.init();
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This case does not work under new gcs task scheduling mode.");
    }
    try {
      ActorHandle<MyActor> actor =
          Ray.actor(MyActor::new)
              .setMemoryMb(1300)
              .setJvmOptions(ImmutableList.of("-Xmx1500m", "-Xms1000m", "-Xmn800m"))
              .remote();
      long maxMemory = actor.task(MyActor::getMaxMemory).remote().get();
      Assert.assertTrue(maxMemory > 500 * 1024 * 1024);
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
    } finally {
      Ray.shutdown();
    }
  }
}
