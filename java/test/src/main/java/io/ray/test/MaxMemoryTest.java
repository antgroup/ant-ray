package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(
    groups = {"cluster"},
    enabled = false)
public class MaxMemoryTest extends BaseTest {

  @BeforeClass
  public void beforeClass() {
    System.setProperty("ray.job.java-heap-fraction", "0.3");
  }

  public static class MyActor {

    public long getMaxMemory() {
      return Runtime.getRuntime().maxMemory();
    }
  }

  public void testInActor() {
    ActorHandle<MyActor> actor = Ray.actor(MyActor::new).setMemoryMb(1000).remote();
    /* Due to GC's survior's space, Runtime.maxMemory() would return a value smaller
     * than the expected one. See the following link for more details.
     * https://stackoverflow.com/questions/23701207/why-do-xmx-and-runtime-maxmemory-not-agree
     * Therefore, the following assertion considers a safety range of 25%.
     */
    long retValue = (long) actor.task(MyActor::getMaxMemory).remote().get();
    long expect = (long) ((double) 1000 * 1024 * 1024 * 0.3);
    Assert.assertTrue(retValue <= expect && retValue >= (long) ((double) expect * 0.75));
  }
}
