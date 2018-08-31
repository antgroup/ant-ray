package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.UniqueID;
import org.ray.api.WaitResult;


@RunWith(MyRunner.class)
public class PlasmaFreeTest {

  @RayRemote
  private static String hello() {
    return "hello";
  }

  @Test
  public void test() {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);
    List<RayObject<String>> waitfor = ImmutableList.of(helloId);
    WaitResult<String> waitResult = Ray.wait(waitfor, 1, 2 * 1000);
    List<RayObject<String>> readyOnes = waitResult.getReadyOnes();
    List<RayObject<String>> remainOnes = waitResult.getRemainOnes();
    Assert.assertEquals(1, readyOnes.size());
    Assert.assertEquals(0, remainOnes.size());

    List<UniqueID> freeList = new ArrayList<>();
    freeList.add(helloId.getId());
    Ray.free(freeList, true);
    // Flush: trigger the release function because Plasma Client has cache.
    for (int i = 0; i < 128; i++) {
      Ray.call(PlasmaFreeTest::hello).get();
    }

    waitResult = Ray.wait(waitfor, 1, 2 * 1000);
    readyOnes = waitResult.getReadyOnes();
    remainOnes = waitResult.getRemainOnes();
    Assert.assertEquals(0, readyOnes.size());
    Assert.assertEquals(1, remainOnes.size());
  }
}
