package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.UpdateJobTotalResourcesRequest;
import io.ray.api.WaitResult;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class UpdateJobTotalResourcesTest extends BaseTest {
  private static final int JAVA_WORKER_PROCESS_DEFAULT_MEMORY_MB = 200;
  private static final int TOTAL_MEMORY_MB = 200;

  @BeforeClass
  public void setupJobConfig() {
    System.setProperty(
        "ray.job.java-worker-process-default-memory-mb",
        String.valueOf(JAVA_WORKER_PROCESS_DEFAULT_MEMORY_MB));
    System.setProperty("ray.job.total-memory-mb", String.valueOf(TOTAL_MEMORY_MB));
    System.setProperty("ray.job.max-total-memory-mb", String.valueOf(TOTAL_MEMORY_MB));
  }

  public static class Actor {

    private int value;

    public Actor(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }
  }

  @Test(
      groups = {"cluster"},
      enabled = false)
  public void testUpdateJobTotalResources() {
    // Test that the totalMemoryMb to update must be a multiple of 50.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> UpdateJobTotalResourcesRequest.newBuilder().setTotalMemoryMb(555).build());

    // Test that the UpdateJobTotalResourcesRequest can not be empty.
    Assert.assertThrows(
        IllegalArgumentException.class, () -> UpdateJobTotalResourcesRequest.newBuilder().build());

    // Now this job's total memory is 200 MB, we create an actor that requires 500 MB.
    // So this actor can't be created.
    ActorHandle<Actor> actor = Ray.actor(Actor::new, 1).setMemoryMb(500).remote();
    ObjectRef<Integer> value = actor.task(Actor::getValue).remote();
    List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
    objectRefList.add(value);
    WaitResult<Integer> waitResult = Ray.wait(objectRefList, objectRefList.size(), 3000);
    Assert.assertEquals(waitResult.getReady().size(), 0);
    Assert.assertEquals(waitResult.getUnready().size(), 1);

    // Now update the job's total memory to 500 MB. Check that the above actor is now created.
    UpdateJobTotalResourcesRequest request =
        UpdateJobTotalResourcesRequest.newBuilder().setTotalMemoryMb(500).build();
    Assert.assertTrue(Ray.updateJobTotalResources(request));
    waitResult = Ray.wait(objectRefList, objectRefList.size(), 3000);
    Assert.assertEquals(waitResult.getReady().size(), 1);
    Assert.assertEquals(waitResult.getUnready().size(), 0);

    // Make sure it's ok to update the job's total memory to the job currently used memory
    // (500 MB).
    Assert.assertTrue(Ray.updateJobTotalResources(request));

    // Scale down the total memory of this job to 200M.
    // Check that an exception will be thrown as the resources are still in use.
    request = UpdateJobTotalResourcesRequest.newBuilder().setTotalMemoryMb(200).build();
    boolean success = false;
    try {
      success = Ray.updateJobTotalResources(request);
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
    Assert.assertFalse(success);

    // Resize the memory to [200, 500]M.
    request =
        UpdateJobTotalResourcesRequest.newBuilder()
            .setTotalMemoryMb(200)
            .setMaxTotalMemoryMb(500)
            .build();
    Assert.assertTrue(Ray.updateJobTotalResources(request));
  }
}
