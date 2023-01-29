package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimecontext.JobInfo;
import io.ray.runtime.exception.RayActorException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class MarkJobStateAsFailedTest extends BaseTest {

  private static class MyActor {

    public boolean killJob() {
      Ray.killCurrentJob();
      return true;
    }
  }

  public static void main(String[] args) throws InterruptedException {
    System.setProperty("ray.job.mark_job_state_as_failed_when_killing", "true");
    Ray.init();
    ActorHandle<MyActor> myActor = Ray.actor(MyActor::new).remote();
    try {
      myActor.task(MyActor::killJob).remote().get();
    } catch (RayActorException ignored) {
      // The actor may be killed at this moment.
    }
    // Driver never return.
    TimeUnit.HOURS.sleep(1000);
  }

  public void testKillJob() throws IOException {
    ProcessBuilder builder = TestUtils.buildDriver(MarkJobStateAsFailedTest.class, null, true);
    builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    builder.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process driver = builder.start();

    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              List<JobInfo> allJobInfo = Ray.getRuntimeContext().getAllJobInfo();
              if (allJobInfo.size() != 2) {
                return false;
              }

              for (JobInfo info : allJobInfo) {
                if (info.isDead && info.state == JobInfo.State.FAILED) {
                  return true;
                }
              }
              return false;
            },
            15 * 1000));
    driver.destroyForcibly();
  }
}
