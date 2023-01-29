package io.ray.test;

import io.ray.api.Ray;
import io.ray.api.id.JobId;
import io.ray.api.runtimecontext.JobInfo;
import java.io.IOException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GetJobInfoTest extends BaseTest {

  public void testGetAllJobInfo() throws IOException, InterruptedException {
    List<JobInfo> jobInfos = Ray.getRuntimeContext().getAllJobInfo();
    Assert.assertEquals(jobInfos.size(), 1);
    JobInfo info = jobInfos.get(0);
    Assert.assertEquals(info.jobId, JobId.fromInt(1));
    Assert.assertFalse(info.isDead);

    /// Start a new driver.
    Process newDriver = startDriver();
    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              List<JobInfo> infos = Ray.getRuntimeContext().getAllJobInfo();
              return infos.size() == 2;
            },
            5000));

    jobInfos = Ray.getRuntimeContext().getAllJobInfo();
    Assert.assertEquals(jobInfos.size(), 2);
    Assert.assertTrue(
        jobInfos.stream()
            .anyMatch(jobInfo -> (jobInfo.jobId.equals(JobId.fromInt(1)) && !jobInfo.isDead)));
    Assert.assertTrue(
        jobInfos.stream()
            .anyMatch(jobInfo -> (jobInfo.jobId.equals(JobId.fromInt(2)) && !jobInfo.isDead)));

    newDriver.waitFor();
    /// newDriver is dead now.
    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              List<JobInfo> infos = Ray.getRuntimeContext().getAllJobInfo();
              return infos.size() == 2
                  && infos.stream()
                      .anyMatch(
                          jobInfo -> (jobInfo.jobId.equals(JobId.fromInt(1)) && !jobInfo.isDead))
                  && infos.stream()
                      .anyMatch(
                          jobInfo -> (jobInfo.jobId.equals(JobId.fromInt(2)) && jobInfo.isDead));
            },
            5000));
  }

  private Process startDriver() throws IOException {
    ProcessBuilder builder = TestUtils.buildDriver(GetJobInfoTest.class, null, false);
    builder.redirectError(ProcessBuilder.Redirect.INHERIT);
    return builder.start();
  }

  @SuppressWarnings("all")
  public static void main(String[] args) {
    Ray.init();
    throw new RuntimeException("Ooops");
  }
}
