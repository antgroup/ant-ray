package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.JobId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.gcs.GcsClient;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GcsClientTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--resources={\"A\":8}");
  }

  public static String assertAvailableResources() {
    RayConfig config = TestUtils.getRuntime().getRayConfig();
    Preconditions.checkNotNull(config);
    GcsClient gcsClient = TestUtils.getRuntime().getGcsClient();

    // TODO(qingw): How to avoid this latency of updating HEARTBEAT table?
    try {
      TimeUnit.MILLISECONDS.sleep(100);
    } catch (InterruptedException e) {
      return "failed";
    }

    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              List<NodeInfo> allNodeInfo = gcsClient.getAllNodeInfo();
              return allNodeInfo.size() == 1
                  && allNodeInfo.get(0).availableResources.containsKey("A")
                  && allNodeInfo.get(0).availableResources.get("A").equals(6.0)
                  && allNodeInfo.get(0).totalResources.containsKey("A")
                  && allNodeInfo.get(0).totalResources.get("A").equals(8.0);
            },
            2000));

    return "ok";
  }

  public void testGetAllNodeInfo() {
    RayConfig config = TestUtils.getRuntime().getRayConfig();

    Preconditions.checkNotNull(config);
    GcsClient gcsClient = TestUtils.getRuntime().getGcsClient();
    List<NodeInfo> allNodeInfo = gcsClient.getAllNodeInfo();
    Assert.assertEquals(allNodeInfo.size(), 1);
    Assert.assertEquals(allNodeInfo.get(0).nodeAddress, config.nodeIp);
    Assert.assertTrue(allNodeInfo.get(0).isAlive);
    Assert.assertEquals((double) allNodeInfo.get(0).availableResources.get("A"), 8.0);
    Assert.assertEquals((double) allNodeInfo.get(0).totalResources.get("A"), 8.0);

    ObjectRef<String> obj =
        Ray.task(GcsClientTest::assertAvailableResources).setResource("A", 2.0).remote();
    Assert.assertEquals(obj.get(), "ok");
  }

  @Test
  public void testNextJob() {
    RayConfig config = TestUtils.getRuntime().getRayConfig();
    // The value of job id of this driver in cluster should be 1.
    Assert.assertEquals(config.getJobId(), JobId.fromInt(1));

    GcsClient gcsClient = TestUtils.getRuntime().getGcsClient();
    for (int i = 2; i < 100; ++i) {
      Assert.assertEquals(gcsClient.nextJobId(), JobId.fromInt(i));
    }
  }
}
