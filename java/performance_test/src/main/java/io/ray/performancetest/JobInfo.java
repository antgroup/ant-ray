package io.ray.performancetest;

import com.google.common.base.Preconditions;
import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.call.BaseActorCreator;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.Bundle;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.api.runtimecontext.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobInfo.class);

  public String jobName;
  public List<NodeInfo> nodes;
  private PlacementGroup placementGroup;

  public static JobInfo parseJobInfo(String[] args) {
    for (String arg : args) {
      LOGGER.info("arg: {}", arg);
    }

    JobInfo jobInfo = new JobInfo();
    if (TestUtils.isDevMode()) {
      jobInfo.jobName = "dev-job-name";
    } else {
      Preconditions.checkState(args.length >= 1);
      jobInfo.jobName = args[0];
    }

    Preconditions.checkNotNull(jobInfo.jobName);
    String nodegroupId = System.getenv("namespaceId");
    LOGGER.info("nodegroupId: {}", nodegroupId);

    List<NodeInfo> nodes = Ray.getRuntimeContext().getAllNodeInfoByNodegroup(nodegroupId);
    LOGGER.info("Found {} nodes in cluster.", nodes.size());
    LOGGER.info("CPUs: {}", TestUtils.getCpuNum());

    int bundleSize = nodes.size();
    List<Bundle> bundles = new ArrayList<>();
    int memoryPerBundle = 0;
    if (TestUtils.isDevMode()) {
      memoryPerBundle = 2000;
    } else {
      memoryPerBundle = 40000;
    }
    for (int i = 0; i < bundleSize; i++) {
      Bundle bundle =
          new Bundle.Builder()
              .setCpu((double) TestUtils.getCpuNum() - 2)
              .setMemoryMb(memoryPerBundle)
              .build();
      bundles.add(bundle);
    }

    PlacementGroupCreationOptions option =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_SPREAD)
            .setName("perf_pg")
            .build();
    jobInfo.placementGroup = PlacementGroups.createPlacementGroup(option);
    if (jobInfo.placementGroup.wait(60)) {
      LOGGER.info("Create perf_pg success.");
    } else {
      LOGGER.warn("Create perf_pg fail.");
    }

    jobInfo.nodes = nodes;
    return jobInfo;
  }

  public <T extends BaseActorCreator> T assignActorToNode(
      BaseActorCreator<T> actorCreator, int nodeIndex) {
    if (TestUtils.isDevMode()) {
      return (T) actorCreator;
    }
    Preconditions.checkState(
        nodes.size() > nodeIndex,
        "Node index " + nodeIndex + " is out of range. Total nodes: " + nodes.size() + ".");
    return (T)
        actorCreator
            .setResource("CPU", 1.0)
            .setPlacementGroup(placementGroup, nodeIndex)
            .setMemoryMb(2000);
  }
}
