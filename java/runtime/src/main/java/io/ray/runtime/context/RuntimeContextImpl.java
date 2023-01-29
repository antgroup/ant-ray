package io.ray.runtime.context;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.ActorInfo;
import io.ray.api.runtimecontext.JobInfo;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.api.runtimecontext.ResourceValue;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.util.ResourceUtil;
import io.ray.runtime.util.SystemUtil;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class RuntimeContextImpl implements RuntimeContext {

  private RayRuntimeInternal runtime;

  public RuntimeContextImpl(RayRuntimeInternal runtime) {
    this.runtime = runtime;
  }

  @Override
  public JobId getCurrentJobId() {
    return runtime.getWorkerContext().getCurrentJobId();
  }

  @Override
  public ActorId getCurrentActorId() {
    ActorId actorId = runtime.getWorkerContext().getCurrentActorId();
    Preconditions.checkState(
        actorId != null && !actorId.isNil(), "This method should only be called from an actor.");
    return actorId;
  }

  @Override
  public String getCurrentNodeName() {
    return runtime.getWorkerContext().getCurrentNodeName();
  }

  @Override
  public TaskId getCurrentTaskId() {
    return runtime.getWorkerContext().getCurrentTaskId();
  }

  @Override
  public boolean wasCurrentActorRestarted() {
    if (isSingleProcess()) {
      return false;
    }
    // We now use the `NumRestarts` field to indicate the number of restarts of
    // the actor in Gcs. So it's ok to invoke this method anywhere.
    return runtime.getGcsClient().wasCurrentActorRestarted(getCurrentActorId());
  }

  @Override
  public boolean isSingleProcess() {
    return RunMode.SINGLE_PROCESS == runtime.getRayConfig().runMode;
  }

  @Override
  public List<NodeInfo> getAllNodeInfo() {
    return runtime.getGcsClient().getAllNodeInfo();
  }

  @Override
  public List<NodeInfo> getAllNodeInfoByNamespace(String namespaceId) {
    return runtime.getGcsClient().getAllNodeInfoByNodegroup(namespaceId);
  }

  @Override
  public List<NodeInfo> getAllNodeInfoByNodegroup(String nodegroupId) {
    return runtime.getGcsClient().getAllNodeInfoByNodegroup(nodegroupId);
  }

  @Override
  public List<ActorInfo> getAllActorInfo() {
    return runtime.getGcsClient().getAllActorInfo();
  }

  @Override
  public List<JobInfo> getAllJobInfo() {
    return runtime.getGcsClient().getAllJobInfo();
  }

  @Override
  public String jobWorkingDir() {
    String workingDir = System.getenv("RAY_JOB_DIR");
    // If no such RAY_JOB_DIR env, we return current directory path.
    if (null == workingDir) {
      workingDir = ".";
    }
    return workingDir;
  }

  @Override
  public <T extends BaseActorHandle> T getCurrentActorHandle() {
    return runtime.getActorHandle(getCurrentActorId());
  }

  @Override
  public List<Long> getGpuIds() {

    Map<String, List<ResourceValue>> resourceIds = runtime.getAvailableResourceIds();
    Set<Long> assignedIds = new HashSet<>();
    for (Map.Entry<String, List<ResourceValue>> entry : resourceIds.entrySet()) {
      String pattern = "^GPU_group_[0-9A-Za-z]+$";
      if (entry.getKey().equals("GPU") || Pattern.matches(pattern, entry.getKey())) {
        assignedIds.addAll(
            entry.getValue().stream().map(x -> x.resourceId).collect(Collectors.toList()));
      }
    }
    List<Long> gpuIds;
    List<String> gpuOnThisNode = ResourceUtil.getCudaVisibleDevices();
    if (gpuOnThisNode != null) {
      gpuIds = new ArrayList<>();
      for (Long id : assignedIds) {
        gpuIds.add(Long.valueOf(gpuOnThisNode.get(id.intValue())));
      }
    } else {
      gpuIds = new ArrayList<>(assignedIds);
    }
    return gpuIds;
  }

  @Override
  public String getNamespace() {
    return runtime.getNamespace();
  }

  @Override
  public UniqueId getCurrentNodeId() {
    return runtime.getCurrentNodeId();
  }

  // ANT-INTERNAL
  @Override
  public synchronized String getJobDataDir() {
    String dataDirBase = System.getenv("RAY_JOB_DATA_DIR_BASE");
    if (StringUtils.isEmpty(dataDirBase)) {
      // This static directory needs to be the same as defined in
      // `constants.h` and `runtime_context.py`.
      dataDirBase = "/tmp/ray/data";
    }
    Path dataDir = Paths.get(dataDirBase, String.valueOf(SystemUtil.pid()));
    File file = dataDir.toFile();
    file.mkdirs();
    return dataDir.toString();
  }
}
