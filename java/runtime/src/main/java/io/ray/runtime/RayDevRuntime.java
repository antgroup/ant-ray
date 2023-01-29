package io.ray.runtime;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import io.ray.api.kv.KvStore;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.runtimecontext.ResourceValue;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.LocalModeWorkerContext;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.generated.Common.TaskSpec;
import io.ray.runtime.kv.InMemoryKvStore;
import io.ray.runtime.object.LocalModeObjectStore;
import io.ray.runtime.task.LocalModeTaskExecutor;
import io.ray.runtime.task.LocalModeTaskSubmitter;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import io.ray.runtime.util.SystemUtil;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayDevRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayDevRuntime.class);

  private AtomicInteger jobCounter = new AtomicInteger(0);

  private InMemoryKvStore kvStore = new InMemoryKvStore();

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }

    updateSessionDir(rayConfig);
    JniUtils.loadLibrary(rayConfig.sessionDir, BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);

    taskExecutor = new LocalModeTaskExecutor(this);
    workerContext = new LocalModeWorkerContext(rayConfig.getJobId());
    objectStore = new LocalModeObjectStore(workerContext);
    taskSubmitter =
        new LocalModeTaskSubmitter(this, taskExecutor, (LocalModeObjectStore) objectStore);
    ((LocalModeObjectStore) objectStore)
        .addObjectPutCallback(
            objectId -> {
              if (taskSubmitter != null) {
                ((LocalModeTaskSubmitter) taskSubmitter).onObjectPut(objectId);
              }
            });
  }

  @Override
  public void run() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean subscribeActorStateChangeEvents(JobId jobId, Consumer consumer) {
    return true;
  }

  @Override
  public boolean subscribeActorMigrationEvents(JobId jobId, Consumer consumer) {
    throw new UnsupportedOperationException();
  }

  public void shutdown() {
    if (objectStore != null) {
      ((LocalModeObjectStore) objectStore).shutdown();
    }
    if (taskSubmitter != null) {
      ((LocalModeTaskSubmitter) taskSubmitter).shutdown();
    }
    taskExecutor = null;
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    LOGGER.error("Not implemented under SINGLE_PROCESS mode.");
  }

  @Override
  public void killActor(BaseActorHandle actor, boolean noRestart) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> Optional<T> getActor(String name, String namespace) {
    return (Optional<T>) ((LocalModeTaskSubmitter) taskSubmitter).getActor(name);
  }

  @Override
  public GcsClient getGcsClient() {
    throw new UnsupportedOperationException("Ray doesn't have gcs client in local mode.");
  }

  @Override
  public KvStore kv() {
    return kvStore;
  }

  @Override
  public Object getAsyncContext() {
    return new AsyncContext(((LocalModeWorkerContext) workerContext).getCurrentTask());
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    Preconditions.checkNotNull(asyncContext);
    TaskSpec task = ((AsyncContext) asyncContext).task;
    ((LocalModeWorkerContext) workerContext).setCurrentTask(task);
    super.setAsyncContext(asyncContext);
  }

  @Override
  List<ObjectId> getCurrentReturnIds(int numReturns, ActorId actorId) {
    return null;
  }

  @Override
  public PlacementGroup getPlacementGroup(PlacementGroupId id) {
    // @TODO(clay4444): We need a LocalGcsClient before implements this.
    throw new UnsupportedOperationException(
        "Ray doesn't support placement group operations in local mode.");
  }

  @Override
  public List<PlacementGroup> getAllPlacementGroups() {
    // @TODO(clay4444): We need a LocalGcsClient before implements this.
    throw new UnsupportedOperationException(
        "Ray doesn't support placement group operations in local mode.");
  }

  @Override
  public String getNamespace() {
    return null;
  }

  @Override
  public void exitActor() {}

  @Override
  public void putJobResult(String result) {
    throw new UnsupportedOperationException("Ray doesn't support put job result in local mode.");
  }

  @Override
  public Map<String, List<ResourceValue>> getAvailableResourceIds() {
    throw new UnsupportedOperationException("Ray doesn't support get resources ids in local mode.");
  }

  @Override
  public String getJobResult() {
    throw new UnsupportedOperationException("Ray doesn't support get job result in local mode.");
  }

  @Override
  public UniqueId getCurrentNodeId() {
    return UniqueId.NIL;
  }

  public void killCurrentJob() {
    throw new UnsupportedOperationException("Ray doesn't support kill current job in local mode.");
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }

  // ============ ANT-INTERNAL below ============

  @Override
  public boolean updateJobResourceRequirements(
      Map<String, Double> minResourceRequirements, Map<String, Double> maxResourceRequirements) {
    throw new UnsupportedOperationException(
        "Ray doesn't support update job resource requirements in local mode.");
  }

  @Override
  public void reportEvent(String severity, String label, String message) {
    LOGGER.info("JAVA worker don't support reportEvent function in the Local Mode.");
  }

  private static class AsyncContext {
    private TaskSpec task;

    private AsyncContext(TaskSpec task) {
      this.task = task;
    }
  }

  private static void updateSessionDir(RayConfig rayConfig) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss-ms");
    Date date = new Date();
    String sessionDir =
        String.format("/tmp/ray/local_mode_session_%s_%d", format.format(date), SystemUtil.pid());
    rayConfig.setSessionDir(sessionDir);
  }
}
