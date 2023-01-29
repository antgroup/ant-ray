package io.ray.runtime;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.api.events.ActorMigrationEvent;
import io.ray.api.events.ActorState;
import io.ray.api.events.ActorStateEvent;
import io.ray.api.exception.RayException;
import io.ray.api.id.ActorId;
import io.ray.api.id.BaseId;
import io.ray.api.id.JobId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.api.kv.KvStore;
import io.ray.api.options.ActorLifetime;
import io.ray.api.runtimecontext.ResourceValue;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.NativeWorkerContext;
import io.ray.runtime.exception.RayIntentionalSystemExitException;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.gcs.GcsClientOptions;
import io.ray.runtime.gcs.RedisClient;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.generated.Gcs.ActorMigrationNotificationBatch;
import io.ray.runtime.generated.Gcs.GcsNodeInfo;
import io.ray.runtime.generated.Gcs.JobConfig;
import io.ray.runtime.kv.NativeKvStore;
import io.ray.runtime.object.NativeObjectStore;
import io.ray.runtime.runner.RunManager;
import io.ray.runtime.task.NativeTaskExecutor;
import io.ray.runtime.task.NativeTaskSubmitter;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import io.ray.runtime.util.ResourceUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Native runtime for cluster mode. */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private boolean startRayHead = false;

  private NativeKvStore kvStore = new NativeKvStore();
  private GcsClient gcsClient;

  /**
   * In Java, GC runs in a standalone thread, and we can't control the exact timing of garbage
   * collection. By using this lock, when {@link NativeObjectStore#nativeRemoveLocalReference} is
   * executing, the core worker will not be shut down, therefore it guarantees some kind of
   * thread-safety. Note that this guarantee only works for driver.
   */
  private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();

  private Thread shutdownHook;

  private static EventManager<ActorStateEvent> actorEventManager = new EventManager<>();
  private static EventManager<ActorMigrationEvent> migrationEventManager = new EventManager<>();

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private void updateSessionDir() {
    if (rayConfig.workerMode == WorkerType.DRIVER) {
      // Fetch session dir from GCS if this is a driver.
      RedisClient client = new RedisClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
      final String sessionDir = client.get("session_dir", null);
      Preconditions.checkNotNull(sessionDir);
      rayConfig.setSessionDir(sessionDir);
    }
  }

  @Override
  public void start() {
    try {
      if (rayConfig.workerMode == WorkerType.DRIVER && rayConfig.getRedisAddress() == null) {
        // Set it to true before `RunManager.startRayHead` so `Ray.shutdown()` can still kill
        // Ray processes even if `Ray.init()` failed.
        startRayHead = true;
        RunManager.startRayHead(rayConfig);
      }
      Preconditions.checkNotNull(rayConfig.getRedisAddress());

      updateSessionDir();

      // Expose ray ABI symbols which may be depended by other shared
      // libraries such as libstreaming_java.so.
      // See BUILD.bazel:libcore_worker_library_java.so
      Preconditions.checkNotNull(rayConfig.sessionDir);
      JniUtils.loadLibrary(rayConfig.sessionDir, BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);

      if (rayConfig.workerMode == WorkerType.DRIVER) {
        GcsNodeInfo nodeInfo = getGcsClient().getNodeToConnectForDriver(rayConfig.nodeIp);
        rayConfig.rayletSocketName = nodeInfo.getRayletSocketName();
        rayConfig.objectStoreSocketName = nodeInfo.getObjectStoreSocketName();
        rayConfig.nodeManagerPort = nodeInfo.getBasicGcsNodeInfo().getNodeManagerPort();
      }

      if (rayConfig.workerMode == WorkerType.DRIVER && rayConfig.getJobId() == JobId.NIL) {
        rayConfig.setJobId(getGcsClient().nextJobId());
      }
      int numWorkersPerProcess =
          rayConfig.workerMode == WorkerType.DRIVER ? 1 : rayConfig.numWorkersPerProcess;

      JobConfig.ActorLifetime defaultActorLifetime = JobConfig.ActorLifetime.NONE;
      if (rayConfig.defaultActorLifetime == ActorLifetime.DETACHED) {
        defaultActorLifetime = JobConfig.ActorLifetime.DETACHED;
      } else if (rayConfig.defaultActorLifetime == ActorLifetime.NON_DETACHED) {
        defaultActorLifetime = JobConfig.ActorLifetime.NON_DETACHED;
      } else {
        Preconditions.checkState(false, "Unknown lifetime {}.", rayConfig.defaultActorLifetime);
      }

      byte[] serializedJobConfig = null;
      if (rayConfig.workerMode == WorkerType.DRIVER) {
        JobConfig.Builder jobConfigBuilder =
            JobConfig.newBuilder()
                .setNumJavaWorkersPerProcess(rayConfig.numWorkersPerProcess)
                .setNumInitialJavaWorkerProcesses(rayConfig.numInitialJavaWorkerProcesses)
                .setJavaWorkerProcessDefaultMemoryUnits(
                    ResourceUtil.toMemoryUnits(
                        rayConfig.javaWorkerProcessDefaultMemoryMb * 1024 * 1024,
                        /*roundUp=*/ false))
                .setTotalMemoryUnits(
                    ResourceUtil.toMemoryUnits(
                        rayConfig.totalMemoryMb * 1024 * 1024, /*roundUp=*/ true))
                .setMaxTotalMemoryUnits(
                    ResourceUtil.toMemoryUnits(
                        rayConfig.maxTotalMemoryMb * 1024 * 1024, /*roundUp=*/ true))
                .setJavaHeapFraction(rayConfig.javaHeapFraction)
                .addAllJvmOptions(rayConfig.jvmOptionsForJavaWorker)
                .addAllCodeSearchPath(rayConfig.codeSearchPath)
                .setRayNamespace(rayConfig.namespace)
                .putAllWorkerEnv(rayConfig.workerEnv)
                .setLongRunning(rayConfig.longRunning)
                .setEnableL1FaultTolerance(rayConfig.enableL1FaultTolerance)
                .setLoggingLevel(rayConfig.logLevel)
                .setActorTaskBackPressureEnabled(rayConfig.actorTaskBackPressureEnabled)
                .setMaxPendingCalls(rayConfig.maxPendingCalls)
                .setSerializedRuntimeEnv(rayConfig.serializedRuntimeEnv)
                .setDefaultActorLifetime(defaultActorLifetime)
                .setMarkJobStateAsFailedWhenKilling(rayConfig.markJobStateAsFailedWHenKilling);
        serializedJobConfig = jobConfigBuilder.build().toByteArray();
      }

      String workerShimPidStr = System.getenv("WORKER_SHIM_PID");
      int workerShimPid = 0;
      if (workerShimPidStr != null) {
        workerShimPid = Integer.parseInt(workerShimPidStr);
      }

      nativeInitialize(
          rayConfig.workerMode.getNumber(),
          rayConfig.nodeIp,
          rayConfig.getNodeManagerPort(),
          rayConfig.workerMode == WorkerType.DRIVER ? System.getProperty("user.dir") : "",
          rayConfig.objectStoreSocketName,
          rayConfig.rayletSocketName,
          (rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL).getBytes(),
          new GcsClientOptions(rayConfig),
          numWorkersPerProcess,
          rayConfig.logDir,
          serializedJobConfig,
          workerShimPid);

      if (rayConfig.workerMode == WorkerType.DRIVER) {
        Preconditions.checkState(shutdownHook == null);
        shutdownHook = new Thread(() -> shutdown(/*fromShutdownHook=*/ true));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
      }

      taskExecutor = new NativeTaskExecutor(this);
      workerContext = new NativeWorkerContext();
      objectStore = new NativeObjectStore(workerContext, shutdownLock);
      taskSubmitter = new NativeTaskSubmitter();

      LOGGER.debug(
          "RayNativeRuntime started with store {}, raylet {}",
          rayConfig.objectStoreSocketName,
          rayConfig.rayletSocketName);
    } catch (Exception e) {
      if (startRayHead) {
        try {
          RunManager.stopRay();
        } catch (Exception e2) {
          // Ignore
        }
      }
      throw e;
    }
  }

  private synchronized void shutdown(boolean fromShutdownHook) {
    // `shutdown` won't be called concurrently, but the lock is also used in `NativeObjectStore`.
    // When an object is garbage collected, the object will be unregistered from core worker.
    // Since GC runs in a separate thread, we need to make sure that core worker is available
    // when `NativeObjectStore` is accessing core worker in the GC thread.
    Lock writeLock = shutdownLock.writeLock();
    writeLock.lock();
    try {
      if (!fromShutdownHook && shutdownHook != null) {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        shutdownHook = null;
      }

      if (rayConfig.workerMode == WorkerType.DRIVER) {
        // We assume that drivers are started by DefaultDriver. In this case, if a Java
        // exception occurs in driver, DefaultDriver will catch it without invoking
        // Ray.shutdown(). If no exception occurs, DefaultDriver will invoke Ray.shutdown()
        // at the end.

        // In summary, if an Java exception occurs in driver, shutdownWithError
        // is true, and core worker will not send an internal disconnect message to Raylet.
        // If an native exception occurs in driver, there's no chance to send an internal
        // disconnect message to Raylet.
        // If driver exits normally, shutdownWithError is false, and an internal disconnect
        // message will be sent to Raylet.

        // If the driver is not started by DefaultDriver, we assume that users will invoke
        // Ray.shutdown() before exiting the driver.

        boolean shutdownWithError = fromShutdownHook;
        nativeShutdown(shutdownWithError);
        if (startRayHead) {
          startRayHead = false;
          RunManager.stopRay();
        }
      }
      if (null != gcsClient) {
        gcsClient.destroy();
        gcsClient = null;
      }
      LOGGER.debug("RayNativeRuntime shutdown");
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void shutdown() {
    shutdown(/*fromShutdownHook=*/ false);
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    Preconditions.checkArgument(Double.compare(capacity, 0) >= 0);
    if (nodeId == null) {
      nodeId = UniqueId.NIL;
    }
    nativeSetResource(resourceName, capacity, nodeId.getBytes());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> Optional<T> getActor(String name, String namespace) {
    if (name.isEmpty()) {
      return Optional.empty();
    }
    byte[] actorIdBytes = nativeGetActorIdOfNamedActor(name, namespace);
    ActorId actorId = ActorId.fromBytes(actorIdBytes);
    if (actorId.isNil()) {
      return Optional.empty();
    } else {
      return Optional.of((T) getActorHandle(actorId));
    }
  }

  @Override
  public KvStore kv() {
    return kvStore;
  }

  @Override
  public void killActor(BaseActorHandle actor, boolean noRestart) {
    nativeKillActor(actor.getId().getBytes(), noRestart);
  }

  @Override
  public Object getAsyncContext() {
    return new AsyncContext(
        workerContext.getCurrentWorkerId(), workerContext.getCurrentClassLoader());
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    nativeSetCoreWorker(((AsyncContext) asyncContext).workerId.getBytes());
    workerContext.setCurrentClassLoader(((AsyncContext) asyncContext).currentClassLoader);
    super.setAsyncContext(asyncContext);
  }

  @Override
  List<ObjectId> getCurrentReturnIds(int numReturns, ActorId actorId) {
    List<byte[]> ret = nativeGetCurrentReturnIds(numReturns, actorId.getBytes());
    return ret.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public void exitActor() {
    if (rayConfig.workerMode != WorkerType.WORKER || runtimeContext.getCurrentActorId().isNil()) {
      throw new RuntimeException("This shouldn't be called on a non-actor worker.");
    }
    LOGGER.info("Actor {} is exiting.", runtimeContext.getCurrentActorId());
    throw new RayIntentionalSystemExitException(
        String.format("Actor %s is exiting.", runtimeContext.getCurrentActorId()));
  }

  @Override
  public GcsClient getGcsClient() {
    if (gcsClient == null) {
      synchronized (this) {
        if (gcsClient == null) {
          gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
        }
      }
    }
    return gcsClient;
  }

  @Override
  public void run() {
    Preconditions.checkState(rayConfig.workerMode == WorkerType.WORKER);
    nativeRunTaskExecutor(taskExecutor);
  }

  @Override
  public Map<String, List<ResourceValue>> getAvailableResourceIds() {
    return nativeGetResourceIds();
  }

  @Override
  public void killCurrentJob() {
    final JobId jobId = Ray.getRuntimeContext().getCurrentJobId();
    final boolean ok = nativeKillJob(jobId.getBytes());
    if (!ok) {
      throw new RayException(String.format("Failed to kill the job %s", jobId));
    }
  }

  @Override
  public String getNamespace() {
    return nativeGetNamespace();
  }

  @Override
  public UniqueId getCurrentNodeId() {
    return new UniqueId(nativeGetCurrentNodeId());
  }

  // ======== ANT-INTERNAL begin ========

  @Override
  public void reportEvent(String severity, String label, String message) {
    nativeReportEvent(severity, label, message);
  }

  @Override
  public boolean updateJobResourceRequirements(
      Map<String, Double> minResourceRequirements, Map<String, Double> maxResourceRequirements) {
    boolean output = false;
    try {
      output =
          getGcsClient()
              .updateJobResourceRequirements(
                  runtimeContext.getCurrentJobId(),
                  minResourceRequirements,
                  maxResourceRequirements);
    } catch (Exception e) {
      throw e;
    }
    return output;
  }

  @Override
  public void putJobResult(String result) {
    getGcsClient()
        .putJobData(runtimeContext.getCurrentJobId(), "RESULT".getBytes(), result.getBytes());
  }

  @Override
  public String getJobResult() {
    return new String(
        getGcsClient().getJobData(runtimeContext.getCurrentJobId(), "RESULT".getBytes()));
  }

  @Override
  public boolean subscribeActorStateChangeEvents(JobId jobId, Consumer<ActorStateEvent> consumer) {
    synchronized (actorEventManager) {
      // We must subscribe in java level first. Or the first batch of event will be ignored.
      boolean ok = actorEventManager.subscribe(jobId, consumer);
      if (!actorEventManager.subscribedFromNative()) {
        // Note we should also subscribe it from JNI to let core worker subscribe it from GCS.
        final boolean subscribedFromNative = nativeSubscribeActorStateChangeEvents();
        actorEventManager.setSubscribedFromNative(subscribedFromNative);
      }
      return ok && actorEventManager.subscribedFromNative();
    }
  }

  @Override
  public boolean subscribeActorMigrationEvents(
      JobId jobId, Consumer<ActorMigrationEvent> consumer) {
    synchronized (actorEventManager) {
      // We must subscribe in java level first. Or the first batch of event will be
      // ignored.
      boolean ok = migrationEventManager.subscribe(jobId, consumer);
      if (!migrationEventManager.subscribedFromNative()) {
        // Note we should also subscribe it from JNI to let core worker subscribe it
        // from GCS.
        final boolean subscribedFromNative = nativeSubscribeActorMigrationEvents();
        migrationEventManager.setSubscribedFromNative(subscribedFromNative);
      }
      return ok && migrationEventManager.subscribedFromNative();
    }
  }

  /// This method will be invoked from native code via JNI.
  public static void onActorStateChanged(String actorIdHexStr, int actorState) {
    ActorId actorId = ActorId.fromBytes(BaseId.hexString2Bytes(actorIdHexStr));
    ActorState state = ActorState.fromValue(actorState);
    LOGGER.info(
        "Got actor state change event: actor id is {} , current state is {} ", actorId, state);
    ActorStateEvent event = new ActorStateEvent(actorId, state);
    actorEventManager.pushEvent(actorId.getJobId(), event);
  }

  /// This method will be invoked from native code via JNI.
  public static void onActorMigrationEvent(byte[] eventBatchPbBytes) {
    try {
      ActorMigrationNotificationBatch notificationBatch =
          ActorMigrationNotificationBatch.parseFrom(eventBatchPbBytes);

      // List<Pair<String, List<ActorID>>>
      // -> Map<JobID, Map<String, Set<ActorID>>>

      Map<JobId, Map<String, Set<ActorId>>> groupedResult =
          notificationBatch.getNotificationListList().stream()
              .flatMap(
                  event ->
                      event.getActorIdSetList().stream()
                          .map(
                              b ->
                                  new ImmutablePair<>(
                                      event.getNodeName().toStringUtf8(),
                                      ActorId.fromBytes(b.toByteArray()))))
              .collect(
                  Collectors.groupingBy(
                      pair -> pair.right.getJobId(),
                      Collectors.groupingBy(
                          pair -> pair.left,
                          Collectors.mapping(pair -> pair.right, Collectors.toSet()))));

      groupedResult.forEach(
          (jobId, map) -> {
            ActorMigrationEvent event = new ActorMigrationEvent();
            event.migrationId = notificationBatch.getMigrationId();
            event.nodeNameGroupedActorId = map;
            migrationEventManager.pushEvent(jobId, event);
          });
    } catch (Exception e) {
      LOGGER.error("Migration event deserialize error!", e);
    }
  }
  // ======== ANT-INTERNAL end ========

  private static native void nativeInitialize(
      int workerMode,
      String ndoeIpAddress,
      int nodeManagerPort,
      String driverName,
      String storeSocket,
      String rayletSocket,
      byte[] jobId,
      GcsClientOptions gcsClientOptions,
      int numWorkersPerProcess,
      String logDir,
      byte[] serializedJobConfig,
      int workerShimPid);

  private static native void nativeRunTaskExecutor(TaskExecutor taskExecutor);

  private static native void nativeShutdown(boolean shutdownWithError);

  private static native void nativeSetResource(String resourceName, double capacity, byte[] nodeId);

  private static native void nativeKillActor(byte[] actorId, boolean noRestart);

  private static native void nativeReportEvent(
      String eventType, String eventSubType, String message);

  private static native Map<String, List<ResourceValue>> nativeGetResourceIds();

  private static native String nativeGetNamespace();

  private static native byte[] nativeGetActorIdOfNamedActor(String actorName, String namespace);

  private static native void nativeSetCoreWorker(byte[] workerId);

  private static native boolean nativeSubscribeActorStateChangeEvents();

  private static native boolean nativeSubscribeActorMigrationEvents();

  private static native List<byte[]> nativeGetCurrentReturnIds(int numReturns, byte[] actorId);

  private static native byte[] nativeGetCurrentNodeId();

  private static native boolean nativeKillJob(byte[] jobId);

  static class AsyncContext {

    public final UniqueId workerId;
    public final ClassLoader currentClassLoader;

    AsyncContext(UniqueId workerId, ClassLoader currentClassLoader) {
      this.workerId = workerId;
      this.currentClassLoader = currentClassLoader;
    }
  }
}
