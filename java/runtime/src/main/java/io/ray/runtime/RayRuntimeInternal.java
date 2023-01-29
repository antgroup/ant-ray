package io.ray.runtime;

import io.ray.api.events.ActorMigrationEvent;
import io.ray.api.events.ActorStateEvent;
import io.ray.api.id.JobId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtime.RayRuntime;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.object.ObjectStore;
import io.ray.runtime.task.TaskExecutor;
import java.util.function.Consumer;

/** This interface is required to make {@link RayRuntimeProxy} work. */
public interface RayRuntimeInternal extends RayRuntime {

  /** Start runtime. */
  void start();

  WorkerContext getWorkerContext();

  ObjectStore getObjectStore();

  TaskExecutor getTaskExecutor();

  FunctionManager getFunctionManager();

  RayConfig getRayConfig();

  GcsClient getGcsClient();

  void setIsContextSet(boolean isContextSet);

  void run();

  UniqueId getCurrentNodeId();

  // ======== ANT-INTERNAL below ========

  /**
   * Subscribe the actor state change events from Ray.
   *
   * @param jobId The id of the job that we will subscribe on.
   * @param consumer The callback will be invoked when the events triggered.
   * @return True if we succeeded to subscribe it, otherwise false.
   */
  boolean subscribeActorStateChangeEvents(JobId jobId, Consumer<ActorStateEvent> consumer);

  boolean subscribeActorMigrationEvents(JobId jobId, Consumer<ActorMigrationEvent> consumer);
}
