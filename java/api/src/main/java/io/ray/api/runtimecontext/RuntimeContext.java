package io.ray.api.runtimecontext;

import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import java.util.List;

/** A class used for getting information of Ray runtime. */
public interface RuntimeContext {

  /** Get the current Job ID. */
  JobId getCurrentJobId();

  /** Get current task ID. */
  TaskId getCurrentTaskId();

  /**
   * Get the current actor ID.
   *
   * <p>Note, this can only be called in actors.
   */
  ActorId getCurrentActorId();

  String getCurrentNodeName();

  /**
   * Returns true if the current actor was reconstructed, false if it's created for the first time.
   *
   * <p>Note, this method should only be called from an actor creation task.
   */
  boolean wasCurrentActorRestarted();

  /**
   * Return true if Ray is running in single-process mode, false if Ray is running in cluster mode.
   */
  boolean isSingleProcess();

  /** Get all node information in Ray cluster. */
  List<NodeInfo> getAllNodeInfo();

  /**
   * Get all node information by namespace in Ray cluster.
   *
   * <p>Note, this method is out-dated, use 'getAllNodeInfoByNodegroup' instead.
   */
  @Deprecated
  List<NodeInfo> getAllNodeInfoByNamespace(String namespaceId);

  /** Get all node information by nodegroup in Ray cluster. */
  List<NodeInfo> getAllNodeInfoByNodegroup(String nodegroupId);

  /**
   * Get all actor information of Ray cluster. Note that this will return all actor information of
   * all jobs in this Ray cluster.
   */
  List<ActorInfo> getAllActorInfo();

  /** Get all job information of Ray cluster. */
  List<JobInfo> getAllJobInfo();

  /** Get the job working directory. */
  String jobWorkingDir();

  /**
   * Get the handle to the current actor itself. Note that this method must be invoked in an actor.
   */
  public <T extends BaseActorHandle> T getCurrentActorHandle();

  /** Get available GPU(deviceIds) for this worker. */
  List<Long> getGpuIds();

  /** Get the namespace of this job. */
  String getNamespace();

  // ANT-INTERNAL
  /** Get a job data directory. The directory will be created when this method run first time. */
  String getJobDataDir();

  /** Get current node id. */
  UniqueId getCurrentNodeId();
}
