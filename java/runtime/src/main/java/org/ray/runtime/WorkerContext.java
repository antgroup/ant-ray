package org.ray.runtime;

import org.ray.api.WorkerMode;
import org.ray.api.id.UniqueId;
import org.ray.runtime.task.TaskSpec;

public class WorkerContext {

  /**
   * The mode of this worker.
   */
  private final WorkerMode workerMode;

  /**
   * id of worker.
   */
  private UniqueId workerId = UniqueId.randomId();

  /**
   * current doing task.
   */
  private TaskSpec currentTask;
  /**
   * current app classloader.
   */
  private ClassLoader currentClassLoader;
  /**
   * how many puts done by current task.
   */
  private int currentTaskPutCount;
  /**
   * how many calls done by current task.
   */
  private int currentTaskCallCount;

  public WorkerContext(WorkerMode workerMode, UniqueId driverId) {

    this.workerMode = workerMode;

    // Initialize some member variables.
    currentTask = createDummyTask(driverId);
    currentTaskPutCount = 0;
    currentTaskCallCount = 0;
    currentClassLoader = null;
  }

  public void setWorkerId(UniqueId workerId) {
    this.workerId = workerId;
  }

  public TaskSpec getCurrentTask() {
    return currentTask;
  }

  public int nextPutIndex() {
    return ++currentTaskPutCount;
  }

  public int nextCallIndex() {
    return ++currentTaskCallCount;
  }

  public UniqueId getCurrentWorkerId() {
    return workerId;
  }

  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentTask(TaskSpec currentTask) {
    this.currentTask = currentTask;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    this.currentClassLoader = currentClassLoader;
  }

  private TaskSpec createDummyTask(UniqueId driverId) {
    TaskSpec dummy = new TaskSpec();
    dummy.parentTaskId = UniqueId.NIL;
    if (workerMode == WorkerMode.DRIVER) {
      dummy.taskId = UniqueId.randomId();
    } else {
      dummy.taskId = UniqueId.NIL;
    }
    dummy.actorId = UniqueId.NIL;
    dummy.driverId = driverId;

    return dummy;
  }
}
