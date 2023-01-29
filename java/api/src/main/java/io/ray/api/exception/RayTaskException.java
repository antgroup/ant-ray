package io.ray.api.exception;

/**
 * Indicates that a task threw an exception during execution.
 *
 * <p>If a task throws an exception during execution, a RayTaskException is stored in the object
 * store as the task's output. Then when the object is retrieved from the object store, this
 * exception will be thrown and propagate the error message.
 *
 * @deprecated This class may be removed or renamed in the future.
 */
@Deprecated
public abstract class RayTaskException extends RayException {
  public RayTaskException(String message) {
    super(message);
  }

  public RayTaskException(String message, Throwable cause) {
    super(message, cause);
  }
}
