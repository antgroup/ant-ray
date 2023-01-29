package io.ray.api.exception;

/**
 * Indicates that the actor died unexpectedly before finishing a task.
 *
 * <p>This exception could happen either because the actor process dies while executing a task, or
 * because a task is submitted to a dead actor.
 *
 * @deprecated This class may be removed or renamed in the future.
 */
@Deprecated
public abstract class RayActorException extends RayException {
  public RayActorException(String message) {
    super(message);
  }

  public RayActorException(String message, Throwable cause) {
    super(message, cause);
  }
}
