package io.ray.api.exception;

/**
 * Raised when the actor cannot be scheduled. One example is that the node specified through
 * NodeAffinitySchedulingStrategy is dead.
 */
public class RayActorUnschedulableException extends RayException {
  public RayActorUnschedulableException(String message) {
    super(message);
  }

  public RayActorUnschedulableException(String message, Throwable cause) {
    super(message, cause);
  }
}
