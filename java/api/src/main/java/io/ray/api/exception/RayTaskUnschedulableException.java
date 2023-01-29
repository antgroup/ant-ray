package io.ray.api.exception;

/**
 * Raised when the task cannot be scheduled. One example is that the node specified through
 * NodeAffinitySchedulingStrategy is dead.
 */
public class RayTaskUnschedulableException extends RayException {
  public RayTaskUnschedulableException(String message) {
    super(message);
  }

  public RayTaskUnschedulableException(String message, Throwable cause) {
    super(message, cause);
  }
}
