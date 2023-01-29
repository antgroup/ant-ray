package io.ray.api.exception;

/**
 * This exception could happen either because Ray.get timeout, or because a task operator timeout
 */
public class RayTimeoutException extends RayException {
  public RayTimeoutException(String message) {
    super(message);
  }

  public RayTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
