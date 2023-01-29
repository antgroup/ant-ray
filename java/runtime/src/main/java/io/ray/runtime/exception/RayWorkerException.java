package io.ray.runtime.exception;

import io.ray.api.exception.RayException;

/** Indicates that the worker died unexpectedly while executing a task. */
public class RayWorkerException extends RayException {

  public RayWorkerException() {
    super("The worker died unexpectedly while executing this task.");
  }

  public RayWorkerException(String message) {
    super(message);
  }

  public RayWorkerException(String message, Throwable cause) {
    super(message, cause);
  }
}
