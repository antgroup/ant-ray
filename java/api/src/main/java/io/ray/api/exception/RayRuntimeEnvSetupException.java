package io.ray.api.exception;

/** This will be throwed when a runtime environment fails to be set up. */
public class RayRuntimeEnvSetupException extends RayException {
  public RayRuntimeEnvSetupException(String message) {
    super(message);
  }

  public RayRuntimeEnvSetupException(String message, Throwable cause) {
    super(message, cause);
  }
}
