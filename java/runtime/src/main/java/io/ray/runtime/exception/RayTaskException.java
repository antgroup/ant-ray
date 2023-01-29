package io.ray.runtime.exception;

import io.ray.runtime.util.NetworkUtil;
import io.ray.runtime.util.SystemUtil;

public class RayTaskException extends io.ray.api.exception.RayTaskException {

  public RayTaskException(String message) {
    this(message, null);
  }

  public RayTaskException(String message, Throwable cause) {
    super(
        String.format(
            "(pid=%d, ip=%s) %s", SystemUtil.pid(), NetworkUtil.getIpAddress(null), message),
        cause);
  }
}
