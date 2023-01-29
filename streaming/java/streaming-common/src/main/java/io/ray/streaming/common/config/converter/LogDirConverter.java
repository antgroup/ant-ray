package io.ray.streaming.common.config.converter;

import io.ray.api.Ray;
import io.ray.runtime.RayRuntimeInternal;
import java.lang.reflect.Method;
import org.aeonbits.owner.Converter;

public class LogDirConverter implements Converter<String> {

  private static final String DEFAULT_LOG_DIR = "/tmp/ray_streaming_logs";

  @Override
  public String convert(Method method, String input) {
    if (input.isEmpty()) {
      return Ray.isInitialized()
          ? ((RayRuntimeInternal) Ray.internal()).getRayConfig().logDir
          : DEFAULT_LOG_DIR;
    }
    return input;
  }
}
