package io.ray.streaming.common.config.converter;

import io.ray.streaming.common.enums.EnvironmentType;
import io.ray.streaming.common.utils.EnvUtil;
import java.lang.reflect.Method;
import org.aeonbits.owner.Converter;

public class EnvConverter implements Converter<String> {

  @Override
  public String convert(Method method, String value) {
    if (EnvUtil.isOnlineEnv()) {
      return EnvironmentType.PROD.getName();
    }
    return EnvironmentType.DEV.getName();
  }
}
