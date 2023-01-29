package io.ray.streaming.common.config.converter;

import io.ray.streaming.common.config.CommonConfig;
import io.ray.streaming.common.utils.EnvUtil;
import java.lang.reflect.Method;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

public class MaxParallelismConverter implements Converter<Integer> {

  private CommonConfig config = ConfigFactory.create(CommonConfig.class);

  @Override
  public Integer convert(Method method, String value) {
    if (!StringUtils.isEmpty(value)
        && config.maxParallelismProd() != Integer.parseInt(value)
        && config.maxParallelismDev() != Integer.parseInt(value)) {
      return Integer.parseInt(value);
    }
    if (EnvUtil.isOnlineEnv()) {
      return config.maxParallelismProd();
    }
    return config.maxParallelismDev();
  }
}
