package io.ray.streaming.connectors.sls.config;

import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlsProducerConfigGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SlsProducerConfigGenerator.class);

  // Config key.
  private static final String totalSizeInBytes_CONFIG_KEY =
      "streaming.output.sls.config.totalSizeInBytes";
  private static final String maxBlockMs_CONFIG_KEY = "streaming.output.sls.config.maxBlockMs";
  private static final String ioThreadCount_CONFIG_KEY =
      "streaming.output.sls.config.ioThreadCount";
  private static final String batchSizeThresholdInBytes_CONFIG_KEY =
      "streaming.output.sls.config.batchSizeThresholdInBytes";
  private static final String batchCountThreshold_CONFIG_KEY =
      "streaming.output.sls.config.batchCountThreshold";
  private static final String lingerMs_CONFIG_KEY = "streaming.output.sls.config.lingerMs";
  private static final String retries_CONFIG_KEY = "streaming.output.sls.config.retries";
  private static final String maxReservedAttempts_CONFIG_KEY =
      "streaming.output.sls.config.maxReservedAttempts";
  private static final String baseRetryBackoffMs_CONFIG_KEY =
      "streaming.output.sls.config.baseRetryBackoffMs";
  private static final String maxRetryBackoffMs_CONFIG_KEY =
      "streaming.output.sls.config.maxRetryBackoffMs";
  private static final String adjustShardHash_CONFIG_KEY =
      "streaming.output.sls.config.adjustShardHash";
  private static final String buckets_CONFIG_KEY = "streaming.output.sls.config.buckets";

  private static final List<String> allConfig =
      ImmutableList.of(
          totalSizeInBytes_CONFIG_KEY,
          maxBlockMs_CONFIG_KEY,
          ioThreadCount_CONFIG_KEY,
          batchSizeThresholdInBytes_CONFIG_KEY,
          batchCountThreshold_CONFIG_KEY,
          lingerMs_CONFIG_KEY,
          retries_CONFIG_KEY,
          maxReservedAttempts_CONFIG_KEY,
          baseRetryBackoffMs_CONFIG_KEY,
          maxRetryBackoffMs_CONFIG_KEY,
          adjustShardHash_CONFIG_KEY,
          buckets_CONFIG_KEY);

  public static ProducerConfig generateProducerFromJobConfig(
      ProducerConfig config, Map<String, String> jobConfig)
      throws InvocationTargetException, IllegalAccessException {
    LOG.info("Generate `Sls Producer` from job config: {}", jobConfig);

    for (String configKey : allConfig) {
      if (!jobConfig.containsKey(configKey)) {
        continue;
      }

      String[] splits = configKey.split("\\.");
      String methodName =
          String.format("%s%s", "set", toUpperCase4Index(splits[splits.length - 1]));

      // Filter the method mentioned above.
      List<Method> setterMethods =
          Arrays.stream(config.getClass().getDeclaredMethods())
              .filter(x -> x.getName().equals(methodName))
              .collect(Collectors.toList());
      if (setterMethods.isEmpty()) {
        LOG.warn(
            "Can't found the method: {} from the config: {}, will skip it.", methodName, configKey);
        continue;
      }
      Preconditions.checkState(setterMethods.size() == 1);
      Method setterMethod = setterMethods.get(0);

      // This method must only has one parameter!
      Class<?>[] parameterTypes = setterMethod.getParameterTypes();
      if (parameterTypes.length != 1) {
        LOG.warn(
            "The number of the parameters is not correct for method: {}, skip it.", methodName);
        continue;
      }

      // Note: We only consider the three types: int, long, boolean.
      String configValue = jobConfig.get(configKey);
      switch (parameterTypes[0].getSimpleName()) {
        case "int":
          setterMethod.invoke(config, Integer.valueOf(configValue));
          break;
        case "long":
          setterMethod.invoke(config, Long.valueOf(configValue));
          break;
        case "boolean":
          setterMethod.invoke(config, Boolean.valueOf(configValue));
          break;
        default:
          LOG.warn(
              "The parameter type: {} for method: {} is not support currently, skip it.",
              parameterTypes[0].getSimpleName(),
              methodName);
      }
    }
    return config;
  }

  public static String toUpperCase4Index(String string) {
    char[] methodName = string.toCharArray();
    methodName[0] = toUpperCase(methodName[0]);
    return String.valueOf(methodName);
  }

  public static char toUpperCase(char chars) {
    if (97 <= chars && chars <= 122) {
      chars ^= 32;
    }
    return chars;
  }
}
