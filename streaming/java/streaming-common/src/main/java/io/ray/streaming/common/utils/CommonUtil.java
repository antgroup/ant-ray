package io.ray.streaming.common.utils;

import io.ray.streaming.common.config.Config;
import io.ray.streaming.common.enums.ResourceKey;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common tools. */
public class CommonUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);

  public static Map<String, Object> stringMapToObjectMap(Map<String, String> sourceMap) {
    Map<String, Object> resultMap = (Map) sourceMap;
    return resultMap;
  }

  public static int gcd(int a, int b) {
    return a % b == 0 ? b : gcd(b, a % b);
  }

  public static int min(int... nums) {
    int minNum = nums[0];
    for (int i = 1; i < nums.length; i++) {
      minNum = Math.min(minNum, nums[i]);
    }
    return minNum;
  }

  public static Map<String, String> objectMapToStringMap(Map<String, Object> sourceMap) {
    return sourceMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  if (e.getValue() instanceof String) {
                    return (String) e.getValue();
                  } else if (e.getValue() == null) {
                    LOG.warn("Invalid value for key: {}.", e.getKey());
                    return "";
                  } else {
                    return e.getValue().toString();
                  }
                }));
  }

  public static void ignore(Object... args) {}

  public static String getStackMsg(Exception e) {
    StringBuffer sb = new StringBuffer();
    StackTraceElement[] stackArray = e.getStackTrace();

    sb.append(e.getMessage() + "\n");
    for (int i = 0; i < stackArray.length; i++) {
      StackTraceElement element = stackArray[i];
      sb.append(element.toString() + "\n");
    }
    return sb.toString();
  }

  /**
   * Get config keys from config interface.
   *
   * @param clazz config interface
   * @return config keys
   */
  public static List<String> getConfigKeys(Class<? extends Config> clazz) {
    List<String> configKeys = new ArrayList<>();

    try {
      for (Field field : clazz.getFields()) {
        Object value = field.get(null);
        if (value instanceof String) {
          configKeys.add((String) value);
        }
      }
    } catch (Exception e) {
      return null;
    }
    return configKeys;
  }

  /**
   * Print partial barrier content in string format.
   *
   * @param globalCheckpointId global checkpoint id for partial barrier
   * @param partialCheckpointId partial checkpoint id for partial barrier
   * @return string
   */
  public static String genPartialBarrierStr(long globalCheckpointId, long partialCheckpointId) {
    return globalCheckpointId + "-" + partialCheckpointId;
  }

  /**
   * Judge whether the input time is expired. true: time < now false: time >= now
   *
   * @param time target time in long value
   * @return result
   */
  public static boolean isTimeExpired(long time) {
    long now = System.currentTimeMillis();
    int nowTimeLength = Long.toString(now).length();
    int timeLength = Long.toString(time).length();

    if (timeLength < nowTimeLength) {
      time = (long) (time * Math.pow(10, nowTimeLength - timeLength));
    } else if (timeLength > nowTimeLength) {
      time = (long) (time / Math.pow(10, nowTimeLength - timeLength));
    }

    if (time >= now) {
      return false;
    }
    return true;
  }

  /**
   * Format double number into readable string format.
   *
   * @param number the number need to be format
   * @param decimalNum haw many decimals should place
   * @return number after formated
   */
  public static String formatDoubleWithSpecifiedDecimals(Double number, int decimalNum) {
    StringBuffer pattern;
    if (decimalNum <= 0) {
      pattern = new StringBuffer("#");
    } else {
      pattern = new StringBuffer("#.");
      for (int i = 0; i < decimalNum; i++) {
        pattern.append("#");
      }
    }

    DecimalFormat decimalFormat = new DecimalFormat(pattern.toString());
    return decimalFormat.format(number);
  }

  /**
   * Is classA a subclass of classB.
   *
   * @param clazzNameA classA' name in string
   * @param clazzNameB classB' name in string
   * @return result
   */
  public static boolean isSubClazz(String clazzNameA, String clazzNameB) {
    try {
      return (Class.forName(clazzNameB)).isAssignableFrom(Class.forName(clazzNameA));
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Merge multi configs into 1.
   *
   * @param configs multi configs
   * @return 1 config map
   */
  public static Map<String, String> chainConfigs(List<Map<String, String>> configs) {
    Map<String, String> chainedConfigs = new HashMap<>(4);
    configs.forEach(
        config -> {
          for (Map.Entry<String, String> eachConfig : config.entrySet()) {
            chainedConfigs.putIfAbsent(eachConfig.getKey(), eachConfig.getValue());
          }
        });
    return chainedConfigs;
  }

  /**
   * Merge multi resources. CPU, GPU: agg and max MEM: sum
   *
   * @param resources multi resources
   * @return resource
   */
  public static Map<String, Double> chainResources(List<Map<String, Double>> resources) {
    Map<String, Double> chainedResources = new HashMap<>(4);
    resources.forEach(
        resource -> {
          for (Map.Entry<String, Double> unitResource : resource.entrySet()) {
            String resourceKey = unitResource.getKey();
            Double resourceValue = unitResource.getValue();

            if (StringUtils.isEmpty(resourceKey) || resourceValue == null || resourceValue <= 0) {
              continue;
            }

            if (!chainedResources.containsKey(resourceKey)) {
              chainedResources.putIfAbsent(resourceKey, resourceValue);
            } else {
              if (ResourceKey.MEM.name().equals(resourceKey)) {
                // for memory, use the sum value for chained operator
                // e.g. OpA(256mb) chain OpB(512mb) = ChainedOp(768mb)
                Double currentValue = chainedResources.get(resourceKey);
                chainedResources.put(resourceKey, currentValue + resourceValue);
              } else {
                // for CPU and GPU, use the max value for each resource type in chained operator
                // e.g. OpA(1CPU) chain OpB(3CPU) = ChainedOp(3CPU)
                if (chainedResources.get(resourceKey) <= resourceValue) {
                  chainedResources.put(resourceKey, resourceValue);
                }
              }
            }
          }
        });
    return chainedResources;
  }
}
