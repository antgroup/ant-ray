package io.ray.runtime.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResourceUtil {
  // A memory unit represents this amount of memory in bytes.
  public static final long MEMORY_RESOURCE_UNIT_BYTES = 1L;

  public static long toMemoryUnits(long memoryInBytes, boolean roundUp) {
    if (roundUp) {
      return (long) Math.ceil((double) memoryInBytes / MEMORY_RESOURCE_UNIT_BYTES);
    } else {
      return memoryInBytes / MEMORY_RESOURCE_UNIT_BYTES;
    }
  }

  public static boolean isMultipleOfMemoryUnit(long memoryInBytes) {
    // Check if the input memory is a multiple of a memory unit.
    // The `memoryInBytes` should be greater than 0.
    return memoryInBytes > 0
        && toMemoryUnits(memoryInBytes, /*roundUp=*/ false) * MEMORY_RESOURCE_UNIT_BYTES
            == memoryInBytes;
  }

  /**
   * Check whether the amount of memory resource can be converted to unit or not.
   *
   * @param resources The resources that will be checked.
   */
  public static void checkMemoryResourceAndConvertToUnit(Map<String, Double> resources) {
    if (resources.containsKey("memory")) {
      Double memoryByteValue = resources.get("memory");
      if (!isMultipleOfMemoryUnit(memoryByteValue.longValue())) {
        throw new IllegalArgumentException(
            "The value of memory resource ("
                + memoryByteValue
                + ") "
                + "must be multiple of 1 Bytes.");
      } else {
        resources.put("memory", (double) toMemoryUnits(memoryByteValue.longValue(), false));
      }
    }
  }

  /**
   * Get the device IDs in the CUDA_VISIBLE_DEVICES environment variable. The local mode is not
   * support.
   *
   * @return devices (List[String]): If CUDA_VISIBLE_DEVICES is set, returns a list of strings
   *     representing the IDs of the visible GPUs. If it is not set or is set to NoDevFiles, returns
   *     empty list.
   */
  public static List<String> getCudaVisibleDevices() {
    List<String> gpuDevices = new ArrayList<>();
    String gpuIdsStr = System.getenv("CUDA_VISIBLE_DEVICES");
    if (gpuIdsStr == null) {
      return null;
    } else if (gpuIdsStr.isEmpty()) {
      return gpuDevices;
    } else if ("NoDevFiles".equals(gpuIdsStr)) {
      return gpuDevices;
    }
    gpuDevices = Arrays.stream(gpuIdsStr.split(",")).collect(Collectors.toList());
    return gpuDevices;
  }
}
