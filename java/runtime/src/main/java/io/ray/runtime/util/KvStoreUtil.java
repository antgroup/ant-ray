package io.ray.runtime.util;

public class KvStoreUtil {
  private static int MAX_ARG_LENGTH = 5 * 1024 * 1024;

  public static void checkArgLen(byte[] arg) {
    if (arg.length > MAX_ARG_LENGTH) {
      throw new IllegalArgumentException("key or value too long");
    }
  }
}
