package io.ray.runtime.util;

// DO NOT USE THIS !!!
public class Serializer {

  public static byte[] encode(Object obj) {
    return io.ray.runtime.serializer.Serializer.encode(obj).getLeft();
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs) {
    return io.ray.runtime.serializer.Serializer.decode(bs, Object.class);
  }
}
