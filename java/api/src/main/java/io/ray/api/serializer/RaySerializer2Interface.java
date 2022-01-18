package io.ray.api.serializer;

import javafx.util.Pair;

public interface RaySerializer2Interface {

  // Return in band buffer and out of band buffer
  // TODO: 这种api如何递归？
  Pair<Object, Object> serialize(Object obj);

  <T> T deserialize(Class<T> expectedType, byte[] data);

}
