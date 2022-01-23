package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import io.ray.api.serializer.RaySerializer2Interface;
import io.ray.api.serializer.SerializedResult;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class SerializerManager {


  private ConcurrentHashMap<Class<?>, RaySerializer2Interface> allSerializers = new ConcurrentHashMap<>();

  public void register(Class<?> type, RaySerializer2Interface serializer) {
    allSerializers.put(type, serializer);
  }


  public SerializedResult serialize(Object obj) {
    if (obj == null) {
      Preconditions.checkState(false, "TODO: add null serializer");
    }

    if (!allSerializers.containsKey(obj.getClass())) {
      Preconditions.checkState(false, "TODO: Unable to serialize");
    }

    RaySerializer2Interface serializer = allSerializers.get(obj.getClass());
    SerializedResult serializedResult = serializer.serialize(obj);
    return serializedResult;
  }

  public Object deserialize(byte[] data) {
    return null;
  }

}
