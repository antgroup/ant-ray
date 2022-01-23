package io.ray.runtime.serializer;

import io.ray.api.serializer.RaySerializer2Interface;
import io.ray.api.serializer.SerializedResult;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;

public class PrimitiveTypesSerializer implements RaySerializer2Interface {
  @Override
  public SerializedResult serialize(Object obj) {
    try {
      if (obj instanceof Integer) {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packInt((Integer) obj);
        byte[] data = packer.toByteArray();
        SerializedResult result = new SerializedResult();
        result.appendInBandData(data);
        return result;

      } else if (obj instanceof String) {

      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public <T> T deserialize(Class<T> expectedType, byte[] data) {
    return null;
  }
}
