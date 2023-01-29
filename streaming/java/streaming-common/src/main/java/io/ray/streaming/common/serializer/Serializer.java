package io.ray.streaming.common.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.ray.fury.Fury;
import io.ray.fury.util.ForwardSerializer;
import io.ray.fury.util.MemoryBuffer;
import io.ray.fury.util.MemoryUtils;

public class Serializer {
  public static String SERIALIZER_TYPE_KEY = "STREAMING_SERIALIZER";
  private static final byte FURY_TYPE_ID = 0;
  private static final byte KRYO_TYPE_ID = 1;
  private static final ThreadLocal<MemoryBuffer> bufferLocal =
      ThreadLocal.withInitial(() -> MemoryUtils.buffer(32));
  private static volatile ForwardSerializer serializer;
  private static final ForwardSerializer furySerializer =
      new ForwardSerializer(
          new ForwardSerializer.DefaultFuryProxy() {
            @Override
            protected Fury newFurySerializer(ClassLoader loader) {
              // We can register custom serializers here.
              return Fury.builder()
                  .withLanguage(Fury.Language.JAVA)
                  .withReferenceTracking(true)
                  .withClassVersionCheck(true)
                  .withClassLoader(loader)
                  .build();
            }
          });

  private static final ForwardSerializer kryoSerializer =
      new ForwardSerializer(
          new ForwardSerializer.SerializerProxy<Kryo>() {
            private final ThreadLocal<Output> outputLocal =
                ThreadLocal.withInitial(() -> new Output(32, Integer.MAX_VALUE));

            @Override
            protected Kryo newSerializer() {
              // We can register custom serializers here.
              return new Kryo();
            }

            @Override
            protected byte[] serialize(Kryo serializer, Object obj) {
              return KryoUtils.writeToByteArray(obj);
            }

            @Override
            protected Object deserialize(Kryo serializer, byte[] bytes) {
              return KryoUtils.readFromByteArray(bytes);
            }
          });

  static {
    createSerializer();
  }

  private static void createSerializer() {
    String serializerType =
        System.getProperty(SERIALIZER_TYPE_KEY, System.getenv(SERIALIZER_TYPE_KEY));
    if (serializerType == null) {
      serializerType = "Kryo";
    }
    setSerializerType(serializerType);
  }

  public static synchronized void setSerializerType(String serializerType) {
    switch (serializerType) {
      case "Fury":
        serializer = furySerializer;
        break;
      case "Kryo":
        serializer = kryoSerializer;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported serializer type " + serializerType);
    }
  }

  public static byte[] encode(Object obj) {
    MemoryBuffer buffer = bufferLocal.get();
    buffer.writerIndex(0);
    serializer.serialize(obj, buffer);
    // write type id at last to be compatible with rayss
    if (serializer == furySerializer) {
      buffer.writeByte(FURY_TYPE_ID);
    } else {
      buffer.writeByte(KRYO_TYPE_ID);
    }
    return buffer.getBytes(0, buffer.writerIndex());
  }

  public static <T> T decode(byte[] bytes) {
    MemoryBuffer buffer = MemoryUtils.wrap(bytes);
    byte typeId = bytes[bytes.length - 1];
    switch (typeId) {
      case FURY_TYPE_ID:
        return furySerializer.deserialize(buffer.slice(0, bytes.length - 1));
      case KRYO_TYPE_ID:
        return kryoSerializer.deserialize(buffer.slice(0, bytes.length - 1));
      default:
        // NOTICE: Compatible with old serialized content.
        return kryoSerializer.deserialize(bytes);
    }
  }
}
