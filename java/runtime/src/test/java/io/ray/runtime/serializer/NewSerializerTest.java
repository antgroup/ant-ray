package io.ray.runtime.serializer;

import org.testng.annotations.Test;

@Test
public class NewSerializerTest {

  public void testF() {
    RaySerializers raySerializers = new RaySerializers();
    raySerializers.registerSerializer(0, (o) -> o instanceof Integer, MsgPackSerializer.class);
    raySerializers.registerSerializer(1, (o) -> o instanceof Integer, ProtobufSerializer.class);
    raySerializers.registerSerializer(2, (o) -> o instanceof Integer, ArrowTableSerializer.class);
    raySerializers.registerSerializer(3, (o) -> o instanceof Integer, ActorHandleSerializer.class);
  }
}
