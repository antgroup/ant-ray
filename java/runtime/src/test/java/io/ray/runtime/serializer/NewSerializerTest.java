package io.ray.runtime.serializer;

import io.ray.api.serializer.SerializedResult;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class NewSerializerTest {

  private SerializerManager serializerManager = new SerializerManager();

  @BeforeMethod
  public void setup() {
    PrimitiveTypesSerializer primitiveTypesSerializer = new PrimitiveTypesSerializer();
    serializerManager.register(Integer.class, primitiveTypesSerializer);
    serializerManager.register(String.class, primitiveTypesSerializer);
    serializerManager.register(byte[].class, new RawDataSerializer());
  }

  public void testF() {
    SerializedResult result = serializerManager.serialize(30);
    serializerManager.writeTo(result, inBandBytes, oob);

    byte[] inBandData = result.getInBandData();

    Assert.assertNotNull(inBandData);
    Object obj = serializerManager.deserialize(inBandData);
    Assert.assertEquals(obj.getClass(), Integer.class);
    Assert.assertEquals(30, obj);
  }

}
