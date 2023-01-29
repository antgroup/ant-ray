package io.ray.streaming.common.serializer;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class SerializerTest {

  @Test
  public void testEncode() {
    List<String> list = new ArrayList<>(10);
    list.add("str1");
    list.add("str2");
    list.add("str3");
    {
      Serializer.setSerializerType("Fury");
      byte[] bytes = Serializer.encode(list);
      Serializer.setSerializerType("Kryo");
      // type id is embedded in data so we can choose serializer in runtime
      assertEquals(Serializer.decode(bytes), list);
    }
    {
      Serializer.setSerializerType("Kryo");
      byte[] bytes = Serializer.encode(list);
      Serializer.setSerializerType("Fury");
      // type id is embedded in data so we can choose serializer in runtime
      assertEquals(Serializer.decode(bytes), list);
    }
  }
}
