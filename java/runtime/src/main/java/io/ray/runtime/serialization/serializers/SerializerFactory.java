package io.ray.runtime.serialization.serializers;

import io.ray.runtime.serialization.Fury;

public interface SerializerFactory {

  Serializer createSerializer(Fury fury, Class<?> cls);
}
