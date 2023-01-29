package io.ray.streaming.common.config;

public interface SerializationConfig extends Config {

  String SERIALIZER_TYPE = "streaming.serializer.type";
  String REFERENCE_TRACKING = "streaming.serializer.referenceTracking";

  @DefaultValue(value = "fury")
  @Key(value = SERIALIZER_TYPE)
  String serializerType();

  @DefaultValue(value = "false")
  @Key(value = REFERENCE_TRACKING)
  boolean referenceTracking();
}
