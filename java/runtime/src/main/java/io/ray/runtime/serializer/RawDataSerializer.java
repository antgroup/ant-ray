package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import io.ray.api.serializer.RaySerializer2Interface;
import javafx.util.Pair;

public class RawDataSerializer implements RaySerializer2Interface {

  @Override
  public Pair<Object, Object> serialize(Object obj) {
    Preconditions.checkState(obj instanceof byte[]);
    /// TODO(qwang): 如何处理out of band
//    return new Pair<>(/*inband=*/null, /*oob=*/new OutOfBandBuffer(obj));
    return null;
  }

  @Override
  public <T> T deserialize(Class<T> expectedType, byte[] data) {
    return null;
  }

}
