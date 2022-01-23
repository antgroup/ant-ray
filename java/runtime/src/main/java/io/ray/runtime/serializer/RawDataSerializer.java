package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.serializer.RaySerializer2Interface;
import io.ray.api.serializer.SerializedResult;
//import javafx.util.Pair;

public class RawDataSerializer implements RaySerializer2Interface {

  @Override
  public SerializedResult serialize(Object obj) {
    Preconditions.checkState(obj instanceof byte[]);
    /// TODO(qwang): 如何处理out of band
    return null;
  }

  @Override
  public <T> T deserialize(Class<T> expectedType, byte[] data) {
    return null;
  }

}
