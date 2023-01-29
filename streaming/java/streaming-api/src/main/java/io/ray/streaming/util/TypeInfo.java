package io.ray.streaming.util;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Used to captures type information.
 *
 * @param <T> Captures the actual type of {@code T}.
 */
public class TypeInfo<T> implements Serializable {
  private final Type type;

  public TypeInfo() {
    type = capture();
  }

  public TypeInfo(Type type) {
    this.type = type;
  }

  public TypeInfo(Class<T> type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  /** Returns the captured type. */
  protected final Type capture() {
    Type superclass = getClass().getGenericSuperclass();
    if (!(superclass instanceof ParameterizedType)) {
      // Class isn't parameterized.
      return Object.class;
    } else {
      return ((ParameterizedType) superclass).getActualTypeArguments()[0];
    }
  }

  @Override
  public boolean equals(Object o) {
    // Subclass of `TypeInfo` should be accepted too. We use `TypeInfo` subclass to capture generics
    if (o instanceof TypeInfo) {
      TypeInfo<?> that = (TypeInfo<?>) o;
      return type.equals(that.type);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return type.hashCode();
  }

  @Override
  public String toString() {
    return type.getTypeName();
  }
}
