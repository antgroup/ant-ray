package io.ray.streaming.common.tuple;

import java.io.ObjectStreamException;

public class Tuple0 extends Tuple {

  private static final long serialVersionUID = 1L;

  public static final Tuple0 INSTANCE = new Tuple0();

  @Override
  public <T> T getField(int pos) {
    throw new IndexOutOfBoundsException(String.valueOf(pos));
  }

  @Override
  public <T> void setField(T value, int pos) {
    throw new IndexOutOfBoundsException(String.valueOf(pos));
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Tuple0 copy() {
    return new Tuple0();
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @SuppressWarnings("EqualsHashCode")
  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof Tuple0;
  }

  @Override
  public String toString() {
    return "()";
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
}
