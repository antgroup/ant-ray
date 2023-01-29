package io.ray.streaming.common.tuple;

public class Tuple1<T0> extends Tuple {

  private static final long serialVersionUID = 1L;

  private T0 f0;

  public Tuple1() {}

  public Tuple1(T0 value0) {
    this.f0 = value0;
  }

  public static <T0> Tuple1<T0> of(T0 value0) {
    return new Tuple1<T0>(value0);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getField(int pos) {
    switch (pos) {
      case 0:
        return (T) this.f0;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void setField(T value, int pos) {
    switch (pos) {
      case 0:
        this.f0 = (T0) value;
        break;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Tuple1<T0> copy() {
    return new Tuple1<T0>(this.f0);
  }

  public void setFields(T0 value0) {
    this.f0 = value0;
  }

  @Override
  public int hashCode() {
    int result = f0 != null ? f0.hashCode() : 0;
    return result;
  }

  @SuppressWarnings("EqualsHashCode")
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Tuple1)) {
      return false;
    }
    @SuppressWarnings("rawtypes")
    Tuple1 tuple = (Tuple1) o;
    if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "(" + this.f0 + ")";
  }

  public T0 getF0() {
    return f0;
  }

  public void setF0(T0 f0) {
    this.f0 = f0;
  }
}
