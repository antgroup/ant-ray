package io.ray.streaming.common.tuple;

public class Tuple4<T0, T1, T2, T3> extends Tuple {

  private static final long serialVersionUID = 1L;

  public T0 f0;
  public T1 f1;
  public T2 f2;
  public T3 f3;

  public Tuple4() {}

  public Tuple4(T0 value0, T1 value1, T2 value2, T3 value3) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
  }

  public static <T0, T1, T2, T3> Tuple4<T0, T1, T2, T3> of(
      T0 value0, T1 value1, T2 value2, T3 value3) {
    return new Tuple4<T0, T1, T2, T3>(value0, value1, value2, value3);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getField(int pos) {
    switch (pos) {
      case 0:
        return (T) this.f0;
      case 1:
        return (T) this.f1;
      case 2:
        return (T) this.f2;
      case 3:
        return (T) this.f3;
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
      case 1:
        this.f1 = (T1) value;
        break;
      case 2:
        this.f2 = (T2) value;
        break;
      case 3:
        this.f3 = (T3) value;
        break;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  @Override
  public int getArity() {
    return 4;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Tuple4<T0, T1, T2, T3> copy() {
    return new Tuple4<T0, T1, T2, T3>(this.f0, this.f1, this.f2, this.f3);
  }

  public void setFields(T0 value0, T1 value1, T2 value2, T3 value3) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
  }

  @Override
  public int hashCode() {
    int result = f0 != null ? f0.hashCode() : 0;
    result = 31 * result + (f1 != null ? f1.hashCode() : 0);
    result = 31 * result + (f2 != null ? f2.hashCode() : 0);
    result = 31 * result + (f3 != null ? f3.hashCode() : 0);
    return result;
  }

  @SuppressWarnings("EqualsHashCode")
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Tuple4)) {
      return false;
    }
    @SuppressWarnings("rawtypes")
    Tuple4 tuple = (Tuple4) o;
    if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) {
      return false;
    }
    if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) {
      return false;
    }
    if (f2 != null ? !f2.equals(tuple.f2) : tuple.f2 != null) {
      return false;
    }
    if (f3 != null ? !f3.equals(tuple.f3) : tuple.f3 != null) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "("
        + arrayAwareToString(this.f0)
        + ","
        + arrayAwareToString(this.f1)
        + ","
        + arrayAwareToString(this.f2)
        + ","
        + arrayAwareToString(this.f3)
        + ")";
  }

  public T0 getF0() {
    return f0;
  }

  public void setF0(T0 f0) {
    this.f0 = f0;
  }

  public T1 getF1() {
    return f1;
  }

  public void setF1(T1 f1) {
    this.f1 = f1;
  }

  public T2 getF2() {
    return f2;
  }

  public void setF2(T2 f2) {
    this.f2 = f2;
  }

  public T3 getF3() {
    return f3;
  }

  public void setF3(T3 f3) {
    this.f3 = f3;
  }
}
