package io.ray.streaming.common.tuple;

import java.io.Serializable;
import java.util.Arrays;

public abstract class Tuple implements Serializable, Cloneable {
  public static final int MAX_ARITY = 25;
  private static final long serialVersionUID = 1L;
  private static final Class<?>[] CLASSES =
      new Class[] {Tuple0.class, Tuple1.class, Tuple2.class, Tuple3.class, Tuple4.class};

  public Tuple() {}

  public static Class<? extends Tuple> getTupleClass(int arity) {
    if (arity < 0 || arity > MAX_ARITY) {
      throw new IllegalArgumentException("The tuple arity must be in [0, " + MAX_ARITY + "].");
    }
    return (Class<? extends Tuple>) CLASSES[arity];
  }

  public abstract <T> T getField(int var1);

  public <T> T getFieldNotNull(int pos) {
    T field = this.getField(pos);
    if (field != null) {
      return field;
    } else {
      throw new NullPointerException();
    }
  }

  public abstract <T> void setField(T var1, int var2);

  public abstract int getArity();

  public abstract <T extends Tuple> T copy();

  public static String arrayAwareToString(Object o) {
    if (o == null) {
      return "null";
    } else {
      return o.getClass().isArray() ? arrayToString(o) : o.toString();
    }
  }

  public static String arrayToString(Object array) {
    if (array == null) {
      throw new NullPointerException();
    } else if (array instanceof int[]) {
      return Arrays.toString((int[]) array);
    } else if (array instanceof long[]) {
      return Arrays.toString((long[]) array);
    } else if (array instanceof Object[]) {
      return Arrays.toString((Object[]) array);
    } else if (array instanceof byte[]) {
      return Arrays.toString((byte[]) array);
    } else if (array instanceof double[]) {
      return Arrays.toString((double[]) array);
    } else if (array instanceof float[]) {
      return Arrays.toString((float[]) array);
    } else if (array instanceof boolean[]) {
      return Arrays.toString((boolean[]) array);
    } else if (array instanceof char[]) {
      return Arrays.toString((char[]) array);
    } else if (array instanceof short[]) {
      return Arrays.toString((short[]) array);
    } else if (array.getClass().isArray()) {
      return "<unknown array type>";
    } else {
      throw new IllegalArgumentException("The given argument is no array.");
    }
  }
}
