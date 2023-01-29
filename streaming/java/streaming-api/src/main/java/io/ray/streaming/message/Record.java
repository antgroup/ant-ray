package io.ray.streaming.message;

import java.io.Serializable;
import java.util.Objects;

public class Record<T> implements Serializable {
  protected String stream;
  protected T value;
  private boolean retract;
  private long traceTimestamp = Long.MIN_VALUE;

  public Record(T value) {
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public boolean isRetract() {
    return retract;
  }

  public void setRetract(boolean retract) {
    this.retract = retract;
  }

  public Record<T> copy() {
    Record<T> record = new Record<>(value);
    record.setStream(stream);
    record.setRetract(retract);
    return record;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Record<?> record = (Record<?>) object;
    return retract == record.retract
        && Objects.equals(stream, record.stream)
        && Objects.equals(value, record.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stream, value, retract);
  }

  @Override
  public String toString() {
    return value.toString();
  }

  public void setTraceTimestamp(long timestamp) {
    traceTimestamp = timestamp;
  }

  public long getTraceTimestamp() {
    return traceTimestamp;
  }
}
