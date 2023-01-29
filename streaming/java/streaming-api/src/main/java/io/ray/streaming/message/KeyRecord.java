package io.ray.streaming.message;

import java.util.Objects;

public class KeyRecord<K, T> extends Record<T> {

  private K key;

  public KeyRecord(K key, T value) {
    super(value);
    this.key = key;
  }

  public KeyRecord(K key, T value, boolean isRetract) {
    super(value);
    this.key = key;
    setRetract(isRetract);
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public KeyRecord<K, T> copy() {
    KeyRecord<K, T> record = new KeyRecord<>(key, value);
    record.setStream(stream);
    record.setRetract(isRetract());
    return record;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    KeyRecord<?, ?> keyRecord = (KeyRecord<?, ?>) o;
    return Objects.equals(key, keyRecord.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), key);
  }
}
