package io.ray.api.kv;

public abstract class KvStore {
  public abstract boolean put(byte[] key, byte[] value, boolean overwrite);

  public abstract boolean put(byte[] key, byte[] value, boolean overwrite, boolean isGlobal);

  public abstract byte[] get(byte[] key);

  public abstract byte[] get(byte[] key, boolean isGlobal);

  public abstract boolean exists(byte[] key);

  public abstract boolean exists(byte[] key, boolean isGlobal);

  public abstract boolean delete(byte[] key);

  public abstract boolean delete(byte[] key, boolean isGlobal);
}
