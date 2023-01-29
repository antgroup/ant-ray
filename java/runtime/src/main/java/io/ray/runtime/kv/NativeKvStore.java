package io.ray.runtime.kv;

import io.ray.api.kv.KvStore;
import io.ray.runtime.util.KvStoreUtil;

public class NativeKvStore extends KvStore {
  @Override
  public boolean put(byte[] key, byte[] value, boolean overwrite) {
    return put(key, value, overwrite, false);
  }

  @Override
  public boolean put(byte[] key, byte[] value, boolean overwrite, boolean isGlobal) {
    KvStoreUtil.checkArgLen(key);
    KvStoreUtil.checkArgLen(value);
    return nativePut(key, value, overwrite, isGlobal);
  }

  @Override
  public byte[] get(byte[] key) {
    return get(key, false);
  }

  @Override
  public byte[] get(byte[] key, boolean isGlobal) {
    KvStoreUtil.checkArgLen(key);
    return nativeGet(key, isGlobal);
  }

  @Override
  public boolean exists(byte[] key) {
    return exists(key, false);
  }

  @Override
  public boolean exists(byte[] key, boolean isGlobal) {
    KvStoreUtil.checkArgLen(key);
    return nativeExists(key, isGlobal);
  }

  @Override
  public boolean delete(byte[] key) {
    return delete(key, false);
  }

  @Override
  public boolean delete(byte[] key, boolean isGlobal) {
    KvStoreUtil.checkArgLen(key);
    return nativeDelete(key, isGlobal);
  }

  private static native boolean nativePut(
      byte[] key, byte[] value, boolean overwrite, boolean isGlobal);

  private static native byte[] nativeGet(byte[] key, boolean isGlobal);

  private static native boolean nativeExists(byte[] key, boolean isGlobal);

  private static native boolean nativeDelete(byte[] key, boolean isGlobal);
}
