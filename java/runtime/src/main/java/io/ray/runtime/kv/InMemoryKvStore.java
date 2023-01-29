package io.ray.runtime.kv;

import io.ray.api.kv.KvStore;
import io.ray.runtime.util.KvStoreUtil;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class InMemoryKvStore extends KvStore {
  private HashMap<ByteBuffer, byte[]> metaMap = new HashMap<ByteBuffer, byte[]>();

  private HashMap<ByteBuffer, byte[]> metaMapGlobal = new HashMap<ByteBuffer, byte[]>();

  @Override
  public boolean put(byte[] key, byte[] value, boolean overwrite, boolean isGlobal) {
    HashMap<ByteBuffer, byte[]> map = getMap(isGlobal);
    KvStoreUtil.checkArgLen(key);
    KvStoreUtil.checkArgLen(value);
    ByteBuffer b = ByteBuffer.wrap(key);
    if (!overwrite && map.containsKey(b)) {
      return false;
    }
    if (overwrite || !map.containsKey(b)) {
      map.put(b, value);
    }
    return true;
  }

  @Override
  public boolean put(byte[] key, byte[] value, boolean overwrite) {
    return put(key, value, overwrite, false);
  }

  @Override
  public byte[] get(byte[] key, boolean isGlobal) {
    HashMap<ByteBuffer, byte[]> map = getMap(isGlobal);
    KvStoreUtil.checkArgLen(key);
    ByteBuffer b = ByteBuffer.wrap(key);
    if (map.containsKey(b)) {
      return map.get(b);
    }
    return null;
  }

  @Override
  public byte[] get(byte[] key) {
    return get(key, false);
  }

  @Override
  public boolean exists(byte[] key, boolean isGlobal) {
    HashMap<ByteBuffer, byte[]> map = getMap(isGlobal);
    KvStoreUtil.checkArgLen(key);
    return map.containsKey(ByteBuffer.wrap(key));
  }

  @Override
  public boolean exists(byte[] key) {
    return exists(key, false);
  }

  @Override
  public boolean delete(byte[] key, boolean isGlobal) {
    HashMap<ByteBuffer, byte[]> map = getMap(isGlobal);
    KvStoreUtil.checkArgLen(key);
    ByteBuffer b = ByteBuffer.wrap(key);
    if (!map.containsKey(b)) {
      return false;
    }
    map.remove(b);
    return true;
  }

  @Override
  public boolean delete(byte[] key) {
    return delete(key, false);
  }

  public HashMap<ByteBuffer, byte[]> getMap(boolean isGlobal) {
    if (isGlobal) {
      return metaMapGlobal;
    }
    return metaMap;
  }
}
