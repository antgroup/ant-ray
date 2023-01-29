package io.ray.runtime.gcs;

import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/** A redis client with retry-mechanism based on jedis. */
public class RedisClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);

  // TODO(qwang): We set this field to 1 to make sure that `retry` work around.
  // Don't change this if you didn't change the `retry` logic.
  private static final int JEDIS_POOL_SIZE = 1;

  private static final int MAX_RETRY_COUNT = 3000;

  private JedisPool jedisPool;

  public RedisClient(String redisAddress) {
    this(redisAddress, null);
  }

  public RedisClient(String redisAddress, String password) {
    String[] ipAndPort = redisAddress.split(":");
    if (ipAndPort.length != 2) {
      throw new IllegalArgumentException(
          "The argument redisAddress " + "should be formatted as ip:port.");
    }

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(JEDIS_POOL_SIZE);

    if (Strings.isNullOrEmpty(password)) {
      jedisPool =
          new JedisPool(jedisPoolConfig, ipAndPort[0], Integer.parseInt(ipAndPort[1]), 5000);
    } else {
      jedisPool =
          new JedisPool(
              jedisPoolConfig, ipAndPort[0], Integer.parseInt(ipAndPort[1]), 5000, password);
    }
  }

  private <R> R retry(Supplier<R> func) {
    int retriedCount = 0;
    while (true) {
      try {
        return func.get();
      } catch (Exception e) {
        LOGGER.error("Got exception when retrying in redis client:", e);

        if (e instanceof JedisConnectionException) {
          LOGGER.debug("Retrying to execute redis command because of getting exception:", e);
        } else if (e instanceof JedisDataException) {
          String errorType = (e.getMessage().split(" "))[0];
          if ("LOADING".equals(errorType)
              || "READONLY".equals(errorType)
              || "ERR REPLICATION LAG".equals(e.getMessage())) {
            LOGGER.debug("Disconnected because of error {}, reconnecting.", e.getMessage());
            try (Jedis jedis = jedisPool.getResource()) {
              jedis.disconnect();
            }
          } else {
            throw new RuntimeException("Failed to retry to redis.", e);
          }
        } else {
          throw new RuntimeException("Failed to retry to redis.", e);
        }

        // sleep
        if (retriedCount <= MAX_RETRY_COUNT) {
          long sleepingTime = 100;
          retriedCount++;
          try {
            Thread.sleep(sleepingTime);
          } catch (InterruptedException ie) {
            LOGGER.error("Got InterruptedException exception when sleeping thread.");
          }
        } else {
          throw new RuntimeException(
              "Couldn't execute command after retrying " + retriedCount + " times.");
        }
      }
    }
  }

  public String set(final String key, final String value) {
    return retry(() -> internalSet(key.getBytes(), value.getBytes()));
  }

  public String set(final byte[] key, final byte[] value) {
    return retry(() -> internalSet(key, value));
  }

  private String internalSet(final byte[] key, final byte[] value) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.set(key, value);
    }
  }

  /**
   * Multi hash value set.
   *
   * @param key the key in redis.
   * @param hash the multi hash value to be set.
   * @return Return OK or Exception if hash is empty.
   */
  public String hmset(byte[] key, Map<String, String> hash) {
    Map<byte[], byte[]> bytesHash = new HashMap<>();
    for (Map.Entry<String, String> entry : hash.entrySet()) {
      bytesHash.put(entry.getKey().getBytes(), entry.getValue().getBytes());
    }
    return retry(() -> internalHmset(key, bytesHash));
  }

  private String internalHmset(byte[] key, Map<byte[], byte[]> hash) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hmset(key, hash);
    }
  }

  public long setnx(final byte[] key, final byte[] value) {
    return retry(() -> internalSetnx(key, value));
  }

  public long internalSetnx(final byte[] key, final byte[] value) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.setnx(key, value);
    }
  }

  /**
   * Get the value of the specified key from State Store.
   *
   * @param key the key to get
   * @param field the field is being got when the item is a hash If it is not hash field should be
   *     filled with null
   * @return Bulk reply If the key does not exist null is returned.
   */
  public String get(final String key, final String field) {
    return retry(() -> internalGet(key, field));
  }

  /**
   * Get the value of the specified key from State Store.
   *
   * @param key The key to get
   * @return Bulk reply If the key does not exist null is returned.
   */
  public byte[] get(final byte[] key) {
    return retry(() -> internalGet(key));
  }

  private String internalGet(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.get(key);
      } else {
        return jedis.hget(key, field);
      }
    }
  }

  private byte[] internalGet(final byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.get(key);
    }
  }

  /** Whether a key exists in Redis. */
  public boolean exists(byte[] key) {
    return retry(() -> internalExists(key));
  }

  private boolean internalExists(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(key);
    }
  }

  public long incr(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.incr(key).intValue();
    }
  }

  public long hset(byte[] key, byte[] field, byte[] value) {
    return retry(() -> internalHset(key, field, value));
  }

  public Map<byte[], byte[]> hgetAll(byte[] key) {
    return retry(() -> internalHgetAll(key));
  }

  public Map<byte[], byte[]> internalHgetAll(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hgetAll(key);
    }
  }

  public long hdel(byte[] key, byte[] field) {
    return retry(() -> internalHdel(key, field));
  }

  public long internalHdel(byte[] key, byte[] field) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hdel(key, field);
    }
  }

  private long internalHset(byte[] key, byte[] field, byte[] value) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hset(key, field, value);
    }
  }

  public byte[] hget(byte[] key, byte[] field) {
    return retry(() -> internalHget(key, field));
  }

  private byte[] internalHget(byte[] key, byte[] field) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hget(key, field);
    }
  }

  public long sadd(String key, String member) {
    return retry(() -> internalSadd(key, member));
  }

  private long internalSadd(String key, String member) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.sadd(key, member);
    }
  }

  public long sremove(String key, String member) {
    return retry(() -> internalSremove(key, member));
  }

  private long internalSremove(String key, String member) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.srem(key, member);
    }
  }

  public Set<String> keys(String prefix) {
    return retry(() -> internalKeys(String.format("%s*", prefix)));
  }

  public Set<String> internalKeys(String prefix) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.keys(prefix);
    }
  }

  public Set<String> smembers(String key) {
    return retry(() -> internalSmembers(key));
  }

  public Set<String> internalSmembers(String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.smembers(key);
    }
  }
}
