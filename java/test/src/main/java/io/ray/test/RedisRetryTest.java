package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.id.UniqueId;
import io.ray.runtime.gcs.RedisClient;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This is the simple integration test for redis client retry mechanism. For more comprehensive
 * tests, we should debug and choose some important points to shutdown redis, like when get resource
 * from jedis pool successfully, then we shutdown redis and test client behavior.
 */
public class RedisRetryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisRetryTest.class);
  private static final String REDIS_ADDRESS = "127.0.0.1:48921";

  private String redisServerPath;
  private Process redisServerProcess;

  private final Map<String, File> tempFiles;

  public RedisRetryTest() {
    tempFiles = new HashMap<>();
  }

  @BeforeClass
  public void setUp() {
    redisServerPath = System.getenv("REDIS_SERVER_EXECUTABLE_PATH");
    if (StringUtils.isEmpty(redisServerPath)) {
      throw new SkipException("Redis server path is not set. Skip test.");
    }
  }

  @AfterMethod
  public void cleanUp() {
    File file = new File(System.getProperty("user.dir") + "/appendonly.aof");
    file.delete();
  }

  @Test
  public void testLoadingReply() {
    Thread[] threads = new Thread[2];
    // Write too many data
    threads[0] = startRedisServer(0);
    RedisClient client = new RedisClient(REDIS_ADDRESS);
    String key = null;
    String value = null;
    // This case might not cover redis `LOADING` state every time. So we writing
    // too many data in redis will make this case appear more probabilistically.
    for (int i = 0; i < 1000; ++i) {
      String k1 = UniqueId.randomId().toString();
      String v1 = UniqueId.randomId().toString();
      if (i == 400) {
        key = k1;
        value = v1;
      }
      client.set(k1, v1);
    }
    killRedisServer();
    // to test
    threads[1] = startRedisServer(3);
    String res1 = client.get(key, null);
    Assert.assertEquals(value, res1);
    killRedisServer();
    joinThreads(threads);
  }

  @Test
  public void testSetRetry() {
    Thread[] threads = new Thread[2];
    threads[0] = startRedisServer(2);
    RedisClient client = new RedisClient(REDIS_ADDRESS);
    Map<String, String> dict1 = new HashMap<>();

    // just generate a random string
    String v1 = UniqueId.randomId().toString();
    String v2 = UniqueId.randomId().toString();

    dict1.put("k1", v1);
    dict1.put("k2", v2);
    String res = client.hmset("dict1".getBytes(), dict1);
    Assert.assertEquals("OK", res);
    killRedisServer();
    threads[1] = startRedisServer(2);
    byte[] actualV1 = client.hget("dict1".getBytes(), "k1".getBytes());
    byte[] actualV2 = client.hget("dict1".getBytes(), "k2".getBytes());
    Assert.assertEquals(actualV1, v1.getBytes());
    Assert.assertEquals(actualV2, v2.getBytes());
    killRedisServer();
    joinThreads(threads);
  }

  @Test
  public void testStringRetry() {
    Thread[] threads = new Thread[3];
    threads[0] = startRedisServer(2);
    RedisClient client = new RedisClient(REDIS_ADDRESS);

    String v1 = UniqueId.randomId().toString();
    String res = client.set("str1", v1);
    Assert.assertEquals("OK", res);

    killRedisServer();
    threads[1] = startRedisServer(2);

    String value1 = client.get("str1", null);
    Assert.assertEquals(v1, value1);

    killRedisServer();
    threads[2] = startRedisServer(2);
    String value2 = client.get("str2", null);
    Assert.assertNull(value2);
    killRedisServer();
    joinThreads(threads);
  }

  // helper methods

  // Start redis server process in `seconds` seconds.
  // It sleeps in another thread.
  private Thread startRedisServer(int seconds) {
    Thread thread =
        new Thread(
            () -> {
              try {
                TimeUnit.SECONDS.sleep(seconds);
              } catch (InterruptedException e) {
                LOGGER.error("Got InterruptedException when sleeping, exit right now.");
                throw new RuntimeException("Got InterruptedException when sleeping.", e);
              }
              // start redis server process
              final List<String> command =
                  ImmutableList.of(
                      redisServerPath,
                      "--loglevel warning",
                      "--port",
                      REDIS_ADDRESS.split(":")[1],
                      "--notify-keyspace-events Kl",
                      "--appendonly yes");

              ProcessBuilder builder = new ProcessBuilder(command);
              try {
                redisServerProcess = builder.start();
              } catch (IOException e) {
                throw new RuntimeException("Failed to start redis server.", e);
              }

              // wait 200ms and check whether the process is alive.
              try {
                TimeUnit.MILLISECONDS.sleep(200);
              } catch (InterruptedException e) {
                LOGGER.error("error", e);
                throw new RuntimeException("Got InterruptedException when sleeping.");
              }

              // TODO(qwang): Just work around.
              if (redisServerProcess == null /*|| !redisServerProcess.isAlive()*/) {
                throw new RuntimeException(
                    "Failed to start redis server: redisServerProcess is "
                        + redisServerProcess.toString());
              }
            });

    thread.start();
    return thread;
  }

  private void killRedisServer() {
    if (redisServerProcess != null && redisServerProcess.isAlive()) {
      redisServerProcess.destroyForcibly();
    }
  }

  private void joinThreads(Thread[] threads) {
    for (int i = 0; i < threads.length; ++i) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage());
      }
    }
  }
}
