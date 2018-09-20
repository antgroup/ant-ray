package org.ray.runtime.config;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.ray.api.RunMode;
import org.ray.api.WorkerMode;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.NetworkUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class RayConfig {
  private Logger logger = LoggerFactory.getLogger(RayConfig.class);

  // Configuration fields.
  public final WorkerMode workerMode;
  public RunMode runMode;
  public final String nodeIp;
  ///public final String redisIp;
  ///public final int redisPort;
  public String redisAddress;
  public final UniqueId driverId;
  public final String logDir;
  public final boolean redirectOutput;
  public boolean cleanup;
  public int numberRedisShards;
  public final int defaultFirstCheckTimeoutMs;
  public final int defaultGetCheckIntervalMs;
  public final String jvmParamters;
  public final Long ObjectStoreOccupiedSize;
  public final int rayletPort;
  public final int workerFetchRequestSize;
  //TODO(qwang): This field can be a map.
  public final String staticResources;

  public final String rayHome;
  public final String[] javaClasspaths;
  public final String[] javaJnilibPaths;
  public final String redisServerPath;
  public final String redisModulePath;
  public final String plasmaStorePath;
  public final String rayletPath;
  public final int headRedisPort;
  ////
  public final int objectStoreNameIndex;
  public String objectStoreName;
  public String rayletSocketName;

  public RayConfig(Config config) {
    workerMode = config.getEnum(WorkerMode.class, "ray.worker.mode");
    runMode = config.getEnum(RunMode.class, "ray.run-mode");


    String ip = null;
    try {
      ip = config.getString("ray.node-ip");
    } catch (ConfigException.Missing e) {
      ip = NetworkUtil.getIpAddress(null);
    }
    nodeIp = ip;
    redisAddress = config.getString("ray.redis.address");
    ///String[] tp = redisAddress.split(":");
    ///if (tp.length != 2) {
    ///  throw new RuntimeException("The configuration of redis-address is not correct.");
    ///}
    ///redisIp = tp[0];
    ///redisPort = Integer.valueOf(tp[1]);

    objectStoreName = config.getString("ray.object-store.name");

    UniqueId uniqueId = null;
    try {
      uniqueId = UniqueId.fromHexString(config.getString("ray.driver-id"));
    } catch (ConfigException.Missing e) {
      uniqueId = UniqueId.randomId();
    }
    driverId = uniqueId;

    String dir = null;
    try {
      dir = config.getString("ray.log-dir");
    } catch (ConfigException.Missing e) {
      dir = "/tmp/raylogs";
    }
    logDir = dir;

    headRedisPort = config.getInt("ray.head.redis.port");
    objectStoreNameIndex = config.getInt("ray.object-store.name-index");
    redirectOutput = config.getBoolean("ray.redirect-output");
    cleanup = config.getBoolean("ray.cleanup");
    numberRedisShards = config.getInt("ray.number-redis-shards");
    defaultFirstCheckTimeoutMs = config.getInt("ray.default-first-check-timeout-ms");
    defaultGetCheckIntervalMs = config.getInt("ray.default-get-check-interval-ms");
    jvmParamters = config.getString("ray.jvm-parameters");
    ObjectStoreOccupiedSize = config.getBytes("ray.object-store.occupied-size");
    rayletSocketName = config.getString("ray.raylet.socket-name");
    rayletPort = config.getInt("ray.raylet.port");
    workerFetchRequestSize = config.getInt("ray.worker-fetch-request-size");
    staticResources = config.getString("ray.static-resources");
    rayHome = config.getString("ray.home");


    //TODO(qwang): We should delete the lastest '/'.
    javaClasspaths = new String[2];
    javaClasspaths[0] = rayHome + "/java/test/target/classes";
    javaClasspaths[1] = rayHome + "/java/test/lib/*";

    javaJnilibPaths = new String[2];
    javaJnilibPaths[0] = rayHome + "/build/src/plasma";
    javaJnilibPaths[1] = rayHome + "/build/src/local_scheduler";

    redisServerPath = rayHome + "/build/src/common/thirdparty/redis/src/redis-server";
    redisModulePath = rayHome + "/build/src/common/redis_module/libray_redis_module.so";
    plasmaStorePath = rayHome + "/build/src/plasma/plasma_store_server";
    rayletPath = rayHome + "/build/src/ray/raylet/raylet";
  }
}
