package org.ray.api.config;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



// TODO(qwang) RayConfig entrance now we keep the old configuration.
// Our next step is moving to Java properties file config,
// which exposes some normal configuration items for users.

public class RayConfig {

  // TODO(qwang) `ray.config.ini` file should be removed later.
  // Search the classpath to find default config file.
  private static final String RAY_CONFIG_INI_DEFAULT = "/ray.config.ini";
  private static final String RAY_CONFIG_DEFAULT = "ray.config.properties";

  private Logger logger = LoggerFactory.getLogger(RayConfig.class);

  private String rayConfigFile = RAY_CONFIG_DEFAULT;
  //TODO(qwang): It(reader) looks like can be a local variable.
  private ConfigReader reader;
  private RayParameters params;
  private PathConfig pathConfig;

  // We store some advanced configuration items here, hide them from users.
  // TODO(qwang): Once the `ray.config.ini` is removed, use this hashMap instead.
  private Properties properties;

  public RayConfig() {
    this(RAY_CONFIG_DEFAULT);
  }

  public RayConfig(String rayConfigFile) {
    this(rayConfigFile, null);
  }

  public RayConfig(String rayConfigFile, String overwrite) {
    logger.debug("current classpath: {}", System.getProperty("java.class.path"));
    this.properties = new Properties();
    try {
      if (rayConfigFile != null) {
        this.rayConfigFile = rayConfigFile;
      }
      // It will search the classpath to find config file, no absolute path.
      properties.load(getClass().getResourceAsStream("/" + this.rayConfigFile));

      if (overwrite != null) {
        Arrays.stream(overwrite.split(";"))
            .map(o -> o.split("="))
            .filter(o -> o.length == 2)
            .forEach(kv -> properties.setProperty(kv[0], kv[1]));
      }
    } catch (IOException e) {
      logger.warn("failed to load rayConfigFile: {}, using empty properties.",
          rayConfigFile, e);
    }
  }

  // TODO(qwang): Remove this once the `ray.config.ini` config is removed.
  // Here overwrite is used to keep compatible with old process start command.
  // Note: We should call `build()` before use rayConfig.
  public RayConfig build() {
    String iniConfigFile = null;
    try {
      String ow = properties.stringPropertyNames().stream()
          .map(k -> k + "=" + properties.getProperty(k))
          .collect(Collectors.joining(";"));

      logger.info("config overwrite: {}", ow);

      iniConfigFile = this.getClass().getResource(RAY_CONFIG_INI_DEFAULT).getFile();
      reader = new ConfigReader(iniConfigFile, ow);
    } catch (Exception e) {
      logger.error("failed to parse iniConfigFile: {}", iniConfigFile, e);
      throw new RuntimeException(e);
    }
    params = new RayParameters(reader);
    pathConfig = new PathConfig(reader);
    return this;
  }

  public String getRayConfigFile() {
    return this.rayConfigFile;
  }

  /**
   * driver or worker should know the redis address
   *
   * @param redisAddr e.g., 127.0.0.1:34222
   * @return an rayConfig
   */
  public RayConfig setRedisAddr(String redisAddr) {
    return set("ray.java.start.redis_address", redisAddr);
  }

  public RayConfig setNodeIpAddr(String nodeIpAddr) {
    return set("ray.java.start.node_ip_address", nodeIpAddr);
  }

  public RayConfig setRedisPort(int redisPort) {
    return set("ray.java.start.redis_port", String.valueOf(redisPort));
  }

  public RayConfig setRunMode(RunMode runMode) {
    return set("ray.java.start.run_mode", runMode.toString());
  }

  public RayConfig setWorkerMode(WorkerMode workerMode) {
    return set("ray.java.start.worker_mode", workerMode.toString());
  }

  public RayParameters getParams() {
    return params;
  }

  public PathConfig getPathConfig() {
    return pathConfig;
  }

  public void setProperity(String key, String value) {
    properties.setProperty(key, value);
  }

  public String getProperity(String key) {
    return properties.getProperty(key);
  }

  // Define some helper methods for config.
  public int getInt(String key) {
    return Integer.valueOf(properties.getProperty(key));
  }

  public int getInt(String key, int defaultValue) {
    try {
      return Integer.valueOf(properties.getProperty(key));
    } catch (NumberFormatException e) {
      logger.warn("use default vaule {} for key: {}", defaultValue, key);
      return defaultValue;
    }
  }

  private RayConfig set(String key, String value) {
    if (value != null) {
      this.setProperity(key, value);
    }
    return this;
  }

}
