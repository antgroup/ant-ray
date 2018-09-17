package org.ray.runtime.config;

import org.ray.runtime.util.config.AConfig;
import org.ray.runtime.util.config.ConfigReader;

/**
 * Path related configurations.
 */
public class PathConfig {

  @AConfig(comment = "additional class path for JAVA",
      defaultArrayIndirectSectionName = "ray.java.path.classes.source")
  public String[] java_class_paths;

  @AConfig(comment = "additional JNI library paths for JAVA",
      defaultArrayIndirectSectionName = "ray.java.path.jni.build")
  public String[] java_jnilib_paths;

  @AConfig(comment = "path to redis-server")
  public String redis_server;

  @AConfig(comment = "path to redis module")
  public String redis_module;

  @AConfig(comment = "path to plasma storage")
  public String store;

  @AConfig(comment = "path to raylet")
  public String raylet;

  public PathConfig(ConfigReader config) {
    if (config.getBooleanValue("ray.java.start", "deploy", false,
        "whether the package is used as a cluster deployment")) {
      config.readObject("ray.java.path.deploy", this, this);
    } else {
      boolean isJar = this.getClass().getResource(this.getClass().getSimpleName() + ".class")
          .getFile().split("!")[0].endsWith(".jar");
      if (isJar) {
        config.readObject("ray.java.path.package", this, this);
      } else {
        config.readObject("ray.java.path.source", this, this);
      }
    }
  }
}
