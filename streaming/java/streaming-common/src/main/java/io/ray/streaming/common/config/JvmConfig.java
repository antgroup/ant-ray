package io.ray.streaming.common.config;

public interface JvmConfig extends Config {

  String JVM_OPTS = "streaming.jvm-opts";
  String JVM_OPTS_OVERHEAD = "streaming.jvm-opts.overhead";

  @DefaultValue(value = "")
  @Key(value = JVM_OPTS)
  String jvmOpts();

  /**
   * Default overhead for jvm. Unit is mb. (If -xmx > 8gb，this value should be independently
   * calculated.)
   */
  @DefaultValue(value = "360")
  @Key(value = JVM_OPTS_OVERHEAD)
  int jvmOptsOverhead();
}
