package io.ray.streaming.common.config;

import static org.aeonbits.owner.Config.DisableableFeature.PARAMETER_FORMATTING;

import io.ray.streaming.common.config.converter.LogDirConverter;

public interface LogConfig extends Config {

  String LOG_PATTERN_LAYOUT = "streaming.log.pattern.layout";
  String LOG_PATTERN_CONVERSION = "streaming.log.pattern.conversion";
  String LOG_PATTERN_FILE = "streaming.log.pattern.file";
  String LOG_ENV_KEY_DIR = "streaming.log.env.key.dir";
  String LOG_ENV_KEY_LEVEL = "streaming.log.env.key.level";
  String LOG_ENV_KEY_MAX_BYTES = "streaming.log.env.key.max-bytes";
  String LOG_ENV_KEY_BACKUP_COUNT = "streaming.log.env.key.backup-count";
  String LOG_ROOT_DIR = "streaming.log.root.dir";
  String LOG_LEVEL = "streaming.log.level";
  String LOG_MAX_BYTES = "streaming.log.max-bytes";
  String LOG_BACKUP_COUNT = "streaming.log.backup.count";

  @DefaultValue(value = "org.apache.log4j.PatternLayout")
  @Key(value = LOG_PATTERN_LAYOUT)
  String logLayoutPattern();

  @DisableFeature(PARAMETER_FORMATTING)
  @DefaultValue(value = "%d{yyyy-MM-dd HH:mm:ss,SS} %-5p %c{1} [%t] : %m%n")
  @Key(value = LOG_PATTERN_CONVERSION)
  String logConversionPattern();

  @DisableFeature(PARAMETER_FORMATTING)
  @DefaultValue(value = "streaming_runtime_pid_%s.log")
  @Key(value = LOG_PATTERN_FILE)
  String logFilePattern();

  @DefaultValue(value = "STREAMING_LOG_DIR")
  @Key(value = LOG_ENV_KEY_DIR)
  String logDirEnvKey();

  @DefaultValue(value = "STREAMING_RUNTIME_LOG_LEVEL")
  @Key(value = LOG_ENV_KEY_LEVEL)
  String logLevelEnvKey();

  @DefaultValue(value = "STREAMING_RUNTIME_LOG_MAX_BYTES")
  @Key(value = LOG_ENV_KEY_MAX_BYTES)
  String logMaxBytesEnvKey();

  @DefaultValue(value = "STREAMING_RUNTIME_LOG_BACKUP_COUNT")
  @Key(value = LOG_ENV_KEY_BACKUP_COUNT)
  String logBackupCountEnvKey();

  @DefaultValue(value = "")
  @ConverterClass(LogDirConverter.class)
  @Key(value = LOG_ROOT_DIR)
  String logRootDir();

  @DefaultValue(value = "INFO")
  @Key(value = LOG_LEVEL)
  String logLevel();

  @DefaultValue(value = "524288000")
  @Key(value = LOG_MAX_BYTES)
  int logMaxBytes();

  @DefaultValue(value = "5")
  @Key(value = LOG_BACKUP_COUNT)
  int logBackupCount();
}
