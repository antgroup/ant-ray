package io.ray.streaming.common.config;

import io.ray.streaming.common.config.converter.EnvConverter;
import io.ray.streaming.common.config.converter.MaxParallelismConverter;

public interface CommonConfig extends Config {
  /** Kepler */
  String KEPLER_OPERATOR_BATCH_EMIT = "kepler.operator.batch.emit";

  String KEPLER_TX_TABLE = "kepler.tx.table";
  String KEPLER_STORAGE_TABLE = "kepler.storage.table";
  String KEPLER_STORAGE_HBASE_CREATE_REGION_NUM = "kepler.storage.hbase.create.region.num";
  String KEPLER_STORAGE_HBASE_GROUP = "kepler.storage.hbase.group";
  String KEPLER_STORAGE_HBASE_FAMILY = "kepler.storage.hbase.family";
  String KEPLER_STORAGE_HBASE_ZOOKEEPER_QUORUM = "kepler.storage.hbase.zookeeper.quorum";
  String KEPLER_STORAGE_ZOOKEEPER_ZNODE_PARENT = "kepler.storage.zookeeper.znode.parent";
  String KEPLER_STORAGE_HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT =
      "kepler.storage.hbase.zookeeper.property.client.port";
  String KEPLER_STORAGE_HBASE_CLIENT_RETRIES_NUMBER = "kepler.storage.hbase.client.retries.number";
  String KEPLER_STORAGE_VERSION_COUNT = "kepler.storage.version.count";
  String KEPLER_STORAGE_BUFFER_SIZE = "kepler.storage.buffer.size";
  String KEPLER_STORAGE_HBASE_COLUMN = "kepler.storage.hbase.column";
  String KEPLER_STORAGE_HBASE_COLUMN_SIZE = "kepler.storage.hbase.column.size";
  String KEPLER_STORAGE_HBASE_CACHE_SIZE = "kepler.storage.hbase.cache.size";
  String KEPLER_STORAGE__EXCEPTION_HBASE_TABLE = "kepler.storage.exception.table";
  String KEPLER_ENABLE_EXCEPTION_HBASE_STORAGE = "kepler.storage.exception.enable";
  String KEPLER_STORAGE_HBASE_CLIENT_KV_MAXSIZE = "kepler.storage.hbase.client.kv.maxsize";
  String KEPLER_STORAGE_HBASE_CLIENT_WRITE_BUFFER = "kepler.storage.hbase.client.write.buffer";

  /** Realtime(Ray streaming) */
  String STREAMING_RUNNING_MODE = "ray.streaming.running.mode";

  String STREAMING_METRICS_ENABLE = "ray.streaming.metrics.enable";

  String STREAMING_META_TABLE = "ray.streaming.meta.table";

  String CHANNEL_TYPE = "channel_type";
  String MEMORY_CHANNEL = "memory_channel";

  String JOB_NAME = "streaming.job.name";
  String PY_JOB_NAME = "job_name";
  String FILE_ENCODING = "streaming.file.encoding";
  String FILE_SUFFIX_ZIP = "streaming.file.zip.suffix";
  String FILE_SUFFIX_JAR = "streaming.file.jar.suffix";
  String FILE_SUFFIX_CLASS = "streaming.file.class.suffix";
  String ENV_TYPE = "streaming.env";

  String MAX_PARALLELISM = "streaming.job.max.parallelism";

  String ACTOR_ID = "actorId";
  String IP = "ip";
  String HOSTNAME = "hostname";
  String PID = "pid";

  /** This is a fixed value to represent 'engine' type when interacting with the external system. */
  String TYPE = "RayRealtime";

  /** Streaming job name. */
  @DefaultValue(value = "default-job-name")
  @Key(value = JOB_NAME)
  String jobName();

  /** Standard file encoding type in streaming. */
  @DefaultValue(value = "UTF-8")
  @Key(value = FILE_ENCODING)
  String fileEncoding();

  /** Standard suffix character for zip file in streaming. */
  @DefaultValue(value = ".zip")
  @Key(value = FILE_SUFFIX_ZIP)
  String zipFileSuffix();

  /** Standard suffix character for jar file in streaming. */
  @DefaultValue(value = ".jar")
  @Key(value = FILE_SUFFIX_JAR)
  String jarFileSuffix();

  /** Standard suffix character for class file in streaming. */
  @DefaultValue(value = ".class")
  @Key(value = FILE_SUFFIX_CLASS)
  String classFileSuffix();

  @DefaultValue(value = "10")
  @ConverterClass(MaxParallelismConverter.class)
  @Key(value = CommonConfig.MAX_PARALLELISM)
  int maxParallelism();

  @DefaultValue(value = "10")
  int maxParallelismDev();

  @DefaultValue(value = "256")
  int maxParallelismProd();

  @DefaultValue(value = "dev")
  @ConverterClass(EnvConverter.class)
  @Key(value = CommonConfig.ENV_TYPE)
  String env();
}
