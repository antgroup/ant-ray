package io.ray.runtime.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import io.ray.api.id.JobId;
import io.ray.api.options.ActorLifetime;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.util.NetworkUtil;
import io.ray.runtime.util.ResourceUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/** Configurations of Ray runtime. See `ray.default.conf` for the meaning of each field. */
public class RayConfig {
  public static final String DEFAULT_CONFIG_FILE = "ray.default.conf";
  public static final String CUSTOM_CONFIG_FILE = "ray.conf";

  private Config config;

  /** IP of this node. if not provided, IP will be automatically detected. */
  public final String nodeIp;

  public final String nodeName;

  public final WorkerType workerMode;
  public final RunMode runMode;
  private JobId jobId;
  public String sessionDir;
  public String logDir;

  private String redisAddress;
  public final String redisPassword;

  // RPC socket name of object store.
  public String objectStoreSocketName;

  // RPC socket name of Raylet.
  public String rayletSocketName;
  // Listening port for node manager.
  public int nodeManagerPort;

  public final String logLevel;

  public final String rayLogLevel;

  public final ActorLifetime defaultActorLifetime;

  public static class LoggerConf {

    public final String loggerName;

    public final String fileName;

    public final String pattern;

    public LoggerConf(String loggerName, String fileName, String pattern) {
      this.loggerName = loggerName;
      this.fileName = fileName;
      this.pattern = pattern;
    }
  }

  public final List<LoggerConf> loggers;

  public final List<String> codeSearchPath;

  public final List<String> headArgs;

  public final int numWorkersPerProcess;
  public final long javaWorkerProcessDefaultMemoryMb;
  public final int numInitialJavaWorkerProcesses;
  public final long totalMemoryMb;
  public final long maxTotalMemoryMb;
  public final float javaHeapFraction;
  public final boolean longRunning;
  public final boolean enableL1FaultTolerance;
  public final String serializedRuntimeEnv;

  public final boolean returnTaskException;

  public final String namespace;

  public final List<String> jvmOptionsForJavaWorker;
  public final Map<String, String> workerEnv;

  public final boolean actorTaskBackPressureEnabled;
  public final long maxPendingCalls;

  public final boolean markJobStateAsFailedWHenKilling;

  public final boolean gcsTaskSchedulingEnabled;

  private void validate() {
    if (workerMode == WorkerType.WORKER) {
      Preconditions.checkArgument(
          redisAddress != null, "Redis address must be set in worker mode.");
    }

    Preconditions.checkArgument(
        numWorkersPerProcess > 0, "numWorkersPerProcess must be greater than 0.");

    if (workerMode == WorkerType.DRIVER) {
      if (gcsTaskSchedulingEnabled) {
        Preconditions.checkArgument(numInitialJavaWorkerProcesses >= 0);
        Preconditions.checkArgument(
            ResourceUtil.isMultipleOfMemoryUnit(javaWorkerProcessDefaultMemoryMb * 1024 * 1024),
            "javaWorkerProcessDefaultMemoryMb must be a multiple of 50.");

        long minimumMemoryMb =
            Math.max(
                numInitialJavaWorkerProcesses * javaWorkerProcessDefaultMemoryMb,
                ResourceUtil.MEMORY_RESOURCE_UNIT_BYTES / (1024 * 1024));
        Preconditions.checkArgument(
            totalMemoryMb >= minimumMemoryMb,
            "totalMemoryMb should be at least " + String.valueOf(minimumMemoryMb));
      }
    }
  }

  private String removeTrailingSlash(String path) {
    if (path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    } else {
      return path;
    }
  }

  public RayConfig(Config config) {
    this.config = config;
    // Worker mode.
    WorkerType localWorkerMode;
    try {
      localWorkerMode = config.getEnum(WorkerType.class, "ray.worker.mode");
    } catch (ConfigException.Missing e) {
      localWorkerMode = WorkerType.DRIVER;
    }
    workerMode = localWorkerMode;
    boolean isDriver = workerMode == WorkerType.DRIVER;
    // Run mode.
    if (config.hasPath("ray.local-mode")) {
      runMode = config.getBoolean("ray.local-mode") ? RunMode.SINGLE_PROCESS : RunMode.CLUSTER;
    } else {
      runMode = config.getEnum(RunMode.class, "ray.run-mode");
    }
    // Node ip.
    if (config.hasPath("ray.node-ip")) {
      nodeIp = config.getString("ray.node-ip");
    } else {
      nodeIp = NetworkUtil.getIpAddress(null);
    }
    // Node name
    if (config.hasPath("ray.node-name")) {
      nodeName = config.getString("ray.node-name");
    } else {
      nodeName = "";
    }
    // Job id.
    String jobId = config.getString("ray.job.id");
    if (!jobId.isEmpty()) {
      this.jobId = JobId.fromHexString(jobId);
    } else {
      this.jobId = JobId.NIL;
    }

    // Namespace of this job.
    String localNamespace = config.getString("ray.job.namespace");
    if (workerMode == WorkerType.DRIVER) {
      namespace =
          StringUtils.isEmpty(localNamespace) ? UUID.randomUUID().toString() : localNamespace;
    } else {
      /// We shouldn't set it for worker.
      namespace = null;
    }

    defaultActorLifetime = config.getEnum(ActorLifetime.class, "ray.job.default-actor-lifetime");

    // jvm options for java workers of this job.
    jvmOptionsForJavaWorker = config.getStringList("ray.job.jvm-options");
    validateJvmOptionsForJavaWorker();

    ImmutableMap.Builder<String, String> workerEnvBuilder = ImmutableMap.builder();
    Config workerEnvConfig = config.getConfig("ray.job.worker-env");
    if (workerEnvConfig != null) {
      for (Map.Entry<String, ConfigValue> entry : workerEnvConfig.entrySet()) {
        workerEnvBuilder.put(entry.getKey(), workerEnvConfig.getString(entry.getKey()));
      }
    }
    workerEnv = workerEnvBuilder.build();
    updateSessionDir(null);

    // Object store socket name.
    if (config.hasPath("ray.object-store.socket-name")) {
      objectStoreSocketName = config.getString("ray.object-store.socket-name");
    }

    // Raylet socket name.
    if (config.hasPath("ray.raylet.socket-name")) {
      rayletSocketName = config.getString("ray.raylet.socket-name");
    }

    // Redis configurations.
    String redisAddress = config.getString("ray.address");
    if (StringUtils.isNotBlank(redisAddress)) {
      setRedisAddress(redisAddress);
    } else {
      // We need to start gcs using `RunManager` for local cluster
      this.redisAddress = null;
    }

    redisPassword = config.getString("ray.redis.password");

    // Raylet node manager port.
    if (config.hasPath("ray.raylet.node-manager-port")) {
      nodeManagerPort = config.getInt("ray.raylet.node-manager-port");
    } else {
      Preconditions.checkState(
          workerMode != WorkerType.WORKER,
          "Worker started by raylet should accept the node manager port from raylet.");
    }

    // Job code search path.
    String codeSearchPathString = null;
    if (config.hasPath("ray.job.code-search-path")) {
      codeSearchPathString = config.getString("ray.job.code-search-path");
    }
    if (StringUtils.isEmpty(codeSearchPathString)) {
      codeSearchPathString = System.getProperty("java.class.path");
    }

    /**
     * If the result of `System.getProperty("java.class.path");` is null, then the
     * `codeSearchPathString` will be null as well, so that `split` will throw NPE. So we filter the
     * null case here.
     */
    if (!StringUtils.isEmpty(codeSearchPathString)) {
      codeSearchPath = Arrays.asList(codeSearchPathString.split(":"));
    } else {
      codeSearchPath = ImmutableList.of();
    }

    numWorkersPerProcess = config.getInt("ray.job.num-java-workers-per-process");

    javaWorkerProcessDefaultMemoryMb =
        config.getLong("ray.job.java-worker-process-default-memory-mb");

    numInitialJavaWorkerProcesses = config.getInt("ray.job.num-initial-java-worker-processes");

    totalMemoryMb = config.getLong("ray.job.total-memory-mb");

    maxTotalMemoryMb = config.getLong("ray.job.max-total-memory-mb");

    javaHeapFraction = (float) config.getDouble("ray.job.java-heap-fraction");

    longRunning = config.getBoolean("ray.job.long-running");

    enableL1FaultTolerance = config.getBoolean("ray.job.enable-l1-fault-tolerance");

    serializedRuntimeEnv = config.getString("ray.job.serialized-runtime-env");

    returnTaskException = config.getBoolean("ray.task.return_task_exception");

    logLevel = config.getString("ray.job.logging-level");

    rayLogLevel = config.getString("ray.logging.level");

    {
      loggers = new ArrayList<>();
      List<Config> loggerConfigs = (List<Config>) config.getConfigList("ray.logging.loggers");
      for (Config loggerConfig : loggerConfigs) {
        Preconditions.checkState(loggerConfig.hasPath("name"));
        Preconditions.checkState(loggerConfig.hasPath("file-name"));

        final String name = loggerConfig.getString("name");
        final String fileName = loggerConfig.getString("file-name");
        final String pattern =
            loggerConfig.hasPath("pattern") ? loggerConfig.getString("pattern") : "";
        loggers.add(new LoggerConf(name, fileName, pattern));
      }
    }

    headArgs = config.getStringList("ray.head-args");

    actorTaskBackPressureEnabled = config.getBoolean("ray.job.actor-task-back-pressure-enabled");
    maxPendingCalls = config.getLong("ray.job.max-pending-calls");
    markJobStateAsFailedWHenKilling =
        config.getBoolean("ray.job.mark_job_state_as_failed_when_killing");
    gcsTaskSchedulingEnabled = !("false".equals(System.getenv("RAY_GCS_TASK_SCHEDULING_ENABLED")));

    // Validate config.
    validate();
  }

  public void setRedisAddress(String redisAddress) {
    Preconditions.checkNotNull(redisAddress);
    Preconditions.checkState(this.redisAddress == null, "Redis address was already set");

    this.redisAddress = redisAddress;
  }

  public String getRedisAddress() {
    return redisAddress;
  }

  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  public JobId getJobId() {
    return this.jobId;
  }

  public int getNodeManagerPort() {
    return nodeManagerPort;
  }

  public void setSessionDir(String sessionDir) {
    updateSessionDir(sessionDir);
  }

  public Config getInternalConfig() {
    return config;
  }

  /** Renders the config value as a HOCON string. */
  @Override
  public String toString() {
    // These items might be dynamically generated or mutated at runtime.
    // Explicitly include them.
    Map<String, Object> dynamic = new HashMap<>();
    dynamic.put("ray.session-dir", sessionDir);
    dynamic.put("ray.raylet.socket-name", rayletSocketName);
    dynamic.put("ray.object-store.socket-name", objectStoreSocketName);
    dynamic.put("ray.raylet.node-manager-port", nodeManagerPort);
    dynamic.put("ray.address", redisAddress);
    dynamic.put("ray.job.code-search-path", codeSearchPath);
    Config toRender = ConfigFactory.parseMap(dynamic).withFallback(config);
    return toRender.root().render(ConfigRenderOptions.concise());
  }

  private void updateSessionDir(String sessionDir) {
    // session dir
    if (config.hasPath("ray.session-dir")) {
      sessionDir = config.getString("ray.session-dir");
    }
    if (sessionDir != null) {
      sessionDir = removeTrailingSlash(sessionDir);
    }
    this.sessionDir = sessionDir;

    // Log dir.
    String localLogDir = null;
    if (config.hasPath("ray.logging.dir")) {
      localLogDir = removeTrailingSlash(config.getString("ray.logging.dir"));
    }
    if (Strings.isNullOrEmpty(localLogDir)) {
      logDir = String.format("%s/logs", sessionDir);
    } else {
      logDir = localLogDir;
    }
  }

  /**
   * Create a RayConfig by reading configuration in the following order: 1. System properties. 2.
   * `ray.conf` file. 3. `ray.default.conf` file.
   */
  public static RayConfig create() {
    ConfigFactory.invalidateCaches();
    Config config = ConfigFactory.systemProperties();
    String configPath = System.getProperty("ray.config-file");
    if (Strings.isNullOrEmpty(configPath)) {
      config = config.withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
    } else {
      config = config.withFallback(ConfigFactory.parseFile(new File(configPath)));
    }
    config = config.withFallback(ConfigFactory.load(DEFAULT_CONFIG_FILE));
    return new RayConfig(config.withOnlyPath("ray"));
  }

  private void validateJvmOptionsForJavaWorker() {
    if (jvmOptionsForJavaWorker == null || jvmOptionsForJavaWorker.isEmpty()) {
      return;
    }

    /// Flag to indicate if user set xmx.
    boolean setXmx = false;
    /// Flag to indicate if user set other jvm memory options except xmx.
    boolean setOtherMemOptions = false;

    for (String item : jvmOptionsForJavaWorker) {
      Preconditions.checkNotNull(item);
      String optionStr = item.trim();
      if (StringUtils.startsWithIgnoreCase(optionStr, "-Xmx")) {
        setXmx = true;
      } else if (StringUtils.startsWithIgnoreCase(optionStr, "-Xms")
          || StringUtils.startsWithIgnoreCase(optionStr, "-Xmn")
          || StringUtils.startsWithIgnoreCase(optionStr, "-Xss")
          || StringUtils.contains(optionStr, "MaxDirectMemorySize")) {
        setOtherMemOptions = true;
      }
    }

    if (setOtherMemOptions) {
      Preconditions.checkState(
          setXmx, "You shouldn't set other jvm memory options without setting xmx first.");
    }
  }
}
