package org.ray.runtime.runner;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.gcs.AddressInfo;
import org.ray.runtime.runner.RunInfo.ProcessType;
import org.ray.runtime.util.ResourceUtil;
import org.ray.runtime.util.StringUtil;
import org.ray.runtime.util.logger.RayLog;
import redis.clients.jedis.Jedis;

/**
 * Ray service management on one box.
 */
public class RunManager {

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("Y-m-d_H-M-S");

  private RayConfig rayConfig;

  private RunInfo runInfo = new RunInfo();

  private Random random = new Random();


  public RunManager(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
  }

  private static boolean killProcess(Process p) {
    if (p.isAlive()) {
      p.destroy();
      return true;
    } else {
      return false;
    }
  }

  public RunInfo info() {
    return runInfo;
  }

  public void startRayHead(RayConfig rayConfig) throws Exception {
    if (rayConfig.redisAddress.length() != 0) {
      throw new Exception("Redis address must be empty in head node.");
    }
    if (rayConfig.numberRedisShards <= 0) {
      rayConfig.numberRedisShards = 1;
    }
    startRayProcesses(rayConfig, true);
  }

  public void startRayNode() throws Exception {
    if (rayConfig.redisAddress.length() == 0) {
      throw new Exception("Redis address cannot be empty in non-head node.");
    }
    if (rayConfig.numberRedisShards != 0) {
      throw new Exception("Number of redis shards should be zero in non-head node.");
    }
    startRayProcesses(rayConfig, false);
  }

  public Process startDriver(String mainClass, String redisAddress, UniqueId driverId,
      String logDir, String ip,
      String driverClass, String driverArgs, String additonalClassPaths,
      String additionalConfigs) {
    String driverConfigs =
        "ray.java.start.driver_id=" + driverId + ";ray.java.start.driver_class=" + driverClass;
    if (driverArgs != null) {
      driverConfigs += ";ray.java.start.driver_args=" + driverArgs;
    }

    if (null != additionalConfigs) {
      additionalConfigs += ";" + driverConfigs;
    } else {
      additionalConfigs = driverConfigs;
    }

    return startJavaProcess(
        RunInfo.ProcessType.PT_DRIVER,
        mainClass,
        additonalClassPaths,
        "",
        ip,
        redisAddress,
        false,
        false,
        null
    );
  }

  private Process startJavaProcess(RunInfo.ProcessType pt, String mainClass,
      String additonalClassPaths, String additionalJvmArgs, String ip, String
      redisAddr, boolean redirect,
      boolean cleanup, String agentlibAddr) {

    String cmd = buildJavaProcessCommand(pt, mainClass, additonalClassPaths,
        additionalJvmArgs, ip, redisAddr, agentlibAddr);
    return startProcess(cmd.split(" "), null, pt, "", redisAddr, ip, redirect, cleanup);
  }

  private String buildJavaProcessCommand(
      RunInfo.ProcessType pt, String mainClass, String additionalClassPaths,
      String additionalJvmArgs, String ip, String redisAddr, String agentlibAddr) {
    String cmd = "java -ea -noverify " + rayConfig.jvmParamters + " ";
    if (agentlibAddr != null && !agentlibAddr.equals("")) {
      cmd += " -agentlib:jdwp=transport=dt_socket,address=" + agentlibAddr + ",server=y,suspend=n";
    }

    cmd += " -Djava.library.path=" + StringUtil.mergeArray(rayConfig.javaJnilibPaths, ":");
    cmd += " -Dray.run-mode=" + rayConfig.runMode;
    cmd += " -Dray.node-ip=" + ip;
    cmd += " -Dray.log-dir=" + rayConfig.logDir;
    cmd += " -classpath " + StringUtil.mergeArray(rayConfig.javaClasspaths, ":");

    if (additionalClassPaths.length() > 0) {
      cmd += ":" + additionalClassPaths;
    }

    if (additionalJvmArgs.length() > 0) {
      cmd += " " + additionalJvmArgs;
    }

    cmd += " " + mainClass;

    return cmd;
  }

  private Process startProcess(String[] cmd, Map<String, String> env, RunInfo.ProcessType type,
      String name,
      String redisAddress, String ip, boolean redirect,
      boolean cleanup) {
    ProcessBuilder builder;
    List<String> newCommand = Arrays.asList(cmd);
    builder = new ProcessBuilder(newCommand);

    if (redirect) {
      int logId = random.nextInt(10000);
      String date = DATE_TIME_FORMATTER.format(LocalDateTime.now());
      String stdout = String.format("%s/%s-%s-%05d.out", rayConfig.logDir, name, date, logId);
      String stderr = String.format("%s/%s-%s-%05d.err", rayConfig.logDir, name, date, logId);
      builder.redirectOutput(new File(stdout));
      builder.redirectError(new File(stderr));
      recordLogFilesInRedis(redisAddress, ip, ImmutableList.of(stdout, stderr));
    }

    if (env != null && !env.isEmpty()) {
      builder.environment().putAll(env);
    }

    Process p = null;
    try {
      p = builder.start();
    } catch (IOException e) {
      RayLog.core.error("Failed to start process {}", name, e);
      return null;
    }

    RayLog.core.info("Process {} started", name);

    if (cleanup) {
      runInfo.toBeCleanedProcesses.get(type.ordinal()).add(p);
    }

    ProcessInfo processInfo = new ProcessInfo();
    processInfo.cmd = cmd;
    processInfo.type = type;
    processInfo.name = name;
    processInfo.redisAddress = redisAddress;
    processInfo.ip = ip;
    processInfo.redirect = redirect;
    processInfo.cleanup = cleanup;
    processInfo.process = p;
    runInfo.allProcesses.get(type.ordinal()).add(processInfo);

    return p;
  }

  private void recordLogFilesInRedis(String redisAddress, String nodeIpAddress,
      List<String> logFiles) {
    if (redisAddress != null && !redisAddress.isEmpty() && nodeIpAddress != null
        && !nodeIpAddress.isEmpty() && logFiles.size() > 0) {
      String[] ipPort = redisAddress.split(":");
      Jedis jedisClient = new Jedis(ipPort[0], Integer.parseInt(ipPort[1]));
      String logFileListKey = String.format("LOG_FILENAMES:{%s}", nodeIpAddress);
      for (String logfile : logFiles) {
        jedisClient.rpush(logFileListKey, logfile);
      }
      jedisClient.close();
    }
  }

  private void startRayProcesses(RayConfig rayConfig, boolean startRedisShards) {
    Jedis redisClient = null;

    RayLog.core.info("start ray processes @ " + rayConfig.nodeIp + " ...");

    // start primary redis
    if (rayConfig.redisAddress.length() == 0) {
      List<String> primaryShards = startRedis(
          rayConfig.nodeIp, rayConfig.headRedisPort, 1, rayConfig.redirectOutput, rayConfig.cleanup);
      rayConfig.redisAddress = primaryShards.get(0);

      String[] args = rayConfig.redisAddress.split(":");
      redisClient = new Jedis(args[0], Integer.parseInt(args[1]));

      // Register the number of Redis shards in the primary shard, so that clients
      // know how many redis shards to expect under RedisShards.
      redisClient.set("NumRedisShards", Integer.toString(rayConfig.numberRedisShards));
    } else {
      String[] args = rayConfig.redisAddress.split(":");
      redisClient = new Jedis(args[0], Integer.parseInt(args[1]));
    }
    runInfo.redisAddress = rayConfig.redisAddress;

    // start redis shards
    if (startRedisShards) {
      runInfo.redisShards = startRedis(
          rayConfig.nodeIp, rayConfig.headRedisPort + 1, rayConfig.numberRedisShards,
          rayConfig.redirectOutput,
          rayConfig.cleanup);

      // Store redis shard information in the primary redis shard.
      for (int i = 0; i < runInfo.redisShards.size(); i++) {
        String addr = runInfo.redisShards.get(i);
        redisClient.rpush("RedisShards", addr);
      }
    }
    redisClient.close();

    AddressInfo info = new AddressInfo();

    // Start object store
    int rpcPort = rayConfig.objectStoreNameIndex;
    String storeName = "/tmp/plasma_store" + rpcPort;

    startObjectStore(0, info,
        rayConfig.redisAddress, rayConfig.nodeIp, rayConfig.redirectOutput, rayConfig.cleanup);

    Map<String, Double> staticResources =
            ResourceUtil.getResourcesMapFromString(rayConfig.staticResources);

    //Start raylet
    startRaylet(storeName, info, 0,
        rayConfig.redisAddress,
        rayConfig.nodeIp, rayConfig.redirectOutput, staticResources, rayConfig.cleanup);

    runInfo.localStores.add(info);

    if (!checkAlive()) {
      cleanup(true);
      throw new RuntimeException("Start Ray processes failed");
    }
  }

  private boolean checkAlive() {
    RunInfo.ProcessType[] types = RunInfo.ProcessType.values();
    for (int i = 0; i < types.length; i++) {
      ProcessInfo p;
      for (int j = 0; j < runInfo.allProcesses.get(i).size(); ) {
        p = runInfo.allProcesses.get(i).get(j);
        if (!p.process.isAlive()) {
          RayLog.core.error("Process " + p.process.hashCode() + " is not alive!" + " Process Type "
              + types[i].name());
          runInfo.deadProcess.add(p);
          runInfo.allProcesses.get(i).remove(j);
        } else {
          j++;
        }
      }
    }

    return runInfo.deadProcess.isEmpty();
  }

  // kill all processes started by startRayHead
  public void cleanup(boolean killAll) {
    // clean up the process in reverse order
    for (int i = ProcessType.values().length - 1; i >= 0; i--) {
      if (killAll) {
        runInfo.allProcesses.get(i).forEach(p -> {
          if (killProcess(p.process)) {
            RayLog.core.info("Kill process " + p.process.hashCode() + " forcely");
          }
        });
      } else {
        runInfo.toBeCleanedProcesses.get(i).forEach(p -> {
          if (killProcess(p)) {
            RayLog.core.info("Kill process " + p.hashCode() + " forcely");
          }
        });
      }

      runInfo.toBeCleanedProcesses.get(i).clear();
      runInfo.allProcesses.get(i).clear();
      runInfo.deadProcess.clear();
    }
  }

  //
  // start a redis server
  //
  // @param ip the IP address of the local node
  // @param port port to be opended for redis traffic
  // @param numOfShards the number of redis shards to start
  // @param redirect whether to redirect the output/err to the log files
  // @param cleanup true if using ray in local mode. If cleanup is true, when
  // all Redis processes started by this method will be killed by @cleanup
  // when the worker exits
  // @return primary redis shard address
  //
  private List<String> startRedis(String ip, int port, int numOfShards,
      boolean redirect, boolean cleanup) {
    ArrayList<String> shards = new ArrayList<>();
    String addr;
    for (int i = 0; i < numOfShards; i++) {
      addr = startRedisInstance(ip, port + i, redirect, cleanup);

      if (addr.length() == 0) {
        cleanup(cleanup);
        shards.clear();
        return shards;
      } else {
        shards.add(addr);
      }
    }

    for (String shard : shards) {
      // TODO: wait for redis server to start
    }

    return shards;
  }

  //
  // @param ip local node ip, only used for logging purpose
  // @param port given port for this redis instance, 0 for auto-selected port
  // @return redis server address
  //
  private String startRedisInstance(String ip, int port,
      boolean redirect, boolean cleanup) {
    String redisFilePath = rayConfig.redisServerPath;
    String redisModule = rayConfig.redisModulePath;

    assert (new File(redisFilePath).exists()) : "file don't exsits : " + redisFilePath;
    assert (new File(redisModule).exists()) : "file don't exsits : " + redisModule;

    String cmd = redisFilePath + " --protected-mode no --port " + port + " --loglevel warning"
        + " --loadmodule " + redisModule;

    Map<String, String> env = null;
    Process p = startProcess(cmd.split(" "), env, RunInfo.ProcessType.PT_REDIS_SERVER,
        "redis", "", ip, redirect, cleanup);

    if (p == null || !p.isAlive()) {
      return "";
    }

    try {
      TimeUnit.MILLISECONDS.sleep(300);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Jedis client = new Jedis(rayConfig.nodeIp, port);

    // Configure Redis to only generate notifications for the export keys.
    client.configSet("notify-keyspace-events", "Kl");

    // Put a time stamp in Redis to indicate when it was started.
    client.set("redis_start_time", LocalDateTime.now().toString());

    client.close();
    return ip + ":" + port;
  }

  private void startRaylet(String storeName, AddressInfo info, int numWorkers,
      String redisAddress, String ip, boolean redirect,
      Map<String, Double> staticResources, boolean cleanup) {

    int rpcPort = rayConfig.rayletPort;
    String rayletSocketName = "/tmp/raylet" + rpcPort;

    String filePath = rayConfig.rayletPath;

    //Create the worker command that the raylet will use to start workers.
    String workerCommand = buildWorkerCommandRaylet(info.storeName, rayletSocketName, ip, redisAddress);

    int sep = redisAddress.indexOf(':');
    assert (sep != -1);
    String gcsIp = redisAddress.substring(0, sep);
    String gcsPort = redisAddress.substring(sep + 1);

    String resourceArgument = ResourceUtil.getResourcesStringFromMap(staticResources);

    int hardwareConcurrency = Runtime.getRuntime().availableProcessors();
    int maximumStartupConcurrency = Math.max(1, Math.min(staticResources.get("CPU").intValue(),
                                                         hardwareConcurrency));

    // The second-last arugment is the worker command for Python, not needed for Java.
    String[] cmds = new String[]{filePath, rayletSocketName, storeName, ip, gcsIp,
        gcsPort, String.valueOf(numWorkers), String.valueOf(maximumStartupConcurrency),
        resourceArgument, "", workerCommand};

    Process p = startProcess(cmds, null, RunInfo.ProcessType.PT_RAYLET,
        "raylet", redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (p == null || !p.isAlive()) {
      info.rayletSocketName = "";
      info.rayletRpcAddr = "";
      throw new RuntimeException("Failed to start raylet process.");
    } else {
      info.rayletSocketName = rayletSocketName;
      info.rayletRpcAddr = ip + ":" + rpcPort;
    }
  }

  private String buildWorkerCommandRaylet(String storeName, String rayletSocketName,
      String ip, String redisAddress) {
    //TODO(qwang): We should remove this code once we get rid of `ConfigReader`.
    String workerConfigs = "ray.java.start.raylet_socket_name=" + rayletSocketName;


    String jvmArgs = "";
    jvmArgs += " -Dlogging.file.name=core-*pid_suffix*";
    jvmArgs += " -Dlogging.path=" + rayConfig.logDir;
    jvmArgs += " -Dray.redis.address=" + rayConfig.redisAddress;
    jvmArgs += " -Dray.object-store.name=" + storeName;
    jvmArgs += " -Dray.raylet.socket-name=" + rayletSocketName;

    return buildJavaProcessCommand(
        RunInfo.ProcessType.PT_WORKER,
        "org.ray.runtime.runner.worker.DefaultWorker",
        workerConfigs,
        jvmArgs,
        ip,
        redisAddress,
        null
    );
  }

  private void startObjectStore(int index, AddressInfo info, String redisAddress,
      String ip, boolean redirect, boolean cleanup) {
    long memoryBytes = rayConfig.ObjectStoreOccupiedSize;
    String filePath = rayConfig.plasmaStorePath;
    int rpcPort = rayConfig.objectStoreNameIndex + index;
    String name = "/tmp/plasma_store" + rpcPort;
    String rpcAddr = "";
    String cmd = filePath + " -s " + name + " -m " + memoryBytes;

    Map<String, String> env = null;
    Process p = startProcess(cmd.split(" "), env, RunInfo.ProcessType.PT_PLASMA_STORE,
        "plasma_store", redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (p == null || !p.isAlive()) {
      info.storeName = "";
      info.storeRpcAddr = "";
      throw new RuntimeException("Start object store failed ...");
    } else {
      info.storeName = name;
      info.storeRpcAddr = rpcAddr;
    }
  }

}
