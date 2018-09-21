package org.ray.cli;

import com.beust.jcommander.JCommander;

import java.io.File;
import java.io.IOException;
import net.lingala.zip4j.core.ZipFile;
import org.ray.api.RunMode;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.functionmanager.NativeRemoteFunctionManager;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.gcs.KeyValueStoreLink;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.gcs.StateStoreProxy;
import org.ray.runtime.gcs.StateStoreProxyImpl;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.runner.worker.DefaultDriver;
import org.ray.runtime.util.FileUtil;
import org.ray.runtime.util.RayLog;


/**
 * Ray command line interface.
 */
public class RayCli {

  private static RayCliArgs rayArgs = new RayCliArgs();

  private static RayConfig rayConfig = new RayConfig();

  private static RunManager startRayHead() {
    RunManager manager = new RunManager(rayConfig);

    try {
      manager.startRayHead();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startRayHead", e);
      throw new RuntimeException("Ray head node start failed", e);
    }

    RayLog.core.info("Started Ray head node. Redis address: {}", manager.info().redisAddress);
    return manager;
  }

  private static RunManager startRayNode() {
    RunManager manager = new RunManager(rayConfig);
    try {
      manager.startRayNode();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startRayNode", e);
      throw new RuntimeException("Ray work node start failed, err = " + e.getMessage());
    }

    RayLog.core.info("Started Ray work node.");
    return manager;
  }

  private static RunManager startProcess(CommandStart cmdStart) {

    // Init RayLog before using it.
    RayLog.init(rayConfig.logDir);

    RayLog.core.info("Using IP address {} for this node.", rayConfig.nodeIp);
    RunManager manager;
    if (cmdStart.head) {
      manager = startRayHead();
    } else {
      manager = startRayNode();
    }
    return manager;
  }

  private static void start(CommandStart cmdStart) {
    startProcess(cmdStart);
  }

  private static void stop(CommandStop cmdStop) {
    String[] cmd = {"/bin/sh", "-c", ""};

    cmd[2] = "killall global_scheduler local_scheduler plasma_store plasma_manager";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      RayLog.core.warn("exception in killing ray processes");
    }

    cmd[2] = "kill $(ps aux | grep redis-server | grep -v grep | "
        + "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      RayLog.core.warn("exception in killing ray processes");
    }

    cmd[2] = "kill -9 $(ps aux | grep DefaultWorker | grep -v grep | "
        + "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      RayLog.core.warn("exception in killing ray processes");
    }
  }
 
  private static void submit(CommandSubmit cmdSubmit) throws Exception {

    rayConfig.redisAddress = cmdSubmit.redisAddress;
    rayConfig.runMode = RunMode.CLUSTER;

    KeyValueStoreLink kvStore = new RedisClient();
    kvStore.setAddr(cmdSubmit.redisAddress);
    StateStoreProxy stateStoreProxy = new StateStoreProxyImpl(kvStore);
    stateStoreProxy.initializeGlobalState();

    RemoteFunctionManager functionManager = new NativeRemoteFunctionManager(kvStore);

    // Register app to Redis. 
    byte[] zip = FileUtil.fileToBytes(cmdSubmit.packageZip);

    String packageName = cmdSubmit.packageZip.substring(
        cmdSubmit.packageZip.lastIndexOf('/') + 1,
        cmdSubmit.packageZip.lastIndexOf('.'));

    UniqueId resourceId = functionManager.registerResource(zip);

    // Init RayLog before using it.
    RayLog.init(rayConfig.logDir);

    RayLog.rapp.debug(
        "registerResource " + resourceId + " for package " + packageName + " done");

    UniqueId appId = rayConfig.driverId;
    functionManager.registerApp(appId, resourceId);
    RayLog.rapp.debug("registerApp " + appId + " for resouorce " + resourceId + " done");
  
    // Unzip the package file.
    String appDir = "/tmp/" + cmdSubmit.className;
    String extPath = appDir + "/" + packageName;
    if (!FileUtil.createDir(extPath, false)) {
      throw new RuntimeException("create dir " + extPath + " failed ");
    }

    ZipFile zipFile = new ZipFile(cmdSubmit.packageZip);
    zipFile.extractAll(extPath);

    // Build the args for driver process.
    File originDirFile = new File(extPath);
    File[] topFiles = originDirFile.listFiles();
    String topDir = null;
    for (File file : topFiles) {
      if (file.isDirectory()) {
        topDir = file.getName();
      }
    }
    RayLog.rapp.debug("topDir of app classes: " + topDir);
    if (topDir == null) {
      RayLog.rapp.error("Can't find topDir of app classes, the app directory " + appDir);
      return;
    }

    String additionalClassPath = appDir + "/" + packageName  + "/" + topDir + "/*";
    RayLog.rapp.debug("Find app class path  " + additionalClassPath);

    // Start driver process.
    RunManager runManager = new RunManager(rayConfig);
    Process proc = runManager.startDriver(
        DefaultDriver.class.getName(),
        cmdSubmit.redisAddress,
        appId,
        appDir,
        rayConfig.nodeIp,
        cmdSubmit.className,
        cmdSubmit.classArgs,
        additionalClassPath,
        null);

    if (null == proc) { 
      RayLog.rapp.error(
          "Create process for app " + packageName + " in local directory " + appDir
          + " failed");
      return;
    }

    RayLog.rapp
    .info("Create app " + appDir + " for package " + packageName + " succeeded");
  }

  public static void main(String[] args) throws Exception {

    CommandStart cmdStart = new CommandStart();
    CommandStop cmdStop = new CommandStop();
    CommandSubmit cmdSubmit = new CommandSubmit();
    JCommander rayCommander = JCommander.newBuilder().addObject(rayArgs)
        .addCommand("start", cmdStart)
        .addCommand("stop", cmdStop)
        .addCommand("submit", cmdSubmit)
        .build();
    rayCommander.parse(args);

    if (rayArgs.help) {
      rayCommander.usage();
      System.exit(0);
    }

    String cmd = rayCommander.getParsedCommand();
    if (cmd == null) {
      rayCommander.usage();
      System.exit(0);
    }

    switch (cmd) {
      case "start": {
        start(cmdStart);
      }
      break;
      case "stop":
        stop(cmdStop);
        break;
      case "submit":
        submit(cmdSubmit);
        break;
      default:
        rayCommander.usage();
    }
  }

}
