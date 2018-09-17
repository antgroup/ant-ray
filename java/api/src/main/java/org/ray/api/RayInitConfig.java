package org.ray.api;

import java.util.HashMap;

public class RayInitConfig {

  private String redisIpAddr;
  private int redisPort;
  private String nodeIpAddr;
  private int nodePort;
  private RunMode runMode = RunMode.SINGLE_BOX;
  private WorkerMode workerMode = WorkerMode.NONE;

  private String overWrite;

  public RayInitConfig(String[] args) {
    String config = null;
    String overWrite = null;
    for (String arg : args) {
      if (arg.startsWith("--config=")) {
        config = arg.substring("--config=".length());
      } else if (arg.startsWith("--overwrite=")) {
        overWrite = arg.substring("--overwrite=".length());
      } else {
        throw new RuntimeException("Args " + arg
                                       + " is not recognized, please use --overwrite to merge it into config file");
      }
    }
  }

  public void setRedisIpAddr(String redisIpAddr) {
    this.redisIpAddr = redisIpAddr;
  }

  public void setRedisPort(int redisPort) {
    this.redisPort = redisPort;
  }

  public void setNodeIpAddr(String nodeIpAddr) {
    this.nodeIpAddr = nodeIpAddr;
  }

  public void setNodePort(int nodePort) {
    this.nodePort = nodePort;
  }

  public void setRunMode(RunMode runMode) {
    this.runMode = runMode;
  }

  public void setWorkerMode(WorkerMode workerMode) {
    this.workerMode = workerMode;
  }

  public String getOverWrite() {
    return overWrite;
  }

}
