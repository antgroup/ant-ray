package org.ray.api;

public class RayInitConfig {

  private String redisIpAddr;
  private int redisPort;
  private String nodeIpAddr;
  private int nodePort;
  private RunMode runMode = RunMode.SINGLE_BOX;
  private WorkerMode workerMode = WorkerMode.NONE;

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

}
