package org.ray.yarn.config;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Ray Cluster Setup
 * TODO: support multiple worker group
 */
public class RayClusterConfig extends AppConfig {

  private Map<String,Object> fullContent;

  // Cluster Name
  private String clusterName;
  // App master priority
  private int priority = 0;
  // Shell Command Container priority
  private int shellCmdPriority = 0;
  // Queue for App master
  private String physicalQueueName;
  // Amt. of memory resource to request for to run the App Master
  private long amMemory = 1024;
  // Amt. of virtual core resource to request for to run the App Master
  private int amVCores = 1;
  // Application master jar url
  private String amJarUploadUrl;

  // Amt. of virtual cores to request for container in which shell script will be executed
  int headContainerVCores = 1;
  // Amt of memory to request for container in which shell script will be executed
  long headContainerMemory = 2048;
  // Amt. of virtual cores to request for container in which shell script will be executed
  int containerVCores = 1;
  // Amt of memory to request for container in which shell script will be executed
  long containerMemory = 4096;
  // No. of containers in which the shell script needs to be executed
  int numContainers = 1;
  // Node Label to schedule
  String nodeLabelExpression = null;
  // supremeFo flag
  boolean supremeFo = false;
  // disable process failover flag
  boolean disableProcessFo = false;
  // Args to be passed to the shell command
  String[] shellArgs = new String[] {};
  // Env variables to be setup for the shell command
  Map<String, String> shellEnv = new HashMap<String, String>();
  // No. of the Ray roles including head and work
  private Map<String, Integer> numRoles = Maps.newHashMapWithExpectedSize(2);

  // Common setup commands
  String setupCommands;
  // Head setup commands
  String headSetupCommands;
  // Worker setup commands
  String workerSetupCommands;
  // Head start commands
  String headStartCommands;
  // Worker start commands
  String workerStartCommands;

  private int headNodeNumber = 1;
  private int workNodeNumber;


  @Override
  public void validate() {
    // TODO
    super.validate();

    // user defined env vars
    if (shellEnv != null && shellEnv.size() > 0) {
      String[] envs = shellArgs;
      for (String env : envs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          shellEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        shellEnv.put(key, val);
      }
    }

    if (numRoles != null) {
      numRoles.put("head", 1);
      numRoles.put("work", 1);
    }

    if (containerMemory < 0 || containerVCores < 0 || numContainers < 1) {
      throw new IllegalArgumentException(
          "Invalid no. of containers or container memory/vcores specified," + " exiting."
              + " Specified containerMemory=" + containerMemory + ", containerVCores="
              + containerVCores + ", numContainer=" + numContainers);
    }

  }

  public Map<String, Object> getFullContent() {
    return fullContent;
  }

  public void setFullContent(Map<String, Object> fullContent) {
    this.fullContent = fullContent;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public int getShellCmdPriority() {
    return shellCmdPriority;
  }

  public void setShellCmdPriority(int shellCmdPriority) {
    this.shellCmdPriority = shellCmdPriority;
  }

  public String getPhysicalQueueName() {
    return physicalQueueName;
  }

  public void setPhysicalQueueName(String physicalQueueName) {
    this.physicalQueueName = physicalQueueName;
  }

  @Override
  public long getAmMemory() {
    return amMemory;
  }

  @Override
  public void setAmMemory(long amMemory) {
    this.amMemory = amMemory;
  }

  @Override
  public int getAmVCores() {
    return amVCores;
  }

  @Override
  public void setAmVCores(int amVCores) {
    this.amVCores = amVCores;
  }

  public String getAmJarUploadUrl() {
    return amJarUploadUrl;
  }

  public void setAmJarUploadUrl(String amJarUploadUrl) {
    this.amJarUploadUrl = amJarUploadUrl;
  }

  public int getHeadContainerVCores() {
    return headContainerVCores;
  }

  public void setHeadContainerVCores(int headContainerVCores) {
    this.headContainerVCores = headContainerVCores;
  }

  public long getHeadContainerMemory() {
    return headContainerMemory;
  }

  public void setHeadContainerMemory(long headContainerMemory) {
    this.headContainerMemory = headContainerMemory;
  }

  public int getContainerVCores() {
    return containerVCores;
  }

  public void setContainerVCores(int containerVCores) {
    this.containerVCores = containerVCores;
  }

  public long getContainerMemory() {
    return containerMemory;
  }

  public void setContainerMemory(long containerMemory) {
    this.containerMemory = containerMemory;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }

  public String getNodeLabelExpression() {
    return nodeLabelExpression;
  }

  public void setNodeLabelExpression(String nodeLabelExpression) {
    this.nodeLabelExpression = nodeLabelExpression;
  }

  public boolean isSupremeFo() {
    return supremeFo;
  }

  public void setSupremeFo(boolean supremeFo) {
    this.supremeFo = supremeFo;
  }

  public boolean isDisableProcessFo() {
    return disableProcessFo;
  }

  public void setDisableProcessFo(boolean disableProcessFo) {
    this.disableProcessFo = disableProcessFo;
  }

  public String[] getShellArgs() {
    return shellArgs;
  }

  public void setShellArgs(String[] shellArgs) {
    this.shellArgs = shellArgs;
  }

  public Map<String, String> getShellEnv() {
    return shellEnv;
  }

  public void setShellEnv(Map<String, String> shellEnv) {
    this.shellEnv = shellEnv;
  }

  public Map<String, Integer> getNumRoles() {
    return numRoles;
  }

  public void setNumRoles(Map<String, Integer> numRoles) {
    this.numRoles = numRoles;
  }

  public String getSetupCommands() {
    return setupCommands;
  }

  public void setSetupCommands(String setupCommands) {
    this.setupCommands = setupCommands;
  }

  public String getHeadSetupCommands() {
    return headSetupCommands;
  }

  public void setHeadSetupCommands(String headSetupCommands) {
    this.headSetupCommands = headSetupCommands;
  }

  public String getWorkerSetupCommands() {
    return workerSetupCommands;
  }

  public void setWorkerSetupCommands(String workerSetupCommands) {
    this.workerSetupCommands = workerSetupCommands;
  }

  public String getHeadStartCommands() {
    return headStartCommands;
  }

  public void setHeadStartCommands(String headStartCommands) {
    this.headStartCommands = headStartCommands;
  }

  public String getWorkerStartCommands() {
    return workerStartCommands;
  }

  public void setWorkerStartCommands(String workerStartCommands) {
    this.workerStartCommands = workerStartCommands;
  }

  public int getHeadNodeNumber() {
    return headNodeNumber;
  }

  public void setHeadNodeNumber(int headNodeNumber) {
    this.headNodeNumber = headNodeNumber;
  }

  public int getWorkNodeNumber() {
    return workNodeNumber;
  }

  public void setWorkNodeNumber(int workNodeNumber) {
    this.workNodeNumber = workNodeNumber;
  }
}
