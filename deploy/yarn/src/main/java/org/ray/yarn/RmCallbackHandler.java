package org.ray.yarn;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.ray.yarn.config.RayClusterConfig;
import org.ray.yarn.utils.TimelineUtil;

public class RmCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {

  public static final Log logger = LogFactory.getLog(RmCallbackHandler.class);
  private final RayClusterConfig rayConf;
  private final ApplicationMasterState amState;
  private final AMRMClientAsync amRmClient = null;
  private final TimelineClient timelineClient;
  private final List<Thread> launchThreads = new ArrayList<Thread>();
  private final UserGroupInformation appSubmitterUgi;

  public RmCallbackHandler(ApplicationMaster applicationMaster) {
    rayConf = applicationMaster.getRayConf();
    amState = applicationMaster.amState;
    timelineClient = applicationMaster.timelineClient;
    appSubmitterUgi = applicationMaster.appSubmitterUgi;
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> completedContainers) {
    Boolean restartClasterFlag = false;
    logger.info(
        "Got response from RM for container ask, completedCnt=" + completedContainers.size());
    for (ContainerStatus containerStatus : completedContainers) {
      logger.info(amState.appAttemptId + " got container status for containerID="
          + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
          + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
          + containerStatus.getDiagnostics());

      // non complete containers should not be here
      assert (containerStatus.getState() == ContainerState.COMPLETE);
      // ignore containers we know nothing about - probably from a previous
      // attempt
      if (!amState.launchedContainers.contains(containerStatus.getContainerId())) {
        logger.info("Ignoring completed status of " + containerStatus.getContainerId()
            + "; unknown container(probably launched by previous attempt)");
        continue;
      }

      // increment counters for completed/failed containers
      int exitStatus = containerStatus.getExitStatus();
      if (0 != exitStatus) {
        // container failed
        logger.info("container failed, exit status is " + exitStatus);
        for (RayNodeContext node : amState.indexToNode) {
          if (node.container != null
              && node.container.getId().equals(containerStatus.getContainerId())) {
            logger.info("ray node failed, the role is " + node.role);
            if (-100 == exitStatus) { /* release container will return -100 */
              if (node.isRunning == false) {
                logger.info("release container will return -100, don't process it");
                break;
              } else {
                logger.warn("the exit status is -100, but this node should be running");
              }
            }
            node.isRunning = false;
            node.isAllocating = false;
            node.instanceId = null;
            node.container = null;
            node.failCounter++;

            if (rayConf.isDisableProcessFo()) {
              logger.info("process failover is disable, ignore container failed");
              break;
            }

            if (rayConf.isSupremeFo()) {
              logger.info("Start supreme failover");
              restartClasterFlag = true;
            }

            if (node.role == "head") {
              restartClasterFlag = true;
            }
            amState.numAllocatedContainers.decrementAndGet();
            amState.numRequestedContainers.decrementAndGet();
            break;
          }
        }

        if (restartClasterFlag) {
          logger.info("restart all the Container of ray node");
          for (RayNodeContext node : amState.indexToNode) {
            if (node.isRunning && node.container != null) {
              amRmClient.releaseAssignedContainer(node.container.getId());
              node.isRunning = false;
              node.isAllocating = false;
              node.instanceId = null;
              node.container = null;
              node.failCounter++;
              amState.numAllocatedContainers.decrementAndGet();
              amState.numRequestedContainers.decrementAndGet();
            }
          }
        }
      } else {
        // nothing to do
        // container completed successfully
        amState.numCompletedContainers.incrementAndGet();
        logger.info("Container completed successfully." + ", containerId="
            + containerStatus.getContainerId());
      }
      if (timelineClient != null) {
        TimelineUtil.publishContainerEndEvent(timelineClient, containerStatus, rayConf.getDomainId(), appSubmitterUgi);
      }

      if (restartClasterFlag) {
        break;
      }
    }

    // ask for more containers if any failed
    int askCount = amState.numTotalContainers - amState.numRequestedContainers.get();
    amState.numRequestedContainers.addAndGet(askCount);

    int requestCount = setupContainerRequest();
    assert requestCount == askCount : "The request count is inconsistent(onContainersCompleted): "
        + requestCount + " != " + askCount;

    if (amState.numCompletedContainers.get() == amState.numTotalContainers) {
      amState.done = true;
    }
  }

  @Override
  public void onContainersAllocated(List<Container> allocatedContainers) {
    logger.info(
        "Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
    amState.numAllocatedContainers.addAndGet(allocatedContainers.size());
    for (Container allocatedContainer : allocatedContainers) {
      String rayInstanceId = Integer.toString(amState.rayInstanceCounter);
      amState.rayInstanceCounter++;

      Thread launchThread = null;
      boolean shouldSleep = false;
      for (RayNodeContext node : amState.indexToNode) {
        if (node.isRunning) {
          continue;
        }
        node.isRunning = true;
        node.isAllocating = false;
        node.instanceId = rayInstanceId;
        node.container = allocatedContainer;
        amState.containerToNode.put(allocatedContainer.getId().toString(), node);
        if (node.role == "head") {
          try {
            amState.redisAddress =
                InetAddress.getByName(allocatedContainer.getNodeHttpAddress().split(":")[0])
                    .getHostAddress() + ":" + amState.redisPort;
          } catch (UnknownHostException e) {
            amState.redisAddress = "";
          }
        } else {
          shouldSleep = true;
        }
        launchThread = ContainerLauncher.create(allocatedContainer, rayInstanceId, node.role,
            shouldSleep ? 20000 : 0);
        break;
      }

      if (launchThread == null) {
        logger.error("The container " + allocatedContainer + " unused!");
        break;
      }

      logger.info("Launching Ray instance on a new container." + ", containerId="
          + allocatedContainer.getId() + ", rayInstanceId=" + rayInstanceId + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory="
          + allocatedContainer.getResource().getMemorySize() + ", containerResourceVirtualCores="
          + allocatedContainer.getResource().getVirtualCores());
      // + ", containerToken"
      // +allocatedContainer.getContainerToken().getIdentifier().toString());

      // launch and start the container on a separate thread to keep
      // the main thread unblocked
      // as all containers may not be allocated at one go.
      launchThreads.add(launchThread);
      amState.launchedContainers.add(allocatedContainer.getId());
      launchThread.start();
    }
  }

  @Override
  public void onContainersUpdated(List<UpdatedContainer> containers) {
  }

  @Override
  public void onShutdownRequest() {
    amState.done = true;
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
  }

  @Override
  public float getProgress() {
    // set progress to deliver to RM on next heartbeat
    float progress = (float) amState.numCompletedContainers.get() / amState.numTotalContainers;
    return progress;
  }

  @Override
  public void onError(Throwable e) {
    logger.error("Error in RMCallbackHandler: ", e);
    amState.done = true;
    amRmClient.stop();
  }

  private int setupContainerRequest() {
    int requestCount = 0;
    for (RayNodeContext nodeContext : amState.indexToNode) {
      if (nodeContext.isRunning == false && nodeContext.isAllocating == false) {
        ContainerRequest containerAsk = setupContainerAskForRm();
        amRmClient.addContainerRequest(containerAsk);
        requestCount++;
        nodeContext.isAllocating = true;
        logger.info("Setup container request: " + containerAsk);
      }
    }
    logger.info("Setup container request, count is " + requestCount);
    return requestCount;
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRm() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Priority.newInstance(rayConf.getShellCmdPriority());

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Resource
        .newInstance(rayConf.getContainerMemory(), rayConf.getContainerVCores());

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    logger.info("Requested container ask: " + request.toString());
    return request;
  }

}
