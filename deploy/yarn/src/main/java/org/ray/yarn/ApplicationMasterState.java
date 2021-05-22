package org.ray.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * ApplicatonMaster State
 */
public class ApplicationMasterState {

  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  ApplicationAttemptId appAttemptId;
  // Counter for completed containers ( complete denotes successful or failed )
  AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  AtomicInteger numRequestedContainers = new AtomicInteger();
  // No. of containers to run shell command on
  @VisibleForTesting
  int numTotalContainers = 1;
  // Ray Node List
  RayNodeContext[] indexToNode = null;

  // No. of each of the Ray roles including head and work
  private Map<String, Integer> numRoles = Maps.newHashMapWithExpectedSize(2);
  Map<String, RayNodeContext> containerToNode = Maps.newHashMap();
  @VisibleForTesting
  final Set<ContainerId> launchedContainers =
      Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());
  volatile boolean done;
  int rayInstanceCounter = 1;
  // The default value should be consistent with Ray RunParameters
  int redisPort = 34222;
  String redisAddress;
  ByteBuffer allTokens;

}
