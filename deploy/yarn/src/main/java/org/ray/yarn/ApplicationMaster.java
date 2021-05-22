package org.ray.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.sun.jersey.api.client.ClientHandlerException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.log4j.LogManager;
import org.ray.yarn.config.RayClusterConfig;
import org.ray.yarn.utils.TimelineUtil;
import org.ray.yarn.utils.YamlUtil;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  private static final Log logger = LogFactory.getLog(ApplicationMaster.class);

  @VisibleForTesting
  @Private
  public static enum DsEvent {
    DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
  }

  @VisibleForTesting
  @Private
  public static enum DsEntity {
    DS_APP_ATTEMPT, DS_CONTAINER
  }

  private static final String YARN_SHELL_ID = "YARN_SHELL_ID";
  private static final String HEAD_NAME = "head";
  private static final String WORK_NAME = "work";

  // Configuration
  private Configuration conf;
  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRmClient;
  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;
  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NmCallbackHandler containerListener;
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  // Runtime state of AM
  ApplicationMasterState amState = null;

  // FIXME state begin
  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  protected ApplicationAttemptId appAttemptId;
  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  protected AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  protected AtomicInteger numRequestedContainers = new AtomicInteger();
  // App Master configuration
  // No. of containers to run shell command on
  @VisibleForTesting
  protected int numTotalContainers = 1;

  // No. of each of the Ray roles including head and work
  private Map<String, Integer> numRoles = Maps.newHashMapWithExpectedSize(2);
  private RayNodeContext[] indexToNode = null;
  private Map<String, RayNodeContext> containerToNode = Maps.newHashMap();

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers =
    Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  private int rayInstanceCounter = 1;
  private volatile boolean done;
  private ByteBuffer allTokens;

  // The default value should be consistent with Ray RunParameters
  private int redisPort = 34222;
  private String redisAddress;
  // FIXME state end

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();
  // Hardcoded path to shell script in launch container's local env
  private static final String rayShellStringPath = "run.sh";
  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";

  // Timeline Client
  @VisibleForTesting
  TimelineClient timelineClient;
  static final String CONTAINER_ENTITY_GROUP_ID = "CONTAINERS";
  static final String APPID_TIMELINE_FILTER_NAME = "appId";
  static final String USER_TIMELINE_FILTER_NAME = "user";
  static final String LINUX_BASH_COMMEND = "bash";

  private RayClusterConfig rayConf;


  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
    // Initialize internal state
    amState = new ApplicationMasterState();
  }

  /**
   * The main entrance of appMaster.
   * @param args Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      logger.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      logger.fatal("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
    if (result) {
      logger.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      logger.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging.
   */
  private void dumpOutDebugInfo() {

    logger.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      logger.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }

    BufferedReader buf = null;
    try {
      String lines = Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        logger.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanup(logger, buf);
    }
  }

  /**
   * Parse command line options.
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   */
  public boolean init(String[] args) throws ParseException, IOException {
    Options opts = new Options();

    opts.addOption(DsConstants.RAY_CONF_PARAM, true, "ray cluster config");
    opts.addOption("appAttemptId", true,
        "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for application master to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    // Check whether customer log4j.properties file exists
    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper
            .updateLog4jConfiguration(ApplicationMaster.class, log4jPath);
      } catch (Exception e) {
        logger.warn("Can not set up custom log4j properties. " + e);
      }
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    String rayConfPath = cliParser.getOptionValue(DsConstants.RAY_CONF_PARAM);
    try {
      rayConf = YamlUtil.loadFile(rayConfPath, RayClusterConfig.class);
      rayConf.validate();
    } catch (IOException e) {
      logger.error("Fail to read ray configuration from file " + rayConfPath);
      throw new RuntimeException("Fail to read ray configuration from file", e);
    }

    Map<String, String> envs = System.getenv();
    initAttemptId(cliParser, envs);
    checkEnvs(envs);
    initContainerInfo();

    logger.info("Application master for app" + ", appId=" + appAttemptId.getApplicationId().getId()
        + ", clustertimestamp=" + appAttemptId.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptId.getAttemptId());
    return true;
  }

  /**
   * Main run function for the application master.
   */
  public void run() throws YarnException, IOException, InterruptedException {
    logger.info("Starting ApplicationMaster");

    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    logger.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      logger.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
    appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);

    AMRMClientAsync.AbstractCallbackHandler allocListener = createRmCallbackHandler();
    amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRmClient.init(conf);
    amRmClient.start();

    containerListener = createNmCallbackHandler();
    nmClientAsync = containerListener.getNmClientAsync();
    nmClientAsync.init(conf);
    nmClientAsync.start();

    // startTimelineClient(conf);
    if (timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptId.toString(),
          DsEvent.DS_APP_ATTEMPT_START, rayConf.getDomainId(), appSubmitterUgi);
    }

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRmClient
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

    // Dump out information about cluster capability as seen by the
    // resource manager
    long maxMem = response.getMaximumResourceCapability().getMemorySize();
    logger.info("Max mem capability of resources in this cluster " + maxMem);
    int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    logger.info("Max vcores capability of resources in this cluster " + maxVCores);

    // A resource ask cannot exceed the max.
    if (rayConf.getContainerMemory() > maxMem) {
      logger.info("Container memory specified above max threshold of cluster." + " Using max value."
          + ", specified=" + rayConf.getContainerMemory() + ", max=" + maxMem);
      rayConf.setContainerMemory(maxMem);
    }

    if (rayConf.getContainerVCores() > maxVCores) {
      logger.info("Container virtual cores specified above max threshold of cluster."
          + " Using max value." + ", specified=" + rayConf.getContainerVCores() + ", max=" + maxVCores);
      rayConf.setContainerVCores(maxVCores);
    }

    List<Container> previousAmRunningContainers = response.getContainersFromPreviousAttempts();
    logger.info(appAttemptId + " received " + previousAmRunningContainers.size()
        + " previous attempts' running containers on AM registration.");
    for (Container container : previousAmRunningContainers) {
      launchedContainers.add(container.getId());
    }
    numAllocatedContainers.addAndGet(previousAmRunningContainers.size());

    int numTotalContainersToRequest = numTotalContainers - previousAmRunningContainers.size();

    if (previousAmRunningContainers.size() > 0) {
      // TODO: support failover about recovery ray node context
      logger.warn("Some previous containers found.");
      rayNodeContextRecovery(indexToNode, previousAmRunningContainers);
    }
    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    int requestCount = setupContainerRequest();

    assert requestCount == numTotalContainersToRequest : "The request count is inconsistent: "
        + requestCount + " != " + numTotalContainersToRequest;

    // for (int i = 0; i < numTotalContainersToRequest; ++i) {
    // ContainerRequest containerAsk = setupContainerAskForRM(null);
    // amRMClient.addContainerRequest(containerAsk);
    // }
    numRequestedContainers.set(numTotalContainers);
  }

  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  private void initAttemptId(CommandLine cliParser, Map<String, String> envs) {
    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("appAttemptId")) {
        String appIdStr = cliParser.getOptionValue("appAttemptId", "");
        appAttemptId = ApplicationAttemptId.fromString(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ContainerId
          .fromString(envs.get(Environment.CONTAINER_ID.name()));
      appAttemptId = containerId.getApplicationAttemptId();
    }
  }

  private void checkEnvs(Map<String, String> envs) {
    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(
          ApplicationConstants.APP_SUBMIT_TIME_ENV
              + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(
          Environment.NM_HOST.name() + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(
          Environment.NM_HTTP_PORT + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(
          Environment.NM_PORT.name() + " not set in the environment");
    }
  }

  private void initContainerInfo() {
    numTotalContainers = rayConf.getNumContainers();
    if (numTotalContainers == 0) {
      throw new IllegalArgumentException("Cannot run distributed shell with no launchedContainers");
    }

    // FIXME get node numbers from rayConf
    numTotalContainers = rayConf.getNumRoles().get("head") + rayConf.getNumRoles().get("work");

    indexToNode = new RayNodeContext[numTotalContainers];
    int i = 0;
    if (rayConf.getNumRoles().get("head") == 1) {
      indexToNode[i] = new RayNodeContext("head");
      ++i;
    }
    for (int j = 0; j < rayConf.getNumRoles().get("work"); ++j) {
      indexToNode[i] = new RayNodeContext("word");
      ++i;
    }
    assert numTotalContainers == i;
  }

  private int setupContainerRequest() {
    int requestCount = 0;
    for (RayNodeContext nodeContext : indexToNode) {
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

  private boolean rayNodeContextRecovery(RayNodeContext[] indexToNode,
      List<Container> containers) {
    // TODO handle AM FO
    return true;
  }

  @VisibleForTesting
  void startTimelineClient(final Configuration conf)
      throws YarnException, IOException, InterruptedException {
    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            // Creating the Timeline Client
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(conf);
            timelineClient.start();
          } else {
            timelineClient = null;
            logger.warn("Timeline service is not enabled");
          }
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new YarnException(e.getCause());
    }
  }

  @VisibleForTesting
  NmCallbackHandler createNmCallbackHandler() {
    return new NmCallbackHandler(this);
  }

  @VisibleForTesting
  protected boolean finish() {
    // wait for completion.
    while (!done && (numCompletedContainers.get() != numTotalContainers)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
        logger.warn("Catch InterruptedException when sleep.");
      }
    }

    if (timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptId.toString(),
          DsEvent.DS_APP_ATTEMPT_END, rayConf.getDomainId(), appSubmitterUgi);
    }

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        logger.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    logger.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    logger.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed="
          + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get()
          + ", failed=" + numFailedContainers.get();
      logger.info(appMessage);
      success = false;
    }
    try {
      amRmClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      logger.error("Failed to unregister application", ex);
    } catch (IOException e) {
      logger.error("Failed to unregister application", e);
    }

    amRmClient.stop();

    // Stop Timeline Client
    if (timelineClient != null) {
      timelineClient.stop();
    }

    return success;
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

  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }

  private String readContent(String filePath) throws IOException {
    DataInputStream ds = null;
    try {
      ds = new DataInputStream(new FileInputStream(filePath));
      return ds.readUTF();
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ds);
    }
  }

  private void publishApplicationAttemptEvent(final TimelineClient timelineClient,
      String appAttemptId, DsEvent appEvent, String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(appAttemptId);
    entity.setEntityType(DsEntity.DS_APP_ATTEMPT.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);
    try {
      TimelinePutResponse response = timelineClient.putEntities(entity);
      TimelineUtil.processTimelineResponseErrors(response);
    } catch (YarnException | IOException | ClientHandlerException e) {
      logger.error("App Attempt " + (appEvent.equals(DsEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for " + appAttemptId.toString(), e);
    }
  }

  RmCallbackHandler createRmCallbackHandler() {
    return new RmCallbackHandler(this);
  }

  @SuppressWarnings("rawtypes")
  @VisibleForTesting
  void setAmRmClient(AMRMClientAsync client) {
    this.amRmClient = client;
  }

  @VisibleForTesting
  int getNumCompletedContainers() {
    return numCompletedContainers.get();
  }

  @VisibleForTesting
  boolean getDone() {
    return done;
  }

  @VisibleForTesting
  Thread createLaunchContainerThread(Container allocatedContainer, String shellId, String role,
      long sleepMillis) {
    ContainerLauncher runnableLaunchContainer = new ContainerLauncher(
        allocatedContainer, containerListener, shellId, role, sleepMillis);
    return new Thread(runnableLaunchContainer);
  }

  public RayClusterConfig getRayConf() {
    return rayConf;
  }

  public TimelineClient getTimelineClient() {
    return timelineClient;
  }


}
