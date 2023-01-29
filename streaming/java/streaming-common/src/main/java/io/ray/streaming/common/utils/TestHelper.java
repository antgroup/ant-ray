package io.ray.streaming.common.utils;

import io.ray.api.Ray;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.common.serializer.Serializer;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHelper {

  private static boolean isSingleProcess = false;

  static {
    if (Ray.isInitialized()
        && Ray.getRuntimeContext() != null
        && Ray.getRuntimeContext().isSingleProcess()) {
      isSingleProcess = true;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

  private static final String UT_PATTERN = "UT_PATTERN";
  private static final String UT_PATTERN_BACK_PRESSURE = UT_PATTERN + "_" + "BACKPRESSURE";
  private static final String UT_PATTERN_BACK_PRESSURE_FOR_WORKER =
      UT_PATTERN + "_" + "BACK_PRESSURE_WORKERS";
  private static final String UT_PATTERN_TARDINESS = UT_PATTERN + "_" + "TARDINESS";
  private static final String UT_PATTERN_CONTAINER_RELEASE_INFO =
      UT_PATTERN + "_" + "CONTAINER_RELEASE_INFO";
  private static final String UT_PATTERN_CONTAINER_ADD_INFO =
      UT_PATTERN + "_" + "CONTAINER_ADD_INFO";
  private static final String UT_PATTERN_CONTAINER_RELEASE_ENABLE =
      UT_PATTERN + "_" + "CONTAINER_RELEASE_ENABLE";
  public static final String RESTSERVER_INFO_PATH_PREFIX = "/tmp/restServerAddress_";
  public static final String HTTP_PREFIX = "http://";
  public static final String REST_API = "/api/v1";
  public static final String JOB_REST_API = "/api/v1/job";
  public static final String DR_TEST = "DR_TEST";

  private static Map<String, Double> UT_MOCK_BACKPRESSURE_MAP = new HashMap<>();

  public static void setUTPattern() {
    System.setProperty(UT_PATTERN, "");
  }

  public static void clearUTPattern() {
    clearMockBackpressure();
    clearMockBackpressureForWorkers();
    clearContainerReleasedInfo();
    clearContainerAddInfo();
    disableContainerReleasing();
    System.clearProperty(UT_PATTERN);
  }

  public static boolean isUTPattern() {
    return System.getProperty(UT_PATTERN) != null || isSingleProcess;
  }

  public static void mockBackpressure() {
    System.setProperty(UT_PATTERN_BACK_PRESSURE, "");
  }

  public static void mockBackpressureForWorkers(Map<String, Double> bpMap) {
    UT_MOCK_BACKPRESSURE_MAP.putAll(bpMap);
    System.setProperty(UT_PATTERN_BACK_PRESSURE_FOR_WORKER, "");
  }

  public static void mockBackpressureForDR(Map<String, Double> bpMap) {
    UT_MOCK_BACKPRESSURE_MAP.putAll(bpMap);
    System.setProperty(DR_TEST, "");
  }

  public static void clearMockBackpressureForDR() {
    System.clearProperty(DR_TEST);
    UT_MOCK_BACKPRESSURE_MAP.clear();
  }

  public static void clearMockBackpressure() {
    System.clearProperty(UT_PATTERN_BACK_PRESSURE);
  }

  public static void clearMockBackpressureForWorkers() {
    System.clearProperty(UT_PATTERN_BACK_PRESSURE_FOR_WORKER);
    UT_MOCK_BACKPRESSURE_MAP.clear();
  }

  public static boolean isMockBackpressure() {
    return System.getProperty(UT_PATTERN_BACK_PRESSURE) != null;
  }

  public static boolean isDRTest() {
    return System.getProperty(DR_TEST) != null;
  }

  public static boolean isMockBackpressureForWorkers() {
    return System.getProperty(UT_PATTERN_BACK_PRESSURE_FOR_WORKER) != null;
  }

  public static Map<String, Double> getUtMockBackpressureMap() {
    return UT_MOCK_BACKPRESSURE_MAP;
  }

  public static void mockTardiness(String hostName) {
    System.setProperty(UT_PATTERN_TARDINESS, hostName);
  }

  public static void clearMockTardiness() {
    System.clearProperty(UT_PATTERN_TARDINESS);
  }

  public static boolean isMockTardiness() {
    return System.getProperty(UT_PATTERN_TARDINESS) != null;
  }

  public static String getMockTardiness() {
    return System.getProperty(UT_PATTERN_TARDINESS);
  }

  public static String getRestServerInfoFilePath(String jobName) {
    return RESTSERVER_INFO_PATH_PREFIX + jobName;
  }

  public static String getRestServerJobUrl(String jobName) throws IOException {
    byte[] bytes = FileUtils.readFileToByteArray(new File(getRestServerInfoFilePath(jobName)));
    return HTTP_PREFIX + Serializer.decode(bytes) + JOB_REST_API;
  }

  public static String getRestServerUrl(String jobName) throws IOException {
    byte[] bytes = FileUtils.readFileToByteArray(new File(getRestServerInfoFilePath(jobName)));
    return HTTP_PREFIX + Serializer.decode(bytes) + REST_API;
  }

  public static boolean isRestSeverAvailable(String jobName) {
    boolean available = true;
    try {
      byte[] bytes = FileUtils.readFileToByteArray(new File(getRestServerInfoFilePath(jobName)));
      String netAdd = Serializer.decode(bytes);
      LOG.info("Net address read from local file is {}", netAdd);
      String[] ipPort = netAdd.split(":");
      String ip = ipPort[0];
      int port = Integer.parseInt(ipPort[1]);
      available = isHostConnectable(ip, port, 1000);
    } catch (IOException e) {
      available = false;
      LOG.warn(e.getMessage());
    }
    return available;
  }

  public static boolean isHostConnectable(String host, int port) {
    return isHostConnectable(host, port, 0);
  }

  /**
   * @param host the Host name
   * @param port the port number
   * @param timeout he timeout value to be used in milliseconds
   */
  public static boolean isHostConnectable(String host, int port, int timeout) {
    boolean connectable = true;
    Socket socket = new Socket();

    try {
      socket.connect(new InetSocketAddress(host, port), timeout);
    } catch (IOException e) {
      connectable = false;
      LOG.error(e.getMessage(), e);
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return connectable;
  }

  public static boolean checkRestServerInfoFileExist(String jobName) {
    File file = new File(getRestServerInfoFilePath(jobName));
    return file.exists() && file.isFile();
  }

  public static void deleteRestServerInfoFile(String jobName) {
    File file = new File(getRestServerInfoFilePath(jobName));
    if (file.exists() && file.isFile()) {
      file.delete();
    }
  }

  public static void cleanUpJobRunningRubbish(String jobName) {
    deleteRestServerInfoFile(jobName);
  }

  public static Map<String, UniqueId> mockNodeName2Ids(List<NodeInfo> nodeInfos) {
    Map<String, UniqueId> nodeName2Ids = new HashMap<>();
    for (NodeInfo nodeInfo : nodeInfos) {
      nodeName2Ids.put(nodeInfo.nodeAddress, nodeInfo.nodeId);
    }
    return nodeName2Ids;
  }

  public static void clearContainerReleasedInfo() {
    System.clearProperty(UT_PATTERN_CONTAINER_RELEASE_INFO);
  }

  public static void clearContainerAddInfo() {
    System.clearProperty(UT_PATTERN_CONTAINER_ADD_INFO);
  }

  public static void enableContainerReleasing() {
    System.setProperty(UT_PATTERN_CONTAINER_RELEASE_ENABLE, "true");
  }

  public static void disableContainerReleasing() {
    System.clearProperty(UT_PATTERN_CONTAINER_RELEASE_ENABLE);
  }

  public static String getTestName(Object clazz, Method method) {
    return clazz.getClass().getName() + "_" + method.getName();
  }
}
