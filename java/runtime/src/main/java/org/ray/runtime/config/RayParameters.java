package org.ray.runtime.config;

import org.ray.api.RunMode;
import org.ray.api.WorkerMode;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.NetworkUtil;
import org.ray.runtime.util.config.AConfig;
import org.ray.runtime.util.config.ConfigReader;

/**
 * Runtime parameters of Ray process.
 */
public class RayParameters {

  @AConfig(comment = "worker mode for this process DRIVER | WORKER | NONE")
  public WorkerMode worker_mode = WorkerMode.DRIVER;

  @AConfig(comment = "run mode for this app SINGLE_PROCESS | SINGLE_BOX | CLUSTER")
  public RunMode run_mode = RunMode.SINGLE_BOX;

  @AConfig(comment = "local node ip")
  public String node_ip_address = NetworkUtil.getIpAddress(null);

  @AConfig(comment = "primary redis address (e.g., 127.0.0.1:34222")
  public String redis_address = "";

  @AConfig(comment = "object store name (e.g., /tmp/store1111")
  public String object_store_name = "";

  @AConfig(comment = "object store rpc listen port")
  public int object_store_rpc_port = 32567;

  @AConfig(comment = "driver ID when the worker is served as a driver")
  public UniqueId driver_id = UniqueId.fromHexString("0123456789abcdef0123456789abcdef01234567");

  @AConfig(comment = "logging directory")
  public String log_dir = "/tmp/raylogs";

  @AConfig(comment = "primary redis port")
  public int redis_port = 34111;

  @AConfig(comment = "number of workers started initially")
  public int num_workers = 2;

  @AConfig(comment = "redirect err and stdout to files for newly created processes")
  public boolean redirect = true;

  @AConfig(comment = "whether to start redis shard server in addition to the primary server")
  public boolean start_redis_shards = false;

  @AConfig(comment = "whether to clean up the processes when there is a process start failure")
  public boolean cleanup = false;

  @AConfig(comment = "number of redis shard servers to be started")
  public int num_redis_shards = 0;

  @AConfig(comment = "whether this is a deployment in cluster")
  public boolean deploy = false;

  @AConfig(comment = "default first check timeout(ms)")
  public int default_first_check_timeout_ms = 1000;

  @AConfig(comment = "default get check rate(ms)")
  public int default_get_check_interval_ms = 5000;

  @AConfig(comment = "add the jvm parameters for java worker")
  public String jvm_parameters = "";

  @AConfig(comment = "set the occupied memory(MB) size of object store")
  public int object_store_occupied_memory_MB = 1000;

  @AConfig(comment = "delay seconds under onebox before app logic for debugging")
  public int onebox_delay_seconds_before_run_app_logic = 0;

  @AConfig(comment = "raylet socket name (e.g., /tmp/raylet1111")
  public String raylet_socket_name = "";

  @AConfig(comment = "raylet rpc listen port")
  public int raylet_port = 35567;

  @AConfig(comment = "worker fetch request size")
  public int worker_fetch_request_size = 1000;

  @AConfig(comment = "static resource list of this node")
  public String static_resources = "CPU:4,GPU:0";



  //TODO(qwang): Write the default vaule.
  @AConfig(comment = "Additional class path for JAVA")
  public String[] java_class_paths = {"/Users/wangqing/Workspace/source/refactor/ray/java/api/target/classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/api/target/test-classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/runtime/target/classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/runtime/target/test-classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/tutorial/target/classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/test/target/classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/test/target/test-classes",
      "/Users/wangqing/Workspace/source/refactor/ray/java/test/lib/*",
      "/Users/wangqing/Workspace/source/refactor/ray/java/conf"};
  @AConfig(comment = "Additional JNI library paths for JAVA")
  public String[] java_jnilib_paths = {
      "/Users/wangqing/Workspace/source/refactor/ray/build/src/plasma",
      "/Users/wangqing/Workspace/source/refactor/ray/build/src/local_scheduler"};
  @AConfig(comment = "Path to redis-server")
  public String redis_server_path = "/Users/wangqing/Workspace/source/refactor/ray/build/src/common/thirdparty/redis/src/redis-server";
  @AConfig(comment = "Path to redis module")
  public String redis_module_path = "/Users/wangqing/Workspace/source/refactor/ray/build/src/common/redis_module/libray_redis_module.so";
  @AConfig(comment = "Path to plasma storage")
  public String plasma_store_path = "/Users/wangqing/Workspace/source/refactor/ray/build/src/plasma/plasma_store_server";
  @AConfig(comment = "Path to raylet")
  public String raylet_path = "/Users/wangqing/Workspace/source/refactor/ray/build/src/ray/raylet/raylet";





  //TODO(qwang): We should change the section key.
  public RayParameters(ConfigReader config) {
    if (null != config) {
      String networkInterface = config.getStringValue("ray.java", "network_interface", null,
          "Network interface to be specified for host ip address(e.g., en0, eth0), may use "
              + "ifconfig to get options");
      node_ip_address = NetworkUtil.getIpAddress(networkInterface);
      config.readObject("ray.java.start", this, this);
    }
  }
}
