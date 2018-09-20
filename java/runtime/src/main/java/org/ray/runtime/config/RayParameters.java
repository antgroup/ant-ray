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


  @AConfig(comment = "local node ip")
  public String node_ip_address = NetworkUtil.getIpAddress(null);

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
