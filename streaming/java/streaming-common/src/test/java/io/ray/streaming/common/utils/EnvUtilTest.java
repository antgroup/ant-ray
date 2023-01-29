package io.ray.streaming.common.utils;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EnvUtilTest {

  @Test()
  public void testGetHostName() {
    String hostname = EnvUtil.getHostName();
    assert !hostname.isEmpty();
  }

  @Test()
  public void testGetHostNameByIp() {
    String hostname = EnvUtil.getHostName();
    String ip = EnvUtil.getHostAddress();
    String hostnameByIp = EnvUtil.getHostnameByAddress(ip);
    assert hostname.equals(hostnameByIp) || hostnameByIp.equals(ip);
  }

  @Test()
  public void testGetParentPid() {
    String ppid = EnvUtil.getParentPid();
    assert !EnvUtil.getJvmPid().equals(ppid);
  }

  @Test
  public void testGetSystemProperties() {
    String envKey = "env_test";
    String envValue = "testing123";
    System.setProperty(envKey, envValue);

    Map<String, String> envMap = EnvUtil.getSystemProperties();
    Assert.assertTrue(envMap.containsKey(envKey));
    Assert.assertEquals(envMap.get(envKey), envValue);
  }
}
