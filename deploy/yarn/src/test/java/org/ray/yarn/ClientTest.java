package org.ray.yarn;

import org.testng.annotations.Test;

public class ClientTest {

  @Test
  public void smokeTest() throws Exception {
    Client client = new Client();
    client.init(new String[]{"--help"});
  }
}
