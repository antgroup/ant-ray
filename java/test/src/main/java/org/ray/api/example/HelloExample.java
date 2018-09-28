package org.ray.api.example;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;

/**
 * HelloWorld example for tests, like test cluster.
 */
public class HelloExample {

  @RayRemote
  public static String sayHello() {
    String ret = "hello";
    System.out.println(ret);
    return ret;
  }

  @RayRemote
  public static String sayWorld() {
    String ret = "world!";
    System.out.println(ret);
    return ret;
  }

  /**
   * A remote function with dependency.
   */
  @RayRemote
  public static String merge(String hello, String world) {
    return hello + "," + world;
  }

  public static String sayHelloWorld() {
    RayObject<String> hello = Ray.call(HelloExample::sayHello);
    RayObject<String> world = Ray.call(HelloExample::sayWorld);
    // Pass unfinished results as the parameters to another remote function.
    return Ray.call(HelloExample::merge, hello, world).get();
  }

  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      String helloWorld = HelloExample.sayHelloWorld();
      System.out.println(helloWorld);
      assert "hello,world!".equals(helloWorld);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }
}
