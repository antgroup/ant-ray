package io.ray.performancetest.test;

/*
 Put 5000 big(greater than 100K) objects.
*/
public class ObjectCreatePerfTestCase2 {
  public static void main(String[] args) {
    ObjectCreatePerfTestBase.run(args, 5000, 200 * 1024);
  }
}
