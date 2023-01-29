package io.ray.performancetest.test;

/*
 Put 50000 small(less than 100K) objects.
*/
public class ObjectCreatePerfTestCase1 {
  public static void main(String[] args) {
    ObjectCreatePerfTestBase.run(args, 5000, 20 * 1024);
  }
}
