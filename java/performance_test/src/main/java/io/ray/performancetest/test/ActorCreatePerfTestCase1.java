package io.ray.performancetest.test;

/** start 500 actors in each node, aka flatten style. */
public class ActorCreatePerfTestCase1 {

  public static void main(String[] args) {
    ActorCreatePerfTestBase.run(args, 500, true);
  }
}
