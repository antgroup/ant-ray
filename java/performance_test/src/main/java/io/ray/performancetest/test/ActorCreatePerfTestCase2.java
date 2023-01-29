package io.ray.performancetest.test;

/*
 * start 500 * #nodes actors in total with randomly selected nodes.
 */
public class ActorCreatePerfTestCase2 {
  public static void main(String[] args) {
    ActorCreatePerfTestBase.run(args, 500, false);
  }
}
