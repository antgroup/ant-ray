package io.ray.performancetest.test;

import io.ray.performancetest.Parameters;
import io.ray.performancetest.test.latency.LatencyParameters;

/** 1对1单向ray call，每个上游发给每个下游，上游下游都是多actor的进程。模拟rayag的场景. */
public class ActorPerformanceTestCase8 {

  public static void main(String[] args) {
    final int[] layers = new int[] {1, 1};
    final int[] actorsPerLayer = new int[] {10, 10};
    final boolean hasReturn = false;
    final int argSize = 0;
    final boolean useDirectByteBuffer = false;
    final boolean ignoreReturn = false;
    final int numJavaWorkerPerProcess = 10;
    ActorPerformanceTestBase.run(
        args,
        layers,
        actorsPerLayer,
        new Parameters(
            hasReturn, argSize, useDirectByteBuffer, ignoreReturn, numJavaWorkerPerProcess, 1),
        new LatencyParameters(false, 0, 0));
  }
}
