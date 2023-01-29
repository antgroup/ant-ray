package io.ray.performancetest.test;

import io.ray.performancetest.Parameters;
import io.ray.performancetest.test.latency.LatencyParameters;

/** 1对1单向ray call，加ignoreReturn, 一收一发. */
public class ActorPerformanceTestCase16 {

  public static void main(String[] args) {
    final int[] layers = new int[] {1, 1};
    final int[] actorsPerLayer = new int[] {1, 1};
    final boolean hasReturn = false;
    final int argSize = 0;
    final boolean useDirectByteBuffer = false;
    final boolean ignoreReturn = true;
    final int numJavaWorkerPerProcess = 1;
    ActorPerformanceTestBase.run(
        args,
        layers,
        actorsPerLayer,
        new Parameters(
            hasReturn, argSize, useDirectByteBuffer, ignoreReturn, numJavaWorkerPerProcess, 1),
        new LatencyParameters(false, 0, 0));
  }
}
