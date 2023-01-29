package io.ray.performancetest.test;

import io.ray.performancetest.Parameters;
import io.ray.performancetest.test.latency.LatencyParameters;

/** 1对1单向ray call，发送端多线程，一次remote一次wait，模拟mpp的场景. */
public class ActorPerformanceTestCase17 {

  public static void main(String[] args) {
    final int[] layers = new int[] {4, 5};
    final int[] actorsPerLayer = new int[] {1, 4};
    final boolean hasReturn = true;
    final int argSize = 1024;
    final boolean useDirectByteBuffer = false;
    final boolean ignoreReturn = false;
    final int numJavaWorkerPerProcess = 1;
    ActorPerformanceTestBase.run(
        args,
        layers,
        actorsPerLayer,
        new Parameters(
            hasReturn, argSize, useDirectByteBuffer, ignoreReturn, numJavaWorkerPerProcess, 200),
        new LatencyParameters(false, 0, 0));
  }
}
