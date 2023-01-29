package io.ray.performancetest.test;

import io.ray.performancetest.Parameters;
import io.ray.performancetest.test.latency.LatencyParameters;

/** 多对一：多个单线程worker进程对一个多线程worker进程。测试接收端的可扩展性：吞吐随线程数增长. */
public class ActorPerformanceTestCase14 {

  public static void main(String[] args) {
    final int[] layers = new int[] {10, 1};
    final int[] actorsPerLayer = new int[] {1, 10};
    final boolean hasReturn = false;
    final int argSize = 0;
    final boolean useDirectByteBuffer = false;
    final boolean ignoreReturn = false;
    // TODO: Source需要设置numJavaWorkerPerProcess=1
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
